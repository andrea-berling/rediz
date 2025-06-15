const std = @import("std");
const resp = @import("resp.zig");
const cmd = @import("command.zig");
const fsm = @import("fsm.zig");
const Command = cmd.Command;
const db = @import("db.zig");
const cfg = @import("config.zig");
const util = @import("util.zig");

const posix = std.posix;
const linux = std.os.linux;

const stdout = std.io.getStdOut().writer();

const IO_URING_ENTRIES = 100;

pub const std_options: std.Options = .{
    // Define logFn to override the std implementation
    .logFn = logger,
};

pub fn logger(
    comptime level: std.log.Level,
    comptime scope: @Type(.enum_literal),
    comptime format: []const u8,
    args: anytype,
) void {
    const timestamp_ms = std.time.milliTimestamp();

    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = @bitCast(@divFloor(timestamp_ms, 1000)) };
    const epoch_day = epoch_seconds.getEpochDay();
    const day_seconds = epoch_seconds.getDaySeconds();
    const year_day = epoch_day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const month = month_day.month.numeric();
    const day = month_day.day_index + 1;

    const scope_prefix = "(" ++ @tagName(scope) ++ "): ";

    const prefix = "[" ++ comptime level.asText() ++ "] " ++ scope_prefix;

    // Print the message to stderr, silently ignoring any errors
    std.debug.lockStdErr();
    defer std.debug.unlockStdErr();
    const stderr = std.io.getStdErr().writer();
    nosuspend stderr.print("[{d}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}Z] ", .{ year_day.year, month, day, day_seconds.getHoursIntoDay(), day_seconds.getMinutesIntoHour(), day_seconds.getSecondsIntoMinute(), @as(u64, @bitCast(@mod(timestamp_ms, 1000))) }) catch return;
    nosuspend stderr.print("{d} ", .{linux.getpid()}) catch return;
    nosuspend stderr.print(prefix ++ format ++ "\n", args) catch return;
}

pub fn main() !u8 {

    // INFO: Allocator set up
    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    defer _ = gpa.deinit();
    var allocator = gpa.allocator();

    // INFO: Command line parsing
    const stderr = std.io.getStdErr();
    // Needs to live until we initialize the DB below
    var config = cfg.parseCommandLineArguments(allocator) catch |err| {
        switch (err) {
            error.Help => return 0,
            error.PositionalArgument => {
                _ = try stderr.write("ERROR: positional arguments are not supported\n");
                return 1;
            },
            error.InvalidOption => {
                _ = try stderr.write("ERROR: an invalid option was provided\n");
                return 1;
            },
            error.MissingArgument => {
                _ = try stderr.write("ERROR: a required option argument was not provided\n");
                return 1;
            },
            error.InvalidArgument => {
                _ = try stderr.write("ERROR: an invalid option argument was provided\n");
                return 1;
            },
            else => {
                _ = try stderr.write("ERROR: some unexpected error occurred\n");
                return err;
            },
        }
    };

    // INFO: Server FSM setup

    const address = "127.0.0.1";
    const port = try std.fmt.parseInt(u16, config.get("listening-port") orelse "6379", 10);
    const server_fsm = try fsm.FSM.newServer(allocator, address, port);

    std.log.scoped(.init).info("Listening on {s}:{d}...", .{ address, port });

    // INFO: Event queue init
    var event_queue = try fsm.EventQueue.init(allocator, IO_URING_ENTRIES);

    defer event_queue.destroy(.{ .user_data_destroyer = fsm.FSM.deinit }) catch {
        @panic("Failed destroying the event queue");
    };

    const new_connection_event = fsm.Event{ .type = .{ .accept = server_fsm.type.server.socket }, .user_data = server_fsm };
    try event_queue.addAsyncEvent(new_connection_event);

    const termination_event = fsm.Event{ .type = .{ .pollin = server_fsm.type.server.signalfd }, .user_data = server_fsm };
    try event_queue.addAsyncEvent(termination_event);

    // INFO: DB init
    var instance = try db.Instance.init(allocator, config);
    defer instance.destroy(allocator);
    config.deinit();

    var master_server_fsm: ?*fsm.FSM = null;

    if (instance.master) |master_fd| {
        master_server_fsm = try fsm.FSM.newConnection(server_fsm, master_fd, .master);
        try master_server_fsm.?.waitForCommand(&event_queue, .{});
    }

    const event_loop_logger = std.log.scoped(.event_loop);
    event_loop: while (true) {
        event_loop_logger.debug("Waiting for something to come through...", .{});
        const completed_event = try event_queue.next();
        const event_fsm: *fsm.FSM = completed_event.awaited_event.user_data;
        event_loop_logger.debug("New event: {any}", .{completed_event});
        switch (completed_event.awaited_event.type) {
            .accept => {
                std.debug.assert(event_fsm.type == .server);
                const connection_socket: posix.socket_t = completed_event.async_result;
                event_loop_logger.info("Accepted new connection", .{});
                const new_connection_fsm = try event_fsm.newConnection(connection_socket, .generic_client);
                try new_connection_fsm.waitForCommand(&event_queue, .{});

                // Re-queing the accept event
                try event_queue.addAsyncEvent(new_connection_event);
            },
            .recv => {
                std.debug.assert(event_fsm.type == .connection);
                const connection_fsm = &event_fsm.type.connection;

                if (completed_event.async_result == 0) { // connection closed
                    if (instance.master != null and connection_fsm.peer_type == .master and instance.diewithmaster) {
                        event_loop_logger.info("Peer closed connection", .{});
                        event_fsm.deinit();
                        break :event_loop;
                    }
                    if (connection_fsm.peer_type != .slave or event_fsm.global_data.slaves.remove(event_fsm)) {
                        event_fsm.deinit();
                    }
                    try event_queue.removePendingEvents(event_fsm);
                    continue :event_loop;
                }

                if (completed_event.async_result < 0) { // error
                    event_loop_logger.err("Error: {} {d}", .{ linux.E.init(@intCast(@as(u32, @bitCast(completed_event.async_result)))), completed_event.async_result });
                    if (connection_fsm.peer_type != .slave or event_fsm.global_data.slaves.remove(event_fsm)) {
                        event_fsm.deinit();
                    }
                    try event_queue.removePendingEvents(event_fsm);
                    continue :event_loop;
                }

                if (connection_fsm.peer_type == .slave and connection_fsm.state == .waiting_for_ack) {
                    var temp_allocator = std.heap.ArenaAllocator.init(allocator);
                    defer temp_allocator.deinit();

                    const response, _ = resp.Value.parse(connection_fsm.buffer, temp_allocator.allocator()) catch {
                        event_loop_logger.debug("Got some invalid bytes on the wire: {?x}", .{connection_fsm.buffer});
                        try event_fsm.respondWith(try resp.SimpleError("Invalid bytes on the wire").encode(temp_allocator.allocator()), &event_queue, .{});
                        continue :event_loop;
                    };

                    std.debug.assert(response == .array);

                    // Response should be of the form: REPLCONF ACK <offset>
                    const new_offset = try std.fmt.parseInt(usize, response.array[2].bulk_string, 10);

                    const old_offset = connection_fsm.peer_type.slave.repl_offset;
                    // Only update the offset if the message is still relevant
                    if (old_offset < new_offset) {
                        connection_fsm.peer_type.slave.repl_offset = new_offset;
                    }

                    var new_pending_waits = std.PriorityQueue(*fsm.PendingWait, void, fsm.PendingWait.order).init(allocator, undefined);
                    while (event_fsm.global_data.pending_waits.removeOrNull()) |pending_wait| {
                        // Only pending waits who have a threshold offset that
                        // wasn't met previously by the slave and that is now
                        // met after the offset update should be counted
                        if (pending_wait.threshold_offset > old_offset and pending_wait.threshold_offset <= new_offset) {
                            pending_wait.actual_n_replicas += 1;
                            if (pending_wait.actual_n_replicas >= pending_wait.expected_n_replicas) {
                                const reply = resp.Integer(@bitCast(pending_wait.actual_n_replicas));
                                try pending_wait.client_connection_fsm.respondWith(try reply.encode(temp_allocator.allocator()), &event_queue, .{});
                                allocator.destroy(pending_wait);
                            } else {
                                try new_pending_waits.add(pending_wait);
                            }
                        } else {
                            try new_pending_waits.add(pending_wait);
                        }
                    }

                    event_fsm.global_data.pending_waits.deinit();
                    event_fsm.global_data.pending_waits = new_pending_waits;

                    connection_fsm.state = .in_sync;
                    continue :event_loop;
                }

                std.debug.assert(connection_fsm.state == .waiting_for_commands or connection_fsm.state == .executing_transaction);

                var n: usize = 0;
                const buffer_size = @as(usize, @intCast(completed_event.async_result));
                var n_new_commands: usize = 0;

                var temp_allocator = std.heap.ArenaAllocator.init(allocator);
                defer temp_allocator.deinit();

                while (buffer_size - n > 0) {
                    const command, const parsed_bytes = Command.parse(connection_fsm.buffer[n..], allocator) catch |err| {
                        event_loop_logger.err("Error while parsing command: {}", .{err});
                        event_loop_logger.debug("Bytes on the wire: {x} (return value: {d})", .{ connection_fsm.buffer, completed_event.async_result });

                        try event_fsm.notify(&event_queue, n_new_commands);
                        try event_fsm.respondWith(try resp.SimpleError(try cmd.errorToString(err)).encode(temp_allocator.allocator()), &event_queue, .{});
                        continue :event_loop;
                    };

                    if (command.type == .exec) {
                        if (connection_fsm.state != .executing_transaction) {
                            try event_fsm.respondWith(try resp.SimpleError("EXEC without MULTI").encode(temp_allocator.allocator()), &event_queue, .{});

                            continue :event_loop;
                        }

                        try event_fsm.notify(&event_queue, 1);
                        continue :event_loop;
                    }

                    if (command.type == .discard) {
                        if (connection_fsm.state != .executing_transaction) {
                            try event_fsm.respondWith(try resp.SimpleError("DISCARD without MULTI").encode(temp_allocator.allocator()), &event_queue, .{});
                            continue :event_loop;
                        }
                        connection_fsm.flushCommandsQueue();
                        try event_fsm.respondWith(try resp.Ok.encode(temp_allocator.allocator()), &event_queue, .{});
                        continue :event_loop;
                    }

                    try connection_fsm.addCommand(command);
                    n_new_commands += 1;
                    n += parsed_bytes;
                }

                std.debug.assert(n_new_commands > 0);
                if (connection_fsm.state != .executing_transaction) {
                    connection_fsm.state = .executing_commands;
                    try event_fsm.notify(&event_queue, n_new_commands);
                } else {
                    try event_fsm.respondWith(try resp.SimpleString("QUEUED").encode(temp_allocator.allocator()), &event_queue, .{ .new_state = .executing_transaction });
                }
            },
            .read => {
                std.debug.assert(event_fsm.type == .connection);
                const connection_fsm = &event_fsm.type.connection;

                if (connection_fsm.state != .executing_commands and connection_fsm.state != .executing_transaction) {
                    event_loop_logger.debug("Maybe I'm replying, come back later", .{});
                    try event_fsm.notify(&event_queue, 1);
                    continue :event_loop;
                }

                var temp_allocator = std.heap.ArenaAllocator.init(allocator);
                defer temp_allocator.deinit();

                switch (connection_fsm.state) {
                    .executing_commands => {
                        if (connection_fsm.popCommand()) |*command| {
                            defer @constCast(command).deinit();
                            switch (command.type) {
                                .wait => |wait_command| {
                                    if (wait_command.num_replicas == 0) {
                                        try event_fsm.respondWith(try resp.Integer(event_fsm.global_data.slaves.count()).encode(temp_allocator.allocator()), &event_queue, .{});
                                    } else {
                                        var count: usize = 0;
                                        const num_replicas_threshold = command.type.wait.num_replicas;
                                        const timeout_timestamp = std.time.milliTimestamp() + @as(i64, @bitCast(command.type.wait.timeout_ms));

                                        const timeout_fd = try util.setupTimer(command.type.wait.timeout_ms);

                                        var slaves_it = event_fsm.global_data.slaves.iterator();
                                        while (slaves_it.next()) |slave| {
                                            const slave_repl_offset = slave.key_ptr.*.type.connection.peer_type.slave.repl_offset;
                                            if (slave_repl_offset >= instance.repl_offset) {
                                                count += 1;
                                            }
                                        }

                                        if (count >= num_replicas_threshold) {
                                            try event_fsm.respondWith(try resp.Integer(@bitCast(count)).encode(temp_allocator.allocator()), &event_queue, .{});
                                            continue :event_loop;
                                        }

                                        // We didn't exit early, we must ask the slaves for their offset and block
                                        slaves_it = event_fsm.global_data.slaves.iterator();
                                        while (slaves_it.next()) |slave| {
                                            try slave.key_ptr.*.requestOffset(&event_queue);
                                        }

                                        const pending_wait = try allocator.create(fsm.PendingWait);
                                        pending_wait.timeout = timeout_timestamp;
                                        pending_wait.client_connection_fsm = event_fsm;
                                        pending_wait.threshold_offset = instance.repl_offset;
                                        pending_wait.expected_n_replicas = num_replicas_threshold;
                                        pending_wait.actual_n_replicas = count;
                                        pending_wait.timerfd = timeout_fd;

                                        connection_fsm.state = .{ .blocked = pending_wait };

                                        try event_fsm.global_data.pending_waits.add(pending_wait);

                                        const timeout_event = fsm.Event{ .type = .{ .pollin = timeout_fd }, .user_data = event_fsm };

                                        try event_queue.addAsyncEvent(timeout_event);
                                    }
                                },
                                .psync => {
                                    connection_fsm.peer_type = .{ .slave = .{ .repl_offset = 0 } };
                                    try event_fsm.global_data.slaves.put(event_fsm, undefined);
                                    var dump = [_]u8{0} ** fsm.CLIENT_BUFFER_SIZE;
                                    const dump_size = try instance.dumpToBuffer(&dump);
                                    // Why 0 for the repl_offset? Codecrafters really
                                    try event_fsm.respondWith(try std.fmt.allocPrint(temp_allocator.allocator(), "+FULLRESYNC {[replid]s} {[repl_offset]d}\r\n${[dump_size]d}\r\n{[dump]s}", .{ .replid = instance.replid, .repl_offset = 0, .dump_size = dump_size, .dump = dump[0..dump_size] }), &event_queue, .{ .new_state = .sending_dump });
                                },
                                .multi => {
                                    try event_fsm.respondWith(try resp.Ok.encode(temp_allocator.allocator()), &event_queue, .{ .new_state = .executing_transaction });
                                    continue :event_loop;
                                },
                                else => {
                                    if (connection_fsm.peer_type == .master or (command.shouldPropagate() and instance.master == null))
                                        instance.repl_offset += command.bytes.len;
                                    const reply = instance.executeCommand(temp_allocator.allocator(), command) catch {
                                        try event_fsm.respondWith(try resp.SimpleError("Some error occurred during command execution").encode(temp_allocator.allocator()), &event_queue, .{});
                                        continue :event_loop;
                                    };

                                    if (command.type == .xread and command.type.xread.block_timeout_ms != null and std.meta.eql(reply, resp.Null)) {
                                        var timerfd: ?posix.fd_t = null;
                                        if (command.type.xread.block_timeout_ms) |timeout_ms| {
                                            timerfd = try util.setupTimer(timeout_ms);
                                        }
                                        var blocked_stream_read = try allocator.create(fsm.BlockedStreamRead);
                                        blocked_stream_read.* = fsm.BlockedStreamRead.init(allocator, event_fsm, timerfd);
                                        for (command.type.xread.requests) |request| {
                                            try blocked_stream_read.addStream(request.stream_key);
                                        }

                                        for (command.type.xread.requests) |request| {
                                            const blocked_stream_read_queue = try event_fsm.global_data.blocked_xreads.getOrPut(try allocator.dupe(u8, request.stream_key));
                                            if (!blocked_stream_read_queue.found_existing) {
                                                blocked_stream_read_queue.value_ptr.* = try allocator.create(std.AutoArrayHashMap(*fsm.BlockedStreamRead, void));
                                                blocked_stream_read_queue.value_ptr.*.* = std.AutoArrayHashMap(*fsm.BlockedStreamRead, void).init(allocator);
                                            }

                                            try blocked_stream_read_queue.value_ptr.*.put(blocked_stream_read, {});
                                        }

                                        connection_fsm.state = .{ .waiting_for_new_data_on_stream = blocked_stream_read };

                                        if (timerfd) |fd| {
                                            const timeout_event = fsm.Event{ .type = .{ .pollin = fd }, .user_data = event_fsm };
                                            try event_queue.addAsyncEvent(timeout_event);
                                        }

                                        continue :event_loop;
                                    }

                                    if (command.type == .xadd) {
                                        const xadd_command = command.type.xadd;
                                        if (event_fsm.global_data.blocked_xreads.get(xadd_command.stream_key)) |blocked_stream_reads| {
                                            for (blocked_stream_reads.keys()) |blocked_stream_read| {
                                                defer allocator.destroy(blocked_stream_read);
                                                defer blocked_stream_read.deinit();

                                                var response = std.ArrayList(resp.Value).init(temp_allocator.allocator());
                                                try response.append(resp.BulkString(xadd_command.stream_key));
                                                var entry_elements = std.ArrayList(resp.Value).init(temp_allocator.allocator());
                                                for (xadd_command.key_value_pairs) |pair| {
                                                    try entry_elements.append(resp.BulkString(pair.key));
                                                    try entry_elements.append(resp.BulkString(pair.value));
                                                }

                                                var tmp_array = try temp_allocator.allocator().alloc(resp.Value, 2);
                                                tmp_array[0] = reply;
                                                tmp_array[1] = resp.Array(try entry_elements.toOwnedSlice());
                                                try response.append(resp.Array(&[_]resp.Value{resp.Array(tmp_array)}));

                                                try blocked_stream_read.client_connection_fsm.respondWith(try resp.Array(&[_]resp.Value{resp.Array(try response.toOwnedSlice())}).encode(temp_allocator.allocator()), &event_queue, .{});
                                            }

                                            if (event_fsm.global_data.blocked_xreads.getKey(xadd_command.stream_key)) |stream_key| {
                                                event_fsm.global_data.blocked_xreads.get(xadd_command.stream_key).?.deinit();
                                                allocator.destroy(event_fsm.global_data.blocked_xreads.get(xadd_command.stream_key).?);
                                                _ = event_fsm.global_data.blocked_xreads.remove(stream_key);
                                                allocator.free(stream_key);
                                            }
                                        }
                                    }

                                    if (command.shouldPropagate() and instance.master == null) {
                                        var slaves_it = event_fsm.global_data.slaves.keyIterator();
                                        while (slaves_it.next()) |slave_fsm| {
                                            try slave_fsm.*.propagateCommand(&event_queue, command.*);
                                        }
                                    }

                                    if (connection_fsm.peer_type == .master and (command.type != .replconf or command.type.replconf != .getack)) {
                                        if (connection_fsm.commands_to_execute.len == 0) {
                                            try event_fsm.waitForCommand(&event_queue, .{});
                                        } else {
                                            try event_fsm.executeCommands(&event_queue);
                                        }
                                        continue :event_loop;
                                    }

                                    try event_fsm.respondWith(try reply.encode(temp_allocator.allocator()), &event_queue, .{});
                                },
                            }
                        } else try event_fsm.waitForCommand(&event_queue, .{});
                    },
                    .executing_transaction => {
                        var replies = std.ArrayList(resp.Value).init(temp_allocator.allocator());
                        while (connection_fsm.popCommand()) |*command| {
                            defer @constCast(command).deinit();
                            const reply = instance.executeCommand(temp_allocator.allocator(), command) catch {
                                try event_fsm.respondWith(try resp.SimpleError("Some error occurred during transaction execution").encode(temp_allocator.allocator()), &event_queue, .{});
                                continue :event_loop;
                            };
                            try replies.append(reply);
                        }
                        try event_fsm.respondWith(try resp.Array(try replies.toOwnedSlice()).encode(temp_allocator.allocator()), &event_queue, .{});
                    },
                    else => unreachable,
                }
            },
            .send => {
                switch (event_fsm.type) {
                    .server => @panic("Not ready yet"),
                    .connection => |*connection_fsm| {
                        switch (connection_fsm.state) {
                            .sending_getack => {
                                connection_fsm.state = .waiting_for_ack;
                                util.resizeBuffer(&connection_fsm.buffer, fsm.CLIENT_BUFFER_SIZE);
                                @memset(connection_fsm.buffer, 0);
                                const new_command_event = fsm.Event{
                                    .type = .{ .recv = .{ .fd = connection_fsm.fd, .buffer = connection_fsm.buffer } },
                                    .user_data = event_fsm,
                                };

                                try event_queue.addAsyncEvent(new_command_event);
                            },
                            .sending_response => {
                                if (connection_fsm.commands_to_execute.len > 0) {
                                    // There are commands to execute, go work on those
                                    try event_fsm.executeCommands(&event_queue);
                                } else try event_fsm.waitForCommand(&event_queue, .{});
                            },
                            .sending_dump, .propagating_command => {
                                connection_fsm.state = .in_sync;
                            },
                            .executing_transaction => {
                                try event_fsm.waitForCommand(&event_queue, .{ .new_state = .executing_transaction });
                            },
                            else => @panic("Invalid state transition"),
                        }
                    },
                }
            },
            .notify => {
                std.debug.assert(event_fsm.type == .connection);

                const state = event_fsm.type.connection.state;

                try event_fsm.executeCommands(&event_queue);
                if (state == .executing_transaction)
                    event_fsm.type.connection.state = .executing_transaction;
            },
            .pollin => |timerfd| {
                switch (event_fsm.type) {
                    .server => {
                        event_loop_logger.info("Gracefully shutting down...", .{});
                        var slaves_it = event_fsm.global_data.slaves.keyIterator();
                        while (slaves_it.next()) |slave_fsm| {
                            slave_fsm.*.deinit();
                        }
                        break;
                    },
                    .connection => |*connection_fsm| { // Blocked XREAD or WAIT timeout
                        var temp_allocator = std.heap.ArenaAllocator.init(allocator);
                        defer temp_allocator.deinit();

                        // Is it a pending wait?
                        var pending_wait_it = event_fsm.global_data.pending_waits.iterator();

                        var maybe_index: ?usize = null;
                        var i: usize = 0;

                        var indicesToRemove = std.ArrayList(usize).init(temp_allocator.allocator());
                        while (pending_wait_it.next()) |pw| : (i += 1) {
                            if (timerfd == pw.timerfd and connection_fsm.state == .blocked and connection_fsm.state.blocked == pw) {
                                maybe_index = i;
                            }
                            if (pw.client_connection_fsm == event_fsm and (connection_fsm.state != .blocked or connection_fsm.state.blocked != pw)) { // late alarm
                                try indicesToRemove.append(i);
                            }
                        }

                        for (indicesToRemove.items) |index| {
                            const pending_wait = event_fsm.global_data.pending_waits.removeIndex(index);
                            allocator.destroy(pending_wait);
                            posix.close(completed_event.awaited_event.type.pollin); // the timerfd
                        }

                        if (maybe_index) |index| {
                            const pending_wait = event_fsm.global_data.pending_waits.removeIndex(index);

                            const reply = resp.Integer(@bitCast(pending_wait.actual_n_replicas));
                            try event_fsm.respondWith(try reply.encode(temp_allocator.allocator()), &event_queue, .{});
                            allocator.destroy(pending_wait);
                            posix.close(completed_event.awaited_event.type.pollin); // the timerfd
                            continue :event_loop;
                        }

                        if (indicesToRemove.items.len > 0)
                            continue :event_loop; // it was a pending wait, go back to handling events

                        // Is it a blocked xread?
                        if (connection_fsm.state == .waiting_for_new_data_on_stream) {
                            const blocked_xread = connection_fsm.state.waiting_for_new_data_on_stream;
                            defer allocator.destroy(blocked_xread);
                            defer blocked_xread.deinit();
                            for (blocked_xread.streams.items) |stream| {
                                if (event_fsm.global_data.blocked_xreads.getKey(stream)) |stream_key| {
                                    var blocked_reads = event_fsm.global_data.blocked_xreads.get(stream_key).?;
                                    _ = blocked_reads.swapRemove(blocked_xread);
                                    if (blocked_reads.count() == 0) {
                                        blocked_reads.deinit();
                                        allocator.destroy(blocked_reads);
                                        allocator.free(stream_key);
                                    }
                                }
                            }
                            try blocked_xread.client_connection_fsm.respondWith(try resp.Null.encode(temp_allocator.allocator()), &event_queue, .{});
                        } else { // Is it a late timeout that needs to be cleaned up?
                            var blocked_xreads_it = event_fsm.global_data.blocked_xreads.iterator();
                            var streams_to_remove = std.ArrayList([]const u8).init(temp_allocator.allocator());
                            while (blocked_xreads_it.next()) |blocked_xread| {
                                const stream = blocked_xread.key_ptr.*;
                                const stream_blocked_reads = blocked_xread.value_ptr.*;
                                var keys_to_remove = std.ArrayList(*fsm.BlockedStreamRead).init(temp_allocator.allocator());
                                for (stream_blocked_reads.keys()) |read| {
                                    if (read.client_connection_fsm == event_fsm and read.timerfd != null and read.timerfd == timerfd) {
                                        try keys_to_remove.append(read);
                                    }
                                }
                                for (keys_to_remove.items) |key| {
                                    _ = stream_blocked_reads.swapRemove(key);
                                }
                                if (stream_blocked_reads.count() == 0) {
                                    stream_blocked_reads.deinit();
                                    allocator.destroy(stream_blocked_reads);
                                    try streams_to_remove.append(stream);
                                }
                            }
                            for (streams_to_remove.items) |stream| {
                                const stream_key = event_fsm.global_data.blocked_xreads.getKey(stream).?;
                                _ = event_fsm.global_data.blocked_xreads.remove(stream_key);
                                allocator.free(stream_key);
                            }
                        }
                    },
                }
            },
        }
    }
    return 0;
}
