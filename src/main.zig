const std = @import("std");
const resp = @import("resp.zig");
const cmd = @import("command.zig");
const fsm = @import("fsm.zig");
const Command = cmd.Command;
const db = @import("db.zig");
const cfg = @import("config.zig");
const util = @import("util.zig");
const RcFSM = @import("fsm.zig").RcFSM;
const st = @import("fsm.zig").StateTransitions;

const posix = std.posix;
const linux = std.os.linux;

const stdout = std.io.getStdOut().writer();

const IO_URING_ENTRIES = 100;

pub const std_options: std.Options = .{
    // Define logFn to override the std implementation
    .log_level = .info,
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
    const main_allocator = gpa.allocator();

    // INFO: Command line parsing
    const stderr = std.io.getStdErr();
    // Needs to live until we initialize the DB below
    var config = cfg.parseCommandLineArguments(main_allocator) catch |err| {
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
    const server_fsm = try RcFSM.init(main_allocator, try fsm.FSM.newServer(main_allocator, address, port));
    defer server_fsm.release();

    std.log.scoped(.init).info("Listening on {s}:{d}...", .{ address, port });

    // INFO: Event queue init
    var event_queue = try fsm.EventQueue.init(main_allocator, IO_URING_ENTRIES);

    defer event_queue.destroy() catch {
        @panic("Failed destroying the event queue");
    };

    const new_connection_event = fsm.Event{ .type = .{ .accept = server_fsm.get().type.server.socket }, .user_data = server_fsm.clone() };
    try server_fsm.get().pending_event_identifiers.put(try event_queue.addAsyncEvent(new_connection_event), undefined);

    const termination_event = fsm.Event{ .type = .{ .pollin = server_fsm.get().type.server.signalfd }, .user_data = server_fsm.clone() };
    try server_fsm.get().pending_event_identifiers.put(try event_queue.addAsyncEvent(termination_event), undefined);

    // INFO: DB init
    var instance = db.Instance.init(main_allocator, config) catch |err| {
        std.log.scoped(.init).err("Datastore initialization failed: {!}", .{err});
        config.deinit();
        return err;
    };
    defer instance.destroy(main_allocator);
    config.deinit();

    var master_server_fsm: ?RcFSM = null;

    if (instance.master) |master_fd| {
        master_server_fsm = try RcFSM.init(server_fsm.get().allocator, try fsm.FSM.newConnection(server_fsm.get().allocator, master_fd, .master, server_fsm.get().global_data));
        try st.waitForCommand(master_server_fsm.?, &event_queue, .{});
    }

    const event_loop_logger = std.log.scoped(.event_loop);
    var shutting_down = false;
    event_loop: while (true) {
        event_loop_logger.debug("Waiting for something to come through...", .{});
        const completed_event = try event_queue.next();
        const event_fsm: RcFSM = completed_event.awaited_event.user_data;
        defer event_fsm.release();
        _ = event_fsm.get().pending_event_identifiers.remove(completed_event.event_id);
        event_loop_logger.debug("New event: {any}", .{completed_event});
        if (shutting_down and event_queue.pending_events == 0) {
            break;
        }
        if (!event_fsm.get().alive or shutting_down) { // e.g. a late timeout on a FSM for a
            // peer that has already disconnected. The reference count will be
            // decreased at the end of the loop
            continue :event_loop;
        }
        switch (completed_event.awaited_event.type) {
            .cancel => { // Nothing to do, the reference count will be decreased at the end of the loop
            },
            .accept => {
                std.debug.assert(event_fsm.get().type == .server);
                const connection_socket: posix.socket_t = completed_event.async_result;
                event_loop_logger.info("Accepted new connection", .{});
                const new_connection_fsm = try RcFSM.init(event_fsm.get().allocator, try fsm.FSM.newConnection(event_fsm.get().allocator, connection_socket, .generic_client, event_fsm.get().global_data));
                defer new_connection_fsm.release();
                try st.waitForCommand(new_connection_fsm, &event_queue, .{});

                // Re-queing the accept event
                const connection_event = fsm.Event{ .type = .{ .accept = event_fsm.get().type.server.socket }, .user_data = event_fsm.clone() };
                try event_fsm.get().pending_event_identifiers.put(try event_queue.addAsyncEvent(connection_event), undefined);
            },
            .recv => {
                std.debug.assert(event_fsm.get().type == .connection);
                const connection_fsm = event_fsm.get().type.connection;

                if (completed_event.async_result == 0) { // connection closed
                    if (instance.master != null and connection_fsm.peer_type == .master and instance.diewithmaster) {
                        event_loop_logger.info("Master closed connection, gracefully shutting down", .{});
                        try st.shutdown(server_fsm, &event_queue);
                        master_server_fsm.?.release();
                        shutting_down = true;
                    }
                    try st.shutdown(event_fsm, &event_queue);
                    continue :event_loop;
                }

                if (completed_event.async_result < 0) { // error
                    event_loop_logger.err("Error: {} {d}", .{ linux.E.init(@intCast(@as(u32, @bitCast(completed_event.async_result)))), completed_event.async_result });
                    try st.shutdown(event_fsm, &event_queue);
                    continue :event_loop;
                }

                // Slave replica offset update
                if (connection_fsm.peer_type == .slave and connection_fsm.state == .waiting_for_ack) {
                    var temp_allocator = std.heap.ArenaAllocator.init(main_allocator);
                    defer temp_allocator.deinit();

                    const response, _ = resp.Value.parse(connection_fsm.buffer, temp_allocator.allocator()) catch {
                        event_loop_logger.debug("Got some invalid bytes on the wire: {?x}", .{connection_fsm.buffer});
                        try st.respondWith(event_fsm, try resp.SimpleError("Invalid bytes on the wire").encode(temp_allocator.allocator()), &event_queue, .{});
                        continue :event_loop;
                    };

                    std.debug.assert(response == .array);

                    // Response should be of the form: REPLCONF ACK <offset>
                    const new_offset = try std.fmt.parseInt(usize, response.array[2].bulk_string, 10);

                    try event_fsm.get().updateSlaveOffset(new_offset, &event_queue);

                    connection_fsm.state = .in_sync;
                    continue :event_loop;
                }

                std.debug.assert(connection_fsm.state == .waiting_for_commands or connection_fsm.state == .executing_transaction);

                var n: usize = 0;
                const buffer_size = @as(usize, @intCast(completed_event.async_result));
                var n_new_commands: usize = 0;

                var temp_allocator = std.heap.ArenaAllocator.init(main_allocator);
                defer temp_allocator.deinit();

                while (buffer_size - n > 0) {
                    const command, const parsed_bytes = Command.parse(connection_fsm.buffer[n..], main_allocator) catch |err| {
                        event_loop_logger.err("Error while parsing command: {}", .{err});
                        event_loop_logger.debug("Bytes on the wire: {x} (return value: {d})", .{ connection_fsm.buffer, completed_event.async_result });

                        try fsm.FSM.notify(event_fsm, &event_queue, n_new_commands);
                        try st.respondWith(event_fsm, try resp.SimpleError(try cmd.errorToString(err)).encode(temp_allocator.allocator()), &event_queue, .{});
                        continue :event_loop;
                    };

                    // Execute transaction
                    if (command.type == .exec) {
                        @constCast(&command).deinit();
                        if (connection_fsm.state != .executing_transaction) {
                            try st.respondWith(event_fsm, try resp.SimpleError("EXEC without MULTI").encode(temp_allocator.allocator()), &event_queue, .{});
                            continue :event_loop;
                        }

                        try fsm.FSM.notify(event_fsm, &event_queue, 1);
                        continue :event_loop;
                    }

                    // Cancel transaction
                    if (command.type == .discard) {
                        @constCast(&command).deinit();
                        if (connection_fsm.state != .executing_transaction) {
                            try st.respondWith(event_fsm, try resp.SimpleError("DISCARD without MULTI").encode(temp_allocator.allocator()), &event_queue, .{});
                            continue :event_loop;
                        }
                        connection_fsm.flushCommandsQueue();
                        try st.respondWith(event_fsm, try resp.Ok.encode(temp_allocator.allocator()), &event_queue, .{});
                        continue :event_loop;
                    }

                    try connection_fsm.addCommand(command);
                    n_new_commands += 1;
                    n += parsed_bytes;
                }

                std.debug.assert(n_new_commands > 0);
                if (connection_fsm.state != .executing_transaction) {
                    connection_fsm.state = .executing_commands;
                    try fsm.FSM.notify(event_fsm, &event_queue, n_new_commands);
                } else {
                    try st.respondWith(event_fsm, try resp.SimpleString("QUEUED").encode(temp_allocator.allocator()), &event_queue, .{ .new_state = .executing_transaction });
                }
            },
            .read => {
                std.debug.assert(event_fsm.get().type == .connection);
                const connection_fsm = event_fsm.get().type.connection;

                if (connection_fsm.state != .executing_commands and connection_fsm.state != .executing_transaction) {
                    event_loop_logger.debug("Maybe I'm replying, come back later", .{});
                    try fsm.FSM.notify(event_fsm, &event_queue, 1);
                    continue :event_loop;
                }

                var temp_allocator = std.heap.ArenaAllocator.init(main_allocator);
                defer temp_allocator.deinit();

                switch (connection_fsm.state) {
                    .executing_commands => {
                        if (connection_fsm.popCommand()) |*command| {
                            defer @constCast(command).deinit();
                            switch (command.type) {
                                .wait => |wait_command| {
                                    if (wait_command.num_replicas == 0) { // No need to block
                                        try st.respondWith(event_fsm, try resp.Integer(event_fsm.get().global_data.slaves.count()).encode(temp_allocator.allocator()), &event_queue, .{});
                                        continue :event_loop;
                                    }

                                    // We might need to block. The timerfd is
                                    // set up now just in case, for better time
                                    // accuracy
                                    var count: usize = 0;
                                    const num_replicas_threshold = command.type.wait.num_replicas;
                                    const timeout_timestamp = std.time.milliTimestamp() + @as(i64, @bitCast(command.type.wait.timeout_ms));

                                    const timeout_fd = try util.setupTimer(command.type.wait.timeout_ms);

                                    var slaves_it = event_fsm.get().global_data.slaves.iterator();
                                    while (slaves_it.next()) |slave| {
                                        const slave_repl_offset = slave.key_ptr.*.type.connection.peer_type.slave.repl_offset;
                                        if (slave_repl_offset >= instance.repl_offset) {
                                            count += 1;
                                        }
                                    }

                                    if (count >= num_replicas_threshold) {
                                        try st.respondWith(event_fsm, try resp.Integer(@bitCast(count)).encode(temp_allocator.allocator()), &event_queue, .{});
                                        continue :event_loop;
                                    }

                                    // We didn't exit early, we must ask the slaves for their offset and block
                                    slaves_it = event_fsm.get().global_data.slaves.iterator();
                                    while (slaves_it.next()) |slave| {
                                        try fsm.FSM.requestOffset(slave.value_ptr.*, &event_queue);
                                    }

                                    try st.setWaiting(event_fsm, timeout_timestamp, instance.repl_offset, num_replicas_threshold, count, timeout_fd, &event_queue);
                                },
                                .psync => {
                                    connection_fsm.peer_type = .{ .slave = .{ .repl_offset = 0 } };
                                    try event_fsm.get().global_data.slaves.put(event_fsm.get(), event_fsm.clone());
                                    var dump = [_]u8{0} ** fsm.CLIENT_BUFFER_SIZE;
                                    const dump_size = try instance.dumpToBuffer(&dump);
                                    // Why 0 for the repl_offset? Codecrafters really
                                    try st.respondWith(event_fsm, try std.fmt.allocPrint(temp_allocator.allocator(), "+FULLRESYNC {[replid]s} {[repl_offset]d}\r\n${[dump_size]d}\r\n{[dump]s}", .{ .replid = instance.replid, .repl_offset = 0, .dump_size = dump_size, .dump = dump[0..dump_size] }), &event_queue, .{ .new_state = .sending_dump });
                                },
                                .multi => {
                                    try st.respondWith(event_fsm, try resp.Ok.encode(temp_allocator.allocator()), &event_queue, .{ .new_state = .executing_transaction });
                                    continue :event_loop;
                                },
                                else => {
                                    if (connection_fsm.peer_type == .master or (command.shouldPropagate() and instance.master == null))
                                        instance.repl_offset += command.bytes.len;

                                    if (command.type == .blpop) {
                                        const blpop_command = command.type.blpop;
                                        const maybe_element = instance.pop(temp_allocator.allocator(), blpop_command.key) catch {
                                            try st.respondWith(event_fsm, try resp.SimpleError("Some error occurred during command execution").encode(temp_allocator.allocator()), &event_queue, .{});
                                            continue :event_loop;
                                        };

                                        // No need to block
                                        if (maybe_element) |element| {
                                            var reply = resp.Array(&[_]resp.Value{ resp.BulkString(blpop_command.key), resp.BulkString(element) });

                                            try st.respondWith(event_fsm, try reply.encode(temp_allocator.allocator()), &event_queue, .{});
                                            continue :event_loop;
                                        }

                                        // We need to block
                                        var timerfd: ?posix.fd_t = null;
                                        if (blpop_command.timeout_s > 0)
                                            timerfd = try util.setupTimer(@as(usize, @intFromFloat(blpop_command.timeout_s * 1000)));
                                        try st.setWaitingOnListPush(event_fsm, blpop_command.key, timerfd, &event_queue);
                                        continue :event_loop;
                                    }

                                    const reply = instance.executeCommand(temp_allocator.allocator(), command) catch {
                                        try st.respondWith(event_fsm, try resp.SimpleError("Some error occurred during command execution").encode(temp_allocator.allocator()), &event_queue, .{});
                                        continue :event_loop;
                                    };

                                    if (command.type == .xread and command.type.xread.block_timeout_ms != null and std.meta.eql(reply, resp.Null)) {
                                        var timerfd: ?posix.fd_t = null;
                                        if (command.type.xread.block_timeout_ms) |timeout_ms| {
                                            if (timeout_ms > 0)
                                                timerfd = try util.setupTimer(timeout_ms);
                                        }

                                        try st.setWaitingOnStreamUpdate(event_fsm, command.type.xread.requests, timerfd, &event_queue);
                                        continue :event_loop;
                                    }

                                    if (command.type == .xadd) {
                                        const xadd_command = command.type.xadd;
                                        try event_fsm.get().unblockAllClientsWaitingOnStream(&xadd_command, reply, &event_queue);
                                    }

                                    if (command.type == .list_push) {
                                        const push_command = command.type.list_push;
                                        try event_fsm.get().unblockOneClientWaitingOnList(push_command.key, &instance, &event_queue);
                                    }

                                    if (command.shouldPropagate() and instance.master == null) {
                                        var slaves_it = event_fsm.get().global_data.slaves.iterator();
                                        while (slaves_it.next()) |slave_fsm_entry| {
                                            try fsm.FSM.propagateCommand(slave_fsm_entry.value_ptr.*, &event_queue, command.*);
                                        }
                                    }

                                    if (connection_fsm.peer_type == .master and (command.type != .replconf or command.type.replconf != .getack)) {
                                        if (connection_fsm.commands_to_execute.len == 0) {
                                            try st.waitForCommand(event_fsm, &event_queue, .{});
                                        } else {
                                            try st.executeCommands(event_fsm, &event_queue);
                                        }
                                        continue :event_loop;
                                    }

                                    try st.respondWith(event_fsm, try reply.encode(temp_allocator.allocator()), &event_queue, .{});
                                },
                            }
                        } else try st.waitForCommand(event_fsm, &event_queue, .{});
                    },
                    .executing_transaction => {
                        var replies = std.ArrayList(resp.Value).init(temp_allocator.allocator());
                        while (connection_fsm.popCommand()) |*command| {
                            defer @constCast(command).deinit();
                            const reply = instance.executeCommand(temp_allocator.allocator(), command) catch {
                                try st.respondWith(event_fsm, try resp.SimpleError("Some error occurred during transaction execution").encode(temp_allocator.allocator()), &event_queue, .{});
                                continue :event_loop;
                            };
                            try replies.append(reply);
                        }
                        try st.respondWith(event_fsm, try resp.Array(try replies.toOwnedSlice()).encode(temp_allocator.allocator()), &event_queue, .{});
                    },
                    else => unreachable,
                }
            },
            .send => {
                switch (event_fsm.get().type) {
                    .server => @panic("Not ready yet"),
                    .connection => |connection_fsm| {
                        // What were we doing with this connection before the send completed?
                        switch (connection_fsm.state) {
                            .sending_getack => {
                                connection_fsm.state = .waiting_for_ack;
                                util.resizeBuffer(&connection_fsm.buffer, fsm.CLIENT_BUFFER_SIZE);
                                @memset(connection_fsm.buffer, 0);
                                const new_command_event = fsm.Event{
                                    .type = .{ .recv = .{ .fd = connection_fsm.fd, .buffer = connection_fsm.buffer } },
                                    .user_data = event_fsm.clone(),
                                };

                                try event_fsm.get().pending_event_identifiers.put(try event_queue.addAsyncEvent(new_command_event), undefined);
                            },
                            .sending_response => {
                                if (connection_fsm.commands_to_execute.len > 0) {
                                    // There are commands to execute, go work on those
                                    try st.executeCommands(event_fsm, &event_queue);
                                } else try st.waitForCommand(event_fsm, &event_queue, .{});
                            },
                            .sending_dump, .propagating_command => {
                                connection_fsm.state = .in_sync;
                            },
                            .executing_transaction => {
                                try st.waitForCommand(event_fsm, &event_queue, .{ .new_state = .executing_transaction });
                            },
                            else => @panic("Invalid state transition"),
                        }
                    },
                }
            },
            .notify => {
                std.debug.assert(event_fsm.get().type == .connection);

                const state = event_fsm.get().type.connection.state;

                try st.executeCommands(event_fsm, &event_queue);
                if (state == .executing_transaction)
                    event_fsm.get().type.connection.state = .executing_transaction;
            },
            .pollin => |timerfd| {
                switch (event_fsm.get().type) {
                    .server => {
                        event_loop_logger.info("Gracefully shutting down...", .{});
                        if (master_server_fsm) |master_fsm| {
                            try st.shutdown(master_fsm, &event_queue);
                            master_fsm.release();
                        } else {
                            var slaves_it = event_fsm.get().global_data.slaves.iterator();
                            while (slaves_it.next()) |slave_fsm_entry| {
                                try st.shutdown(slave_fsm_entry.value_ptr.*, &event_queue);
                                slave_fsm_entry.value_ptr.release();
                            }
                        }

                        shutting_down = true;
                        try st.shutdown(event_fsm, &event_queue);
                    },
                    .connection => { // Blocked XREAD, BLPOP, or WAIT timeout
                        const timeout = event_fsm.get().classifyTimeout(timerfd) orelse {
                            // There was no pending timeout that matched this
                            // timerfd and FSM, which means it was already
                            // cleaned up during un unblocking operation
                            // earlier
                            // We can just continue
                            continue :event_loop;
                        };

                        switch (timeout.kind) {
                            .PENDING_WAIT => {
                                if (timeout.valid) {
                                    try event_fsm.get().unblockWaitingClient(timerfd, &event_queue);
                                } else {
                                    try event_fsm.get().cleanupLateWaitTimeout(timerfd);
                                }
                            },
                            .BLOCKED_XREAD => {
                                if (timeout.valid) {
                                    try event_fsm.get().unblockStreamReadClient(&event_queue);
                                } else {
                                    try event_fsm.get().cleanupLateStreamReadTimeout(timerfd);
                                }
                            },
                            .BLOCKED_BLPOP => {
                                if (timeout.valid) {
                                    try event_fsm.get().unblockBlpopClient(timerfd, &event_queue);
                                } else {
                                    try event_fsm.get().cleanupLateBlpopTimeout(timerfd);
                                }
                            },
                        }
                    },
                }
            },
        }
    }
    return 0;
}
