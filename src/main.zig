const std = @import("std");
const eq = @import("event_queue.zig");
const resp = @import("resp.zig");
const cmd = @import("command.zig");
const fsm = @import("fsm.zig");
const Command = cmd.Command;
const db = @import("db.zig");
const resizeBuffer = @import("util.zig").resizeBuffer;

const net = std.net;
const posix = std.posix;
const linux = std.os.linux;
const Allocator = std.mem.Allocator;

const stdout = std.io.getStdOut().writer();

const IO_URING_ENTRIES = 100;

const EventQueue = eq.EventQueue(fsm.FSM);
const Event = eq.Event(fsm.FSM);

fn respondWith(response: []const u8, finite_state_machine: *fsm.FSM, event_queue: *EventQueue, new_state: struct { new_state: @TypeOf(finite_state_machine.type.connection.state) = .sending_response }) !void {
    if (finite_state_machine.type != .connection)
        return error.InvalidFSM;
    const connection_fsm = &finite_state_machine.type.connection;
    resizeBuffer(&connection_fsm.buffer, fsm.CLIENT_BUFFER_SIZE);
    connection_fsm.buffer = try std.fmt.bufPrint(connection_fsm.buffer, "{s}", .{response});
    connection_fsm.state = new_state.new_state;
    const response_sent_event = Event{
        .type = .{ .send = .{ .fd = connection_fsm.fd, .buffer = connection_fsm.buffer } },
        .user_data = finite_state_machine,
    };
    try event_queue.addAsyncEvent(response_sent_event);
}

fn waitForCommand(finite_state_machine: *fsm.FSM, event_queue: *EventQueue, new_state: struct { new_state: @TypeOf(finite_state_machine.type.connection.state) = .waiting_for_commands }) !void {
    if (finite_state_machine.type != .connection)
        return error.InvalidFSM;
    const connection_fsm = &finite_state_machine.type.connection;
    resizeBuffer(&connection_fsm.buffer, fsm.CLIENT_BUFFER_SIZE);
    @memset(connection_fsm.buffer, 0);
    connection_fsm.state = new_state.new_state;
    const new_command_event = Event{
        .type = .{ .recv = .{ .fd = connection_fsm.fd, .buffer = connection_fsm.buffer } },
        .user_data = finite_state_machine,
    };

    try event_queue.addAsyncEvent(new_command_event);
}

fn FSMdestroyer(finite_state_machine: *fsm.FSM) void {
    if (finite_state_machine.type != .connection or finite_state_machine.type.connection.peer_type != .slave)
        finite_state_machine.deinit();
}

fn setupTimer(timeout_ms: usize) !posix.fd_t {
    const timeout_fd = try posix.timerfd_create(posix.timerfd_clockid_t.REALTIME, linux.TFD{});

    const time_to_wait = @as(isize, @bitCast(timeout_ms)) * 1_000_000;
    const timeout = linux.itimerspec{
        .it_interval = linux.timespec{
            .sec = 0,
            .nsec = 0,
        },
        .it_value = linux.timespec{ .sec = @divFloor(time_to_wait, 1_000_000_000), .nsec = @rem(time_to_wait, 1_000_000_000) },
    };

    try posix.timerfd_settime(timeout_fd, linux.TFD.TIMER{}, &timeout, null);

    return timeout_fd;
}

// The caller owns the resulting slice that was allocated with the given allocator
// Slices of strings in the options point to argv
fn parseCommandLineArguments(allocator: std.mem.Allocator) ![]db.ConfigOption {
    var config = std.ArrayList(struct { []const u8, []const u8 }).init(allocator);
    errdefer config.deinit();
    const arg_type = enum { STRING, U16 };
    const available_options: struct { short: []const ?u8, long: []const ?[]const u8, description: []const []const u8, arg_type: []const ?arg_type } = .{
        .short = &[_]?u8{ 'h', null, null, 'p', null },
        .long = &[_]?[]const u8{ "help", "dir", "dbfilename", "port", "replicaof" },
        .description = &[_][]const u8{ "Display this help and exit", "Directory where dbfilename can be found", "The name of a .rdb file to load on startup", "The port to listen on", "The master instance for this replica (e.g. \"127.0.0.1 6379\")" },
        .arg_type = &[_]?arg_type{ null, .STRING, .STRING, .U16, .STRING },
    };

    var args_it = std.process.ArgIterator.init();
    const program_name = args_it.next().?;

    var temp_allocator = std.heap.ArenaAllocator.init(allocator);
    defer temp_allocator.deinit();

    var help_message = std.ArrayList(u8).init(temp_allocator.allocator());
    const help_message_writer = help_message.writer();

    try std.fmt.format(help_message_writer, "Usage: {s} [options..]\n", .{program_name});
    for (0..available_options.short.len) |i| {
        if (available_options.short[i] != null and available_options.long[i] != null) {
            try std.fmt.format(help_message_writer, "\t-{c}, --{s}", .{ available_options.short[i].?, available_options.long[i].? });
        } else if (available_options.short[i] != null) {
            try std.fmt.format(help_message_writer, "\t-{}", .{available_options.short[i].?});
        } else {
            try std.fmt.format(help_message_writer, "\t--{s}", .{available_options.long[i].?});
        }
        if (available_options.arg_type[i]) |arg| {
            switch (arg) {
                .U16 => try std.fmt.format(help_message_writer, " <0-65535>", .{}),
                .STRING => try std.fmt.format(help_message_writer, " <str>", .{}),
            }
        }
        if (i <= 1) try std.fmt.format(help_message_writer, "\t", .{}); // dirty trick to align descriptions, might use line length in the future to choose how many tabs to use
        try std.fmt.format(help_message_writer, "\t{s}\n", .{available_options.description[i]});
    }
    const stderr = std.io.getStdErr();

    var options = std.ArrayList(@typeInfo(@typeInfo(@typeInfo(@TypeOf(parseCommandLineArguments)).@"fn".return_type.?).error_union.payload).pointer.child).init(allocator);
    errdefer options.deinit();

    while (args_it.next()) |arg| {
        var n_dashes: u8 = 0;
        var optname = arg[0..];
        if (arg[n_dashes] != '-') {
            _ = try stderr.write(help_message.items);
            return error.PositionalArgument;
        }
        while (arg[n_dashes] == '-') : (n_dashes += 1) {}
        optname = optname[n_dashes..];
        var optidx: ?usize = null;
        switch (n_dashes) {
            1 => {
                inline for (available_options.short, 0..) |maybe_option, i| {
                    if (maybe_option) |option| {
                        if (optname[0] == option) {
                            optidx = i;
                            break;
                        }
                    }
                }
            },
            2 => {
                inline for (available_options.long, 0..) |maybe_option, i| {
                    if (maybe_option) |option| {
                        if (std.mem.eql(u8, option, optname)) {
                            optidx = i;
                            break;
                        }
                    }
                }
            },
            else => return error.InvalidOption,
        }
        if (optidx == null) {
            _ = try stderr.write(help_message.items);
            return error.InvalidOption;
        }

        switch (optidx.?) {
            0 => {
                _ = try stderr.write(help_message.items);
                return error.Help;
            }, // help
            1 => { // dir
                const next_arg = args_it.next() orelse {
                    _ = try stderr.write(help_message.items);
                    return error.MissingArgument;
                };
                if (next_arg[0] == '-') {
                    _ = try stderr.write(help_message.items);
                    return error.InvalidArgument;
                }
                try options.append(.{ .name = optname, .value = next_arg[0..] });
            },
            2 => {
                const next_arg = args_it.next() orelse {
                    _ = try stderr.write(help_message.items);
                    return error.MissingArgument;
                };
                if (next_arg[0] == '-') {
                    _ = try stderr.write(help_message.items);
                    return error.InvalidArgument;
                }
                try options.append(.{ .name = optname, .value = next_arg[0..] });
            }, // dbfilename
            3 => {
                const next_arg = args_it.next() orelse {
                    _ = try stderr.write(help_message.items);
                    return error.MissingArgument;
                };
                _ = std.fmt.parseInt(u16, next_arg[0..], 10) catch {
                    _ = try stderr.write(help_message.items);
                    return error.InvalidArgument;
                };
                try options.append(.{ .name = "listening-port", .value = next_arg[0..] });
            }, // port
            4 => { // replicaof
                const next_arg = args_it.next() orelse {
                    _ = try stderr.write(help_message.items);
                    return error.MissingArgument;
                };
                if (next_arg[0] == '-') {
                    _ = try stderr.write(help_message.items);
                    return error.InvalidArgument;
                }
                try options.append(.{ .name = "master", .value = next_arg[0..] });
            },
            else => unreachable,
        }
    }
    return try options.toOwnedSlice();
}

pub fn main() !void {

    // You can use print statements as follows for debugging, they'll be visible when running tests.

    // INFO: Allocator set up

    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    defer _ = gpa.deinit();
    var allocator = gpa.allocator();

    // INFO: Config parsing
    const stderr = std.io.getStdErr();
    const config = parseCommandLineArguments(allocator) catch |err| {
        switch (err) {
            error.Help => return,
            error.PositionalArgument => {
                _ = try stderr.write("ERROR: positional arguments are not supported\n");
                posix.exit(1);
            },
            error.InvalidOption => {
                _ = try stderr.write("ERROR: an invalid option was provided\n");
                posix.exit(1);
            },
            error.MissingArgument => {
                _ = try stderr.write("ERROR: a required option argument was not provided\n");
                posix.exit(1);
            },
            error.InvalidArgument => {
                _ = try stderr.write("ERROR: an invalid option argument was provided\n");
                posix.exit(1);
            },
            else => unreachable,
        }
    };

    try stdout.print("Logs from your program will appear here!\n", .{});
    std.debug.print("My PID is {}\n", .{linux.getpid()});

    const port = for (config) |opt| {
        if (std.mem.eql(u8, opt.name, "listening-port")) {
            break try std.fmt.parseInt(u16, opt.value, 10);
        }
    } else blk: {
        break :blk 6379;
    };

    const address = try net.Address.resolveIp("127.0.0.1", port);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    std.debug.print("Listening on port {}...\n", .{port});

    // INFO: signalfd set up to catch signals (SIGINT & SIGTERM)

    var sigset = posix.empty_sigset;
    linux.sigaddset(&sigset, posix.SIG.TERM);
    linux.sigaddset(&sigset, posix.SIG.INT);
    posix.sigprocmask(posix.SIG.BLOCK, &sigset, null);
    const sfd = try posix.signalfd(-1, &sigset, linux.SFD.NONBLOCK | linux.SFD.CLOEXEC);
    defer posix.close(sfd);

    // INFO: Server FSM setup

    var global_data = fsm.GlobalData.init(allocator);
    defer global_data.deinit();

    const server_fsm = try fsm.FSM.newServer(allocator, listener.stream.handle, &global_data);

    // INFO: Event queue init

    var event_queue = try EventQueue.init(allocator, IO_URING_ENTRIES);

    defer event_queue.destroy(.{ .user_data_destroyer = FSMdestroyer }) catch {
        @panic("Failed destroying the event queue");
    };

    const new_connection_event = Event{ .type = .{ .accept = server_fsm.type.server.socket }, .user_data = server_fsm };

    const termination_event = Event{ .type = .{ .pollin = sfd }, .user_data = server_fsm };

    try event_queue.addAsyncEvent(termination_event);
    try event_queue.addAsyncEvent(new_connection_event);

    // INFO: DB init
    var instance = try db.Instance.init(allocator, config);
    defer instance.destroy(allocator);
    allocator.free(config);

    var master_server_fsm: ?*fsm.FSM = null;

    if (instance.master) |master_fd| {
        master_server_fsm = try fsm.FSM.newConnection(allocator, master_fd, .master, &global_data);
        const command_received_event = Event{
            .type = .{ .recv = .{ .fd = master_fd, .buffer = master_server_fsm.?.type.connection.buffer } },
            .user_data = master_server_fsm.?,
        };
        try event_queue.addAsyncEvent(command_received_event);
    }

    event_loop: while (true) {
        try stdout.print("Waiting for something to come through...\n", .{});
        const completed_event = try event_queue.next();
        const event_fsm: *fsm.FSM = completed_event.awaited_event.user_data;
        try stdout.print("New event: {any}\n", .{completed_event});
        switch (completed_event.awaited_event.type) {
            .accept => {
                if (event_fsm.type != .server) {
                    @panic("Something is very wrong");
                }
                const connection_socket: posix.socket_t = completed_event.async_result;
                try stdout.print("accepted new connection\n", .{});
                // TODO: fetch user data from pending events to destroy FSMs in
                // the signal handling code
                const new_connection_fsm = try fsm.FSM.newConnection(allocator, connection_socket, .generic_client, &global_data);
                try waitForCommand(new_connection_fsm, &event_queue, .{});

                try event_queue.addAsyncEvent(new_connection_event);
            },
            .recv => {
                switch (event_fsm.type) {
                    .server => @panic("Not ready yet"),
                    .connection => |*connection_fsm| {
                        if (completed_event.async_result == 0) { // connection closed
                            if (connection_fsm.peer_type != .slave or event_fsm.global_data.slaves.remove(event_fsm)) {
                                event_fsm.deinit();
                            }
                            try event_queue.removePendingEvents(event_fsm);
                            continue :event_loop;
                        }

                        if (completed_event.async_result < 0) { // error
                            std.debug.print("Error: {} {d}\n", .{ linux.E.init(@intCast(@as(u32, @bitCast(completed_event.async_result)))), completed_event.async_result });
                            if (connection_fsm.peer_type != .slave or event_fsm.global_data.slaves.remove(event_fsm)) {
                                event_fsm.deinit();
                            }
                            try event_queue.removePendingEvents(event_fsm);
                            continue;
                        }

                        if (connection_fsm.peer_type == .slave and connection_fsm.state == .waiting_for_ack) {
                            var temp_allocator = std.heap.ArenaAllocator.init(allocator);
                            defer temp_allocator.deinit();

                            const response, _ = resp.Value.parse(connection_fsm.buffer, temp_allocator.allocator()) catch {
                                std.debug.print("{?x}\n", .{connection_fsm.buffer});
                                @panic("TODO");
                            };

                            if (response != .array)
                                @panic("TODO");

                            const new_offset = try std.fmt.parseInt(usize, response.array[2].bulk_string, 10);

                            const old_offset = connection_fsm.peer_type.slave.repl_offset;
                            if (old_offset < new_offset) {
                                connection_fsm.peer_type.slave.repl_offset = new_offset;
                            }

                            var new_pending_waits = std.PriorityQueue(*fsm.PendingWait, void, fsm.PendingWait.order).init(allocator, undefined);
                            while (event_fsm.global_data.pending_waits.removeOrNull()) |pending_wait| {
                                if (pending_wait.threshold_offset > old_offset and pending_wait.threshold_offset <= new_offset) {
                                    pending_wait.actual_n_replicas += 1;
                                    if (pending_wait.actual_n_replicas >= pending_wait.expected_n_replicas) {
                                        const reply = resp.Integer(@bitCast(pending_wait.actual_n_replicas));
                                        try respondWith(try reply.encode(temp_allocator.allocator()), pending_wait.client_connection_fsm, &event_queue, .{});
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
                        if (connection_fsm.state != .waiting_for_commands and connection_fsm.state != .executing_transaction)
                            @panic("Bad");

                        var n: usize = 0;
                        const buffer_size = @as(usize, @intCast(completed_event.async_result));
                        var n_new_commands: usize = 0;
                        var notify_fsm_event = Event{
                            .type = .{ .notify = .{ .fd = connection_fsm.new_commands_notification_fd, .n = 0 } },
                            .user_data = event_fsm,
                        };

                        var temp_allocator = std.heap.ArenaAllocator.init(allocator);
                        defer temp_allocator.deinit();

                        while (buffer_size - n > 0) {
                            const command, const parsed_bytes = Command.parse(connection_fsm.buffer[n..], allocator) catch |err| {
                                std.debug.print("Error while parsing command: {}\n", .{err});
                                std.debug.print("Bytes on the wire: {x} (return value: {d})\n", .{ connection_fsm.buffer, completed_event.async_result });

                                if (n_new_commands > 0) {
                                    try event_queue.addAsyncEvent(notify_fsm_event);
                                }
                                try respondWith(try resp.SimpleError(try cmd.errorToString(err)).encode(temp_allocator.allocator()), event_fsm, &event_queue, .{});
                                continue :event_loop;
                            };

                            if (command.type == .exec) {
                                if (connection_fsm.state != .executing_transaction) {
                                    try respondWith(try resp.SimpleError("EXEC without MULTI").encode(temp_allocator.allocator()), event_fsm, &event_queue, .{});

                                    continue :event_loop;
                                }

                                notify_fsm_event.type.notify.n = 1;
                                try event_queue.addAsyncEvent(notify_fsm_event);
                                continue :event_loop;
                            }

                            if (command.type == .discard) {
                                if (connection_fsm.state != .executing_transaction) {
                                    try respondWith(try resp.SimpleError("DISCARD without MULTI").encode(temp_allocator.allocator()), event_fsm, &event_queue, .{});
                                    continue :event_loop;
                                }
                                connection_fsm.flushCommandsQueue();
                                try respondWith(try resp.Ok.encode(temp_allocator.allocator()), event_fsm, &event_queue, .{});
                                continue :event_loop;
                            }

                            try connection_fsm.addCommand(command);
                            n_new_commands += 1;
                            notify_fsm_event.type.notify.n = n_new_commands;
                            n += parsed_bytes;
                        }

                        std.debug.assert(n_new_commands > 0);
                        if (connection_fsm.state != .executing_transaction) {
                            connection_fsm.state = .executing_commands;
                            try event_queue.addAsyncEvent(notify_fsm_event);
                        } else {
                            try respondWith(try resp.SimpleString("QUEUED").encode(temp_allocator.allocator()), event_fsm, &event_queue, .{ .new_state = .executing_transaction });
                        }
                    },
                }
            },
            .read => {
                if (event_fsm.type != .connection) {
                    @panic("Something is very wrong");
                }

                const connection_fsm = &event_fsm.type.connection;

                if (connection_fsm.state != .executing_commands and connection_fsm.state != .executing_transaction) {
                    std.debug.print("Maybe I'm replying, come back later\n", .{});
                    const notify_fsm_event = Event{
                        .type = .{ .notify = .{ .fd = connection_fsm.new_commands_notification_fd, .n = 1 } },
                        .user_data = event_fsm,
                    };
                    try event_queue.addAsyncEvent(notify_fsm_event);
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
                                        try respondWith(try resp.Integer(event_fsm.global_data.slaves.count()).encode(temp_allocator.allocator()), event_fsm, &event_queue, .{});
                                    } else {
                                        var count: usize = 0;
                                        const num_replicas_threshold = command.type.wait.num_replicas;
                                        const timeout_timestamp = std.time.milliTimestamp() + @as(i64, @bitCast(command.type.wait.timeout_ms));

                                        const timeout_fd = try setupTimer(command.type.wait.timeout_ms);

                                        var slaves_it = event_fsm.global_data.slaves.iterator();
                                        while (slaves_it.next()) |slave| {
                                            const slave_repl_offset = slave.key_ptr.*.type.connection.peer_type.slave.repl_offset;
                                            if (slave_repl_offset >= instance.repl_offset) {
                                                count += 1;
                                            }
                                        }

                                        if (count >= num_replicas_threshold) {
                                            try respondWith(try resp.Integer(@bitCast(count)).encode(temp_allocator.allocator()), event_fsm, &event_queue, .{});
                                            continue :event_loop;
                                        }

                                        // We didn't exit early, we must ask the slaves for their offset and block
                                        slaves_it = event_fsm.global_data.slaves.iterator();
                                        while (slaves_it.next()) |slave| {
                                            const ack_request = resp.Array(&[_]resp.Value{ resp.BulkString("REPLCONF"), resp.BulkString("GETACK"), resp.BulkString("*") });
                                            const slave_fsm = &slave.key_ptr.*.type.connection;
                                            resizeBuffer(&slave_fsm.buffer, fsm.CLIENT_BUFFER_SIZE);
                                            slave_fsm.buffer = try std.fmt.bufPrint(slave_fsm.buffer, "{s}", .{try ack_request.encode(temp_allocator.allocator())});
                                            const getack_event = Event{ .type = .{ .send = .{ .buffer = slave_fsm.buffer, .fd = slave_fsm.fd } }, .user_data = slave.key_ptr.* };
                                            slave_fsm.state = .sending_getack;
                                            try event_queue.addAsyncEvent(getack_event);
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

                                        const timeout_event = Event{ .type = .{ .pollin = timeout_fd }, .user_data = event_fsm };

                                        try event_queue.addAsyncEvent(timeout_event);
                                    }
                                },
                                .psync => {
                                    connection_fsm.peer_type = .{ .slave = .{ .repl_offset = 0 } };
                                    try event_fsm.global_data.slaves.put(event_fsm, undefined);
                                    var dump = [_]u8{0} ** fsm.CLIENT_BUFFER_SIZE;
                                    const dump_size = try instance.dumpToBuffer(&dump);
                                    // Why 0 for the repl_offset? Codecrafters really
                                    try respondWith(try std.fmt.allocPrint(temp_allocator.allocator(), "+FULLRESYNC {[replid]s} {[repl_offset]d}\r\n${[dump_size]d}\r\n{[dump]s}", .{ .replid = instance.replid, .repl_offset = 0, .dump_size = dump_size, .dump = dump[0..dump_size] }), event_fsm, &event_queue, .{ .new_state = .sending_dump });
                                },
                                .multi => {
                                    try respondWith(try resp.Ok.encode(temp_allocator.allocator()), event_fsm, &event_queue, .{ .new_state = .executing_transaction });
                                    continue :event_loop;
                                },
                                else => {
                                    if (connection_fsm.peer_type == .master or (command.shouldPropagate() and instance.master == null))
                                        instance.repl_offset += command.bytes.len;
                                    const reply = instance.executeCommand(temp_allocator.allocator(), command) catch {
                                        try respondWith(try resp.SimpleError("Some error occurred during command execution").encode(temp_allocator.allocator()), event_fsm, &event_queue, .{});
                                        continue :event_loop;
                                    };

                                    if (command.type == .xread and command.type.xread.block_timeout_ms != null and std.meta.eql(reply, resp.Null)) {
                                        var timerfd: ?posix.fd_t = null;
                                        if (command.type.xread.block_timeout_ms) |timeout_ms| {
                                            timerfd = try setupTimer(timeout_ms);
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
                                            const timeout_event = Event{ .type = .{ .pollin = fd }, .user_data = event_fsm };

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

                                                try respondWith(try resp.Array(&[_]resp.Value{resp.Array(try response.toOwnedSlice())}).encode(temp_allocator.allocator()), blocked_stream_read.client_connection_fsm, &event_queue, .{});
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
                                            const slave_connection = &slave_fsm.*.type.connection;
                                            slave_connection.state = .propagating_command;
                                            resizeBuffer(&slave_connection.buffer, fsm.CLIENT_BUFFER_SIZE);
                                            slave_connection.buffer = try std.fmt.bufPrint(slave_connection.buffer, "{s}", .{command.bytes});
                                            const propagation_event = Event{
                                                .type = .{ .send = .{ .fd = slave_connection.fd, .buffer = slave_connection.buffer } },
                                                .user_data = slave_fsm.*,
                                            };
                                            try event_queue.addAsyncEvent(propagation_event);
                                        }
                                    }

                                    if (connection_fsm.peer_type == .master and (command.type != .replconf or command.type.replconf != .getack)) {
                                        if (connection_fsm.commands_to_execute.len == 0) {
                                            try waitForCommand(event_fsm, &event_queue, .{});
                                        } else {
                                            const wakeup_event = Event{
                                                .type = .{ .read = .{ .fd = connection_fsm.new_commands_notification_fd, .buffer = std.mem.asBytes(&connection_fsm.notification_val) } },
                                                .user_data = event_fsm,
                                            };
                                            try event_queue.addAsyncEvent(wakeup_event);
                                        }
                                        continue :event_loop;
                                    }

                                    try respondWith(try reply.encode(temp_allocator.allocator()), event_fsm, &event_queue, .{});
                                },
                            }
                        } else try waitForCommand(event_fsm, &event_queue, .{});
                    },
                    .executing_transaction => {
                        var replies = std.ArrayList(resp.Value).init(temp_allocator.allocator());
                        while (connection_fsm.popCommand()) |*command| {
                            defer @constCast(command).deinit();
                            const reply = instance.executeCommand(temp_allocator.allocator(), command) catch {
                                try respondWith(try resp.SimpleError("Some error occurred during transaction execution").encode(temp_allocator.allocator()), event_fsm, &event_queue, .{});
                                continue :event_loop;
                            };
                            try replies.append(reply);
                        }
                        try respondWith(try resp.Array(try replies.toOwnedSlice()).encode(temp_allocator.allocator()), event_fsm, &event_queue, .{});
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
                                resizeBuffer(&connection_fsm.buffer, fsm.CLIENT_BUFFER_SIZE);
                                @memset(connection_fsm.buffer, 0);
                                const new_command_event = Event{
                                    .type = .{ .recv = .{ .fd = connection_fsm.fd, .buffer = connection_fsm.buffer } },
                                    .user_data = event_fsm,
                                };

                                try event_queue.addAsyncEvent(new_command_event);
                            },
                            .sending_response => {
                                if (connection_fsm.commands_to_execute.first != null) {
                                    // There are commands to execute, go work on those
                                    const notify_fsm_event = Event{
                                        .type = .{ .notify = .{ .fd = connection_fsm.new_commands_notification_fd } },
                                        .user_data = event_fsm,
                                    };
                                    connection_fsm.state = .executing_commands;
                                    try event_queue.addAsyncEvent(notify_fsm_event);
                                } else try waitForCommand(event_fsm, &event_queue, .{});
                            },
                            .sending_dump, .propagating_command => {
                                connection_fsm.state = .in_sync;
                            },
                            .executing_transaction => {
                                try waitForCommand(event_fsm, &event_queue, .{ .new_state = .executing_transaction });
                            },
                            else => @panic("Invalid state transition"),
                        }
                    },
                }
            },
            .notify => {
                if (event_fsm.type != .connection) {
                    @panic("Something is very wrong");
                }

                const connection_fsm = &event_fsm.type.connection;
                const wakeup_event = Event{
                    .type = .{ .read = .{ .fd = connection_fsm.new_commands_notification_fd, .buffer = std.mem.asBytes(&connection_fsm.notification_val) } },
                    .user_data = event_fsm,
                };

                if (connection_fsm.state != .executing_transaction)
                    connection_fsm.state = .executing_commands;
                try event_queue.addAsyncEvent(wakeup_event);
            },
            .pollin => |timerfd| {
                switch (event_fsm.type) {
                    .server => {
                        std.debug.print("Gracefully shutting down...\n", .{});
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
                            try respondWith(try reply.encode(temp_allocator.allocator()), event_fsm, &event_queue, .{});
                            allocator.destroy(pending_wait);
                            posix.close(completed_event.awaited_event.type.pollin); // the timerfd
                            continue :event_loop;
                        }

                        if (indicesToRemove.items.len > 0)
                            continue :event_loop; // it was a pending wait, go ahead

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
                            try respondWith(try resp.Null.encode(temp_allocator.allocator()), blocked_xread.client_connection_fsm, &event_queue, .{});
                        } else {
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
}
