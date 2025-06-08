const std = @import("std");
const eq = @import("event_queue.zig");
const resp = @import("resp.zig");
const cmd = @import("command.zig");
const fsm = @import("fsm.zig");
const Command = cmd.Command;
const db = @import("db.zig");
const clap = @import("clap");
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

fn waitForCommand(finite_state_machine: *fsm.FSM, event_queue: *EventQueue) !void {
    if (finite_state_machine.type != .connection)
        return error.InvalidFSM;
    const connection_fsm = &finite_state_machine.type.connection;
    resizeBuffer(&connection_fsm.buffer, fsm.CLIENT_BUFFER_SIZE);
    @memset(connection_fsm.buffer, 0);
    connection_fsm.state = .waiting_for_commands;
    const new_command_event = Event{
        .type = .{ .recv = .{ .fd = connection_fsm.fd, .buffer = connection_fsm.buffer } },
        .user_data = finite_state_machine,
    };

    try event_queue.addAsyncEvent(new_command_event);
}

pub fn main() !void {

    // You can use print statements as follows for debugging, they'll be visible when running tests.

    try stdout.print("Logs from your program will appear here!\n", .{});
    std.debug.print("My PID is {}\n", .{linux.getpid()});

    // INFO: Allocator set up

    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    defer _ = gpa.deinit();
    var allocator = gpa.allocator();

    // INFO: Config parsing

    var config = std.ArrayList(struct { []const u8, []const u8 }).init(allocator);

    // TODO: I don't need a full data model for my options, a list of string pairs
    //  will suffice. I can take the parser out of clap and just use that part
    // First we specify what parameters our program can take.
    // We can use `parseParamsComptime` to parse a string into an array of `Param(Help)`.
    const params = comptime clap.parseParamsComptime(
        \\-h, --help             Display this help and exit.
        \\--dir <str>   Directory where dbfilename can be found
        \\--dbfilename <str>  The name of a .rdb file to load on startup
        \\-p, --port <u16>  The port to listen on
        \\--replicaof <str>  The master instance for this replica
        \\
    );

    // Initialize our diagnostics, which can be used for reporting useful errors.
    // This is optional. You can also pass `.{}` to `clap.parse` if you don't
    // care about the extra information `Diagnostics` provides.
    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, clap.parsers.default, .{
        .diagnostic = &diag,
        .allocator = gpa.allocator(),
    }) catch |err| {
        // Report useful error and exit.
        diag.report(std.io.getStdErr().writer(), err) catch {};
        return err;
    };
    defer res.deinit();

    if (res.args.dir) |dir| {
        try config.append(.{ "dir", dir });
    }

    if (res.args.dbfilename) |dbfilename| {
        try config.append(.{ "dbfilename", dbfilename });
    }

    if (res.args.replicaof) |replicaof| {
        try config.append(.{ "master", replicaof });
    }

    // INFO: address binding and socket listening

    var listening_port = [_]u8{0} ** 5;

    try config.append(.{ "listening-port", try std.fmt.bufPrint(&listening_port, "{d}", .{res.args.port orelse 6379}) });

    const port = res.args.port orelse 6379;

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

    const server_fsm = try allocator.create(fsm.FSM);
    server_fsm.* = fsm.FSM{ .global_data = &global_data, .type = .{ .server = fsm.Server{
        .socket = listener.stream.handle,
        .state = .waiting,
    } } };

    // INFO: Event queue init

    var event_queue = try EventQueue.init(allocator, IO_URING_ENTRIES);

    defer event_queue.destroy(.{ .user_data_allocator = allocator }) catch {
        @panic("Failed destroying the event queue");
    };

    const new_connection_event = Event{ .type = .{ .accept = server_fsm.type.server.socket }, .user_data = server_fsm };

    const termination_event = Event{ .type = .{ .pollin = sfd }, .user_data = server_fsm };

    try event_queue.addAsyncEvent(termination_event);
    try event_queue.addAsyncEvent(new_connection_event);

    // INFO: DB init
    const db_config = try config.toOwnedSlice();
    var instance = try db.Instance.init(allocator, db_config);
    defer instance.destroy(allocator);
    allocator.free(db_config);

    var master_server_fsm: ?*fsm.FSM = null;

    if (instance.master) |master_fd| {
        const new_buffer = try allocator.alloc(u8, fsm.CLIENT_BUFFER_SIZE);
        master_server_fsm = try allocator.create(fsm.FSM);
        master_server_fsm.?.* = fsm.FSM{ .global_data = &global_data, .type = .{ .connection = .{ .fd = master_fd, .buffer = new_buffer, .peer_type = .MASTER, .new_commands_notification_fd = try posix.eventfd(0, linux.EFD.SEMAPHORE), .notification_val = 0, .state = .waiting_for_commands, .allocator = allocator, .commands_to_execute = std.DoublyLinkedList(Command){} } } };
        const command_received_event = Event{
            .type = .{ .recv = .{ .fd = master_fd, .buffer = new_buffer } },
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
                var new_connection_fsm = try allocator.create(fsm.FSM);
                new_connection_fsm.type = .{ .connection = try fsm.Connection.init(allocator, connection_socket) };
                new_connection_fsm.global_data = &global_data;

                const new_command_event = Event{
                    .type = .{ .recv = .{ .fd = new_connection_fsm.type.connection.fd, .buffer = new_connection_fsm.type.connection.buffer } },
                    .user_data = new_connection_fsm,
                };

                try event_queue.addAsyncEvent(new_command_event);
                try event_queue.addAsyncEvent(new_connection_event);
            },
            .recv => {
                switch (event_fsm.type) {
                    .server => @panic("Not ready yet"),
                    .connection => |*connection_fsm| {
                        if (connection_fsm.state != .waiting_for_commands)
                            @panic("Bad");
                        if (completed_event.async_result == 0) { // connection closed
                            if (connection_fsm.peer_type == .SLAVE) {
                                _ = event_fsm.global_data.slaves.remove(event_fsm);
                            }
                            connection_fsm.deinit();
                            if (master_server_fsm == null or (master_server_fsm != null and event_fsm != master_server_fsm.?))
                                allocator.destroy(event_fsm);
                            continue :event_loop;
                        }

                        if (completed_event.async_result < 0) { // error
                            std.debug.print("Error: {} {d}\n", .{ linux.E.init(@intCast(@as(u32, @bitCast(completed_event.async_result)))), completed_event.async_result });
                            _ = event_fsm.global_data.slaves.remove(event_fsm);
                            connection_fsm.deinit();
                            if (master_server_fsm != null and event_fsm != master_server_fsm.?)
                                allocator.destroy(event_fsm);
                            continue;
                        }

                        var n: usize = 0;
                        const buffer_size = @as(usize, @intCast(completed_event.async_result));
                        var notify_fsm_about_new_commands = false;
                        const notify_fsm_event = Event{
                            .type = .{ .notify = connection_fsm.new_commands_notification_fd },
                            .user_data = event_fsm,
                        };

                        while (buffer_size - n > 0) {
                            const command, const parsed_bytes = Command.parse(connection_fsm.buffer[n..], allocator) catch |err| {
                                std.debug.print("Error while parsing command: {}\n", .{err});
                                std.debug.print("Bytes on the wire: {x} (return value: {d})\n", .{ connection_fsm.buffer, completed_event.async_result });

                                var temp_allocator = std.heap.ArenaAllocator.init(allocator);
                                defer temp_allocator.deinit();

                                if (notify_fsm_about_new_commands) {
                                    try event_queue.addAsyncEvent(notify_fsm_event);
                                }
                                try respondWith(try resp.SimpleError(try cmd.errorToString(err)).encode(temp_allocator.allocator()), event_fsm, &event_queue, .{});
                                continue :event_loop;
                            };

                            var new_command = try allocator.create(std.DoublyLinkedList(Command).Node);
                            new_command.data = command;
                            connection_fsm.commands_to_execute.append(new_command);
                            instance.repl_offset += parsed_bytes;
                            notify_fsm_about_new_commands = true;
                            n += parsed_bytes;
                        }

                        if (notify_fsm_about_new_commands) {
                            connection_fsm.state = .executing_commands;
                            try event_queue.addAsyncEvent(notify_fsm_event);
                        }
                    },
                }
            },
            .send => {
                switch (event_fsm.type) {
                    .server => @panic("Not ready yet"),
                    .connection => |*connection_fsm| {
                        switch (connection_fsm.state) {
                            .sending_response => {
                                if (connection_fsm.commands_to_execute.first != null) {
                                    // There are commands to execute, go work on those
                                    const notify_fsm_event = Event{
                                        .type = .{ .notify = connection_fsm.new_commands_notification_fd },
                                        .user_data = event_fsm,
                                    };
                                    connection_fsm.state = .executing_commands;
                                    try event_queue.addAsyncEvent(notify_fsm_event);
                                } else try waitForCommand(event_fsm, &event_queue);
                            },
                            .sending_dump, .propagating_command => {
                                connection_fsm.state = .in_sync;
                            },
                            else => @panic("Invalid state transition"),
                        }
                    },
                }
            },
            .pollin => {
                if (event_fsm.type != .server)
                    @panic("Wtf");
                std.debug.print("Gracefully shutting down...\n", .{});
                break;
            },
            .read => {
                if (event_fsm.type != .connection) {
                    @panic("Something is very wrong");
                }

                const connection_fsm = &event_fsm.type.connection;

                if (connection_fsm.state != .executing_commands) {
                    std.debug.print("Maybe I'm replying, come back later\n", .{});
                    try event_queue.addAsyncEvent(completed_event.awaited_event);
                    continue :event_loop;
                }

                var temp_allocator = std.heap.ArenaAllocator.init(allocator);
                defer temp_allocator.deinit();
                if (connection_fsm.commands_to_execute.pop()) |new_command_node| {
                    // TODO: destroy arrays allocated by the command, if any
                    var command = new_command_node.data;
                    defer command.destroy(allocator);
                    allocator.destroy(new_command_node);
                    switch (command) {
                        .psync => {
                            connection_fsm.peer_type = .SLAVE;
                            try event_fsm.global_data.slaves.put(event_fsm, undefined);
                            var dump = [_]u8{0} ** fsm.CLIENT_BUFFER_SIZE;
                            const dump_size = try instance.dumpToBuffer(&dump);
                            // Why 0 for the repl_offset? Codecrafters really
                            try respondWith(try std.fmt.allocPrint(temp_allocator.allocator(), "+FULLRESYNC {[replid]s} {[repl_offset]d}\r\n${[dump_size]d}\r\n{[dump]s}", .{ .replid = instance.replid, .repl_offset = 0, .dump_size = dump_size, .dump = dump[0..dump_size] }), event_fsm, &event_queue, .{ .new_state = .sending_dump });
                        },
                        else => {
                            const reply = instance.executeCommand(temp_allocator.allocator(), &command) catch {
                                try respondWith(try resp.SimpleError("Some error occurred during command execution").encode(temp_allocator.allocator()), event_fsm, &event_queue, .{});
                                continue :event_loop;
                            };

                            if (command.shouldPropagate() and instance.master == null) {
                                var slaves_it = event_fsm.global_data.slaves.keyIterator();
                                while (slaves_it.next()) |slave_fsm| {
                                    const slave_connection = &slave_fsm.*.type.connection;
                                    slave_connection.state = .propagating_command;
                                    slave_connection.buffer = try std.fmt.bufPrint(slave_connection.buffer, "{s}", .{try command.encode(temp_allocator.allocator())});
                                    const propagation_event = Event{
                                        .type = .{ .send = .{ .fd = slave_connection.fd, .buffer = slave_connection.buffer } },
                                        .user_data = slave_fsm.*,
                                    };
                                    try event_queue.addAsyncEvent(propagation_event);
                                }
                            }

                            if (connection_fsm.peer_type == .MASTER and (command != .replconf or command.replconf != .getack)) {
                                try waitForCommand(event_fsm, &event_queue);
                                continue :event_loop;
                            }

                            try respondWith(try reply.encode(temp_allocator.allocator()), event_fsm, &event_queue, .{});
                        },
                    }
                } else try waitForCommand(event_fsm, &event_queue);
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

                connection_fsm.state = .executing_commands;
                try event_queue.addAsyncEvent(wakeup_event);
            },
            // .send: EventWithBuffer,
            // .recv: EventWithBuffer,
            // .read: EventWithBuffer,
            // .CONNECTION => {
            //     const connection_socket = event.async_result.?;
            //     try stdout.print("accepted new connection\n", .{});
            //     try addReceiveCommandEvent(connection_socket, try allocator.alloc(u8, CLIENT_BUFFER_SIZE), &event_queue, null, allocator);
            //     try event_queue.addAsyncEvent(&connection_event, false);
            // },
            // .RECEIVE_COMMAND => {
            //     const buffer = event.buffer.?;
            //     const return_value = event.async_result.?;
            //
            //     if (return_value == 0) { // Connection closed
            //         _ = slaves.remove(event.fd); // Might be a slave, doesn't hurt if not
            //         // TODO: Make optional so the codecrafters test can pass
            //         // if (instance.master) |master_fd| {
            //         //     if (event.fd == master_fd) {
            //         //         std.debug.print("Master died, shutting down...\n", .{});
            //         //         destroyEvent(event, allocator);
            //         //         break;
            //         //     }
            //         // }
            //         destroyEvent(event, allocator);
            //         continue;
            //     }
            //
            //     if (return_value < 0) {
            //         std.debug.print("Error: {} {d}\n", .{ linux.E.init(@intCast(@as(u32, @bitCast(return_value)))), return_value });
            //         _ = slaves.remove(event.fd); // Might be a slave, doesn't hurt if not
            //         destroyEvent(event, allocator);
            //         continue;
            //     }
            //
            //     const recv_return_value = @as(usize, @intCast(@as(u32, @bitCast(return_value))));
            //
            //     var temp_allocator = std.heap.ArenaAllocator.init(allocator);
            //     defer temp_allocator.deinit();
            //
            //     var replies = std.ArrayList(u8).init(temp_allocator.allocator());
            //
            //     var n: usize = 0;
            //     var requeue = true;
            //
            //     while (recv_return_value - n != 0) {
            //         const command, const bytes_parsed = Command.parse(buffer[n..], temp_allocator.allocator()) catch |err| {
            //             std.debug.print("Error while parsing command: {}\n", .{err});
            //             std.debug.print("Bytes on the wire: {x} (return value: {d})\n", .{ buffer, recv_return_value });
            //             resizeBuffer(&event.buffer.?, CLIENT_BUFFER_SIZE);
            //             event.buffer = try std.fmt.bufPrint(event.buffer.?, "{s}", .{try resp.SimpleError(try cmd.errorToString(err)).encode(temp_allocator.allocator())});
            //             event.ty = eq.EVENT_TYPE.SENT_RESPONSE;
            //             try event_queue.addAsyncEvent(event, true);
            //             _ = slaves.remove(event.fd); // Might be a slave, doesn't hurt if not
            //             continue :event_loop;
            //         };
            //
            //         // TODO: Why the weird logic? To make Codecrafters tests pass really
            //         if (command == .wait and command.wait.num_replicas != 0 and instance.repl_offset > 0) {
            //             requeue = false;
            //             var count: usize = 0;
            //             const num_replicas_threshold = command.wait.num_replicas;
            //             const timeout_ms = command.wait.timeout_ms;
            //
            //             const timeout_timestamp = std.time.milliTimestamp() + @as(i64, @bitCast(timeout_ms));
            //
            //             const timeout_fd = try posix.timerfd_create(posix.timerfd_clockid_t.REALTIME, linux.TFD{});
            //
            //             const time_to_wait = @as(isize, @bitCast(timeout_ms)) * 1_000_000;
            //             const timeout = linux.itimerspec{
            //                 .it_interval = linux.timespec{
            //                     .sec = 0,
            //                     .nsec = 0,
            //                 },
            //                 .it_value = linux.timespec{ .sec = @divFloor(time_to_wait, 1_000_000_000), .nsec = @rem(time_to_wait, 1_000_000_000) },
            //             };
            //
            //             try posix.timerfd_settime(timeout_fd, linux.TFD.TIMER{}, &timeout, null);
            //
            //             var slaves_it = slaves.iterator();
            //             var block = true;
            //             while (slaves_it.next()) |slave| {
            //                 if (slave.value_ptr.* >= instance.repl_offset) {
            //                     count += 1;
            //                     if (count >= num_replicas_threshold) {
            //                         block = false;
            //                         try replies.appendSlice(try resp.Integer(@bitCast(count)).encode(temp_allocator.allocator()));
            //                         break;
            //                     }
            //                 }
            //             }
            //             slaves_it = slaves.iterator();
            //             if (block) {
            //                 while (slaves_it.next()) |slave| {
            //                     var send_get_ack_request_event = try allocator.create(eq.Event);
            //                     send_get_ack_request_event.ty = eq.EVENT_TYPE.SEND_GETACK;
            //                     send_get_ack_request_event.fd = slave.key_ptr.*;
            //                     send_get_ack_request_event.buffer = try allocator.alloc(u8, GETACK_BUFFER_SIZE);
            //                     const ack_request = resp.Array(&[_]resp.Value{ resp.BulkString("REPLCONF"), resp.BulkString("GETACK"), resp.BulkString("*") });
            //                     send_get_ack_request_event.buffer = try std.fmt.bufPrint(send_get_ack_request_event.buffer.?, "{s}", .{try ack_request.encode(temp_allocator.allocator())});
            //                     send_get_ack_request_event.canary = null;
            //                     try event_queue.addAsyncEvent(send_get_ack_request_event, true);
            //                 }
            //
            //                 const pending_wait = try allocator.create(PendingWait);
            //                 pending_wait.timeout = timeout_timestamp;
            //                 pending_wait.client_event = event;
            //                 pending_wait.threshold_offset = instance.repl_offset;
            //                 pending_wait.expected_n_replicas = num_replicas_threshold;
            //                 pending_wait.actual_n_replicas = count;
            //
            //                 try pending_waits.add(pending_wait);
            //
            //                 var timeout_event = try allocator.create(eq.Event);
            //                 timeout_event.ty = eq.EVENT_TYPE.TIMEOUT;
            //                 timeout_event.fd = timeout_fd;
            //                 // Using the client buffer instead of allocating a
            //                 // new one. Should not be destroyed in case of
            //                 // timeout
            //                 timeout_event.buffer = event.buffer;
            //                 timeout_event.canary = null;
            //                 timeout_event.pending_wait = pending_wait;
            //                 try event_queue.addAsyncEvent(timeout_event, true);
            //             }
            //         } else {
            //             if (instance.master != null and event.canary != null and event.canary.? == MASTER_CANARY) {
            //                 instance.repl_offset += bytes_parsed; // propagated command from master
            //             }
            //             if (command == .psync) {
            //                 const reply = try instance.executeCommand(temp_allocator.allocator(), &command);
            //                 event.ty = .FULL_SYNC;
            //                 resizeBuffer(&event.buffer.?, CLIENT_BUFFER_SIZE);
            //                 event.buffer = try std.fmt.bufPrint(event.buffer.?, "{s}", .{try reply.encode(temp_allocator.allocator())});
            //
            //                 try event_queue.addAsyncEvent(event, true);
            //                 continue :event_loop;
            //             }
            //             const reply = instance.executeCommand(temp_allocator.allocator(), &command) catch {
            //                 // TODO: better error handling
            //                 event.ty = eq.EVENT_TYPE.SENT_RESPONSE;
            //                 resizeBuffer(&event.buffer.?, CLIENT_BUFFER_SIZE);
            //                 event.buffer = try std.fmt.bufPrint(event.buffer.?, "{s}", .{try resp.SimpleError("Some error occurred during command execution").encode(temp_allocator.allocator())});
            //                 try event_queue.addAsyncEvent(event, true);
            //                 continue :event_loop;
            //             };
            //
            //             if (instance.master == null and command.shouldPropagate())
            //                 instance.repl_offset += bytes_parsed; // Updating the master's counter
            //
            //             if (instance.master != null and event.canary != null and event.canary.? == MASTER_CANARY) {
            //                 if (command == .replconf and command.replconf == .getack) {
            //                     try replies.appendSlice(try reply.encode(temp_allocator.allocator()));
            //                 }
            //             } else {
            //                 try replies.appendSlice(try reply.encode(temp_allocator.allocator()));
            //             }
            //
            //             if (command.shouldPropagate()) {
            //                 var slaves_it = slaves.keyIterator();
            //                 while (slaves_it.next()) |slave| {
            //                     var propagation_event = try allocator.create(eq.Event);
            //                     propagation_event.ty = eq.EVENT_TYPE.PROPAGATE_COMMAND;
            //                     propagation_event.fd = slave.*;
            //                     propagation_event.buffer = try allocator.alloc(u8, n + bytes_parsed);
            //                     propagation_event.canary = null;
            //                     @memcpy(propagation_event.buffer.?, buffer[n..][0..bytes_parsed]);
            //                     try event_queue.addAsyncEvent(propagation_event, true);
            //                 }
            //             }
            //         }
            //
            //         n += bytes_parsed;
            //     }
            //
            //     const reply = try replies.toOwnedSlice();
            //
            //     if (reply.len > 0) {
            //         event.ty = eq.EVENT_TYPE.SENT_RESPONSE;
            //         resizeBuffer(&event.buffer.?, reply.len);
            //         @memcpy(event.buffer.?, reply);
            //         try event_queue.addAsyncEvent(event, true);
            //     } else if (requeue) {
            //         resizeBuffer(&event.buffer.?, CLIENT_BUFFER_SIZE);
            //         event.ty = eq.EVENT_TYPE.RECEIVE_COMMAND;
            //         @memset(event.buffer.?, 0);
            //         try event_queue.addAsyncEvent(event, true);
            //     }
            // },
            // .FULL_SYNC => {
            //     resizeBuffer(&event.buffer.?, CLIENT_BUFFER_SIZE);
            //     const dump_size = try instance.dumpToBuffer(event.buffer.?);
            //     const n_digits = if (dump_size == 0) 1 else std.math.log10_int(dump_size) + 1;
            //     std.mem.copyBackwards(u8, event.buffer.?[1 + n_digits + 2 ..], event.buffer.?[0..dump_size]);
            //     const preamble = try std.fmt.bufPrint(event.buffer.?, "${d}\r\n", .{dump_size});
            //     resizeBuffer(&event.buffer.?, preamble.len + dump_size);
            //     event.ty = eq.EVENT_TYPE.SENT_DUMP;
            //     try event_queue.addAsyncEvent(event, true);
            // },
            // .SENT_RESPONSE, .SENT_DUMP => {
            //     resizeBuffer(&event.buffer.?, CLIENT_BUFFER_SIZE);
            //     if (event.ty == .SENT_DUMP) {
            //         try slaves.put(event.fd, 0);
            //         instance.n_slaves += 1;
            //         if (event.buffer) |buffer| {
            //             allocator.free(buffer);
            //         }
            //         allocator.destroy(event);
            //         continue;
            //     }
            //     event.ty = eq.EVENT_TYPE.RECEIVE_COMMAND;
            //     @memset(event.buffer.?, 0);
            //     try event_queue.addAsyncEvent(event, true);
            // },
            // .PROPAGATE_COMMAND => {
            //     allocator.free(event.buffer.?);
            //     allocator.destroy(event);
            // },
            // .SEND_GETACK => {
            //     resizeBuffer(&event.buffer.?, GETACK_BUFFER_SIZE);
            //     @memset(event.buffer.?, 0);
            //     event.ty = .RECEIVED_ACK;
            //     try event_queue.addAsyncEvent(event, true);
            // },
            // .RECEIVED_ACK => {
            //     var temp_allocator = std.heap.ArenaAllocator.init(allocator);
            //     defer temp_allocator.deinit();
            //
            //     if (event.async_result.? == 0) {
            //         //destroyEvent(event, allocator);
            //         continue;
            //     }
            //
            //     const response, _ = resp.Value.parse(event.buffer.?, temp_allocator.allocator()) catch {
            //         std.debug.print("{?x}\n", .{event.buffer});
            //         std.debug.print("{?d}\n", .{event.async_result});
            //         @panic("TODO");
            //     };
            //
            //     if (response != .array)
            //         @panic("TODO");
            //
            //     const new_offset = try std.fmt.parseInt(usize, response.array[2].bulk_string, 10);
            //
            //     const old_offset = slaves.get(event.fd);
            //     if (old_offset.? < new_offset) {
            //         try slaves.put(event.fd, new_offset);
            //     }
            //
            //     var new_pending_waits = std.PriorityQueue(*PendingWait, void, compareWaits).init(allocator, undefined);
            //     while (pending_waits.removeOrNull()) |pending_wait| {
            //         if (pending_wait.threshold_offset > old_offset.? and pending_wait.threshold_offset <= new_offset) {
            //             pending_wait.actual_n_replicas += 1;
            //             if (pending_wait.actual_n_replicas >= pending_wait.expected_n_replicas) {
            //                 const reply = resp.Integer(@bitCast(pending_wait.actual_n_replicas));
            //                 resizeBuffer(&pending_wait.client_event.buffer.?, CLIENT_BUFFER_SIZE);
            //                 pending_wait.client_event.buffer = try std.fmt.bufPrint(pending_wait.client_event.buffer.?, "{s}", .{try reply.encode(temp_allocator.allocator())});
            //                 pending_wait.client_event.ty = eq.EVENT_TYPE.SENT_RESPONSE;
            //                 try event_queue.addAsyncEvent(pending_wait.client_event, true);
            //                 allocator.destroy(pending_wait);
            //             } else {
            //                 try new_pending_waits.add(pending_wait);
            //             }
            //         } else {
            //             try new_pending_waits.add(pending_wait);
            //         }
            //     }
            //
            //     pending_waits.deinit();
            //     pending_waits = new_pending_waits;
            //
            //     allocator.destroy(event);
            //     allocator.free(event.buffer.?);
            // },
            // .TIMEOUT => {
            //     var temp_allocator = std.heap.ArenaAllocator.init(allocator);
            //     defer temp_allocator.deinit();
            //
            //     const pending_wait = event.pending_wait.?;
            //     var pending_wait_it = pending_waits.iterator();
            //     var maybe_index: ?usize = null;
            //     var i: usize = 0;
            //     while (pending_wait_it.next()) |pw| : (i += 1) {
            //         if (pw == event.pending_wait.?) {
            //             maybe_index = i;
            //             break;
            //         }
            //     }
            //     if (maybe_index) |index| {
            //         _ = pending_waits.removeIndex(index);
            //
            //         const reply = resp.Integer(@bitCast(pending_wait.actual_n_replicas));
            //         resizeBuffer(&pending_wait.client_event.buffer.?, CLIENT_BUFFER_SIZE);
            //         pending_wait.client_event.buffer = try std.fmt.bufPrint(pending_wait.client_event.buffer.?, "{s}", .{try reply.encode(temp_allocator.allocator())});
            //         pending_wait.client_event.ty = eq.EVENT_TYPE.SENT_RESPONSE;
            //         try event_queue.addAsyncEvent(pending_wait.client_event, true);
            //     }
            //
            //     allocator.destroy(pending_wait);
            //     posix.close(event.fd); // the timerfd
            //     // not destroying the event buffer because it's the client's
            //     // event buffer (will be destroyed on client connection closed)
            //     allocator.destroy(event);
            // },
            // .SIGTERM => {
            //     std.debug.print("Gracefully shutting down...\n", .{});
            //     break;
            // },
        }
    }
}
