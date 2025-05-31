const std = @import("std");
const eq = @import("event_queue.zig");
const resp = @import("resp.zig");
const db = @import("db.zig");
const clap = @import("clap");

const net = std.net;
const posix = std.posix;
const linux = std.os.linux;
const Allocator = std.mem.Allocator;

const stdout = std.io.getStdOut().writer();

const CLIENT_BUFFER_SIZE = 1024;
const IO_URING_ENTRIES = 100;
const MASTER_CANARY = 0x1;

inline fn resizeBuffer(buffer: []u8, new_size: usize) []u8 {
    return (@as([*]u8, @ptrCast(buffer.ptr)))[0..new_size];
}

inline fn addReceiveCommandEvent(fd: linux.socket_t, buffer: []u8, event_queue: *eq.EventQueue, canary: ?u64, allocator: Allocator) !void {
    var receive_command_event = try allocator.create(eq.Event);
    @memset(buffer, 0);
    receive_command_event.ty = eq.EVENT_TYPE.RECEIVE_COMMAND;
    receive_command_event.fd = fd;
    receive_command_event.buffer = buffer;
    receive_command_event.canary = canary;
    try event_queue.addAsyncEvent(receive_command_event, true);
}

inline fn destroyEvent(event: *eq.Event, allocator: Allocator) void {
    posix.close(event.fd);
    if (event.buffer) |event_buffer| {
        allocator.free(event_buffer);
    }
    allocator.destroy(event);
}

pub fn main() !void {

    // You can use print statements as follows for debugging, they'll be visible when running tests.

    try stdout.print("Logs from your program will appear here!\n", .{});
    std.debug.print("My PID is {}\n", .{linux.getpid()});

    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    defer _ = gpa.deinit();
    var allocator = gpa.allocator();

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

    var listening_port = [_]u8{0} ** 5;

    try config.append(.{ "listening-port", try std.fmt.bufPrint(&listening_port, "{d}", .{res.args.port orelse 6379}) });

    try config.append(.{
        // TODO: Janky, to be fixed
        "END", "",
    });

    const port = res.args.port orelse 6379;

    const address = try net.Address.resolveIp("127.0.0.1", port);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    std.debug.print("Listening on port {}...\n", .{port});

    var event_queue = try eq.EventQueue.init(allocator, IO_URING_ENTRIES);
    defer event_queue.destroy() catch {
        @panic("Failed destroying the event queue");
    };

    var sigset = posix.empty_sigset;
    linux.sigaddset(&sigset, posix.SIG.TERM);
    linux.sigaddset(&sigset, posix.SIG.INT);
    posix.sigprocmask(posix.SIG.BLOCK, &sigset, null);
    const sfd = try posix.signalfd(-1, &sigset, linux.SFD.NONBLOCK | linux.SFD.CLOEXEC);
    defer posix.close(sfd);
    const termination_event: eq.Event = .{
        .ty = eq.EVENT_TYPE.SIGTERM,
        .fd = sfd,
    };
    try event_queue.addAsyncEvent(&termination_event, false);

    const connection_event: eq.Event = .{
        .ty = eq.EVENT_TYPE.CONNECTION,
        .fd = listener.stream.handle,
    };
    try event_queue.addAsyncEvent(&connection_event, false);

    var instance = try db.Instance.init(allocator, config.allocatedSlice());
    defer instance.destroy(allocator);

    if (instance.master) |master_fd| {
        try addReceiveCommandEvent(master_fd, try allocator.alloc(u8, CLIENT_BUFFER_SIZE), &event_queue, MASTER_CANARY, allocator);
    }

    config.deinit();

    var slaves = std.AutoHashMap(posix.socket_t, void).init(allocator);
    defer slaves.deinit();

    event_loop: while (true) {
        try stdout.print("Waiting for something to come through...\n", .{});
        const event = try event_queue.next();
        try stdout.print("Event fd/type/ptr: {} {} {*}\n", .{ event.fd, event.ty, event });
        switch (event.ty) {
            .CONNECTION => {
                const connection_socket = event.async_result.?;
                try stdout.print("accepted new connection\n", .{});
                try addReceiveCommandEvent(connection_socket, try allocator.alloc(u8, CLIENT_BUFFER_SIZE), &event_queue, null, allocator);
                try event_queue.addAsyncEvent(&connection_event, false);
            },
            .RECEIVE_COMMAND => {
                const buffer = event.buffer.?;
                const return_value = event.async_result.?;

                if (return_value == 0) { // Connection closed
                    _ = slaves.remove(event.fd); // Might be a slave, doesn't hurt if not
                    destroyEvent(event, allocator);
                    continue;
                }

                if (return_value < 0) {
                    std.debug.print("Error: {} {d}\n", .{ linux.E.init(@intCast(@as(u32, @bitCast(return_value)))), return_value });
                    _ = slaves.remove(event.fd); // Might be a slave, doesn't hurt if not
                    destroyEvent(event, allocator);
                    continue;
                }

                const recv_return_value = @as(usize, @intCast(@as(u32, @bitCast(return_value))));

                var temp_allocator = std.heap.ArenaAllocator.init(allocator);
                defer temp_allocator.deinit();

                var replies = std.ArrayList(u8).init(temp_allocator.allocator());

                var n: usize = 0;
                while (recv_return_value - n != 0) {
                    const request, const bytes_parsed = resp.parseArray(temp_allocator.allocator(), buffer[n..]) catch |err| {
                        std.debug.print("Error: {}\n", .{err});
                        std.debug.print("Got: {x} (return value: {d})\n", .{ buffer, recv_return_value });
                        _ = slaves.remove(event.fd); // Might be a slave, doesn't hurt if not
                        // TODO: be more gracious if the client sends invalid commands
                        destroyEvent(event, allocator);
                        continue;
                    };
                    if (instance.master != null and event.canary != null and event.canary.? == MASTER_CANARY) {
                        instance.repl_offset += bytes_parsed;
                    }
                    if (std.ascii.eqlIgnoreCase(request[0], "PSYNC")) {
                        const reply, _ = try instance.executeCommand(temp_allocator.allocator(), request);
                        event.ty = .FULL_SYNC;
                        event.buffer = resizeBuffer(event.buffer.?, reply.len);
                        @memcpy(event.buffer.?, reply);
                        try event_queue.addAsyncEvent(event, true);
                        continue :event_loop;
                    }
                    const reply, const propagate = instance.executeCommand(temp_allocator.allocator(), request) catch {
                        // TODO: what if the command fails? Should I close the connection and kill the event? Notify the client?
                        continue;
                    };

                    if (instance.master != null and event.canary != null and event.canary.? == MASTER_CANARY) {
                        if (std.ascii.eqlIgnoreCase(request[0], "REPLCONF") and
                            std.ascii.eqlIgnoreCase(request[1], "GETACK"))
                        {
                            try replies.appendSlice(reply);
                        }
                    } else {
                        try replies.appendSlice(reply);
                    }

                    if (propagate) {
                        var slaves_it = slaves.keyIterator();
                        while (slaves_it.next()) |slave| {
                            var propagation_event = try allocator.create(eq.Event);
                            propagation_event.ty = eq.EVENT_TYPE.PROPAGATE_COMMAND;
                            propagation_event.fd = slave.*;
                            propagation_event.buffer = try allocator.alloc(u8, n + bytes_parsed);
                            propagation_event.canary = null;
                            @memcpy(propagation_event.buffer.?, buffer[n..][0..bytes_parsed]);
                            try event_queue.addAsyncEvent(propagation_event, true);
                        }
                    }
                    n += bytes_parsed;
                }

                const reply = try replies.toOwnedSlice();

                if (reply.len > 0) {
                    event.ty = eq.EVENT_TYPE.SENT_RESPONSE;
                    event.buffer = resizeBuffer(event.buffer.?, reply.len);
                    @memcpy(event.buffer.?, reply);
                    try event_queue.addAsyncEvent(event, true);
                } else {
                    event.buffer = resizeBuffer(event.buffer.?, CLIENT_BUFFER_SIZE);
                    event.ty = eq.EVENT_TYPE.RECEIVE_COMMAND;
                    @memset(event.buffer.?, 0);
                    try event_queue.addAsyncEvent(event, true);
                }
            },
            .FULL_SYNC => {
                var temp_allocator = std.heap.ArenaAllocator.init(allocator);
                defer temp_allocator.deinit();
                // TODO: not stricly correct, whatever
                const tmp_buf = try temp_allocator.allocator().alloc(u8, CLIENT_BUFFER_SIZE);
                const dump_size = try instance.dumpToBuffer(tmp_buf);
                const preamble = try std.fmt.bufPrint(event.buffer.?, "${d}\r\n", .{dump_size});
                event.buffer = resizeBuffer(event.buffer.?, CLIENT_BUFFER_SIZE);
                @memcpy(event.buffer.?[preamble.len..][0..dump_size], tmp_buf[0..dump_size]);
                event.buffer = resizeBuffer(event.buffer.?, preamble.len + dump_size);
                event.ty = eq.EVENT_TYPE.SENT_DUMP;
                try event_queue.addAsyncEvent(event, true);
            },
            .SENT_RESPONSE, .SENT_DUMP => {
                if (event.ty == .SENT_DUMP) {
                    try slaves.put(event.fd, {});
                }
                event.buffer = resizeBuffer(event.buffer.?, CLIENT_BUFFER_SIZE);
                event.ty = eq.EVENT_TYPE.RECEIVE_COMMAND;
                @memset(event.buffer.?, 0);
                try event_queue.addAsyncEvent(event, true);
            },
            .PROPAGATE_COMMAND => {
                allocator.free(event.buffer.?);
                allocator.destroy(event);
            },
            .SIGTERM => {
                std.debug.print("Gracefully shutting down...\n", .{});
                break;
            },
        }
    }
}
