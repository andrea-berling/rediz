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

inline fn addReceiveCommandEvent(fd: linux.socket_t, buffer: []u8, event_queue: *eq.EventQueue, allocator: Allocator) !void {
    var receive_command_event = try allocator.create(eq.Event);
    @memset(buffer, 0);
    receive_command_event.ty = eq.EVENT_TYPE.RECEIVE_COMMAND;
    receive_command_event.fd = fd;
    receive_command_event.buffer = buffer;
    try event_queue.addAsyncEvent(receive_command_event);
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
    std.debug.print("My PID is {}\n", .{ linux.getpid() });

    // Uncomment this block to pass the first stage

    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    // TODO: error handling?

    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    defer _ = gpa.deinit();
    var allocator = gpa.allocator();

    var event_queue = try eq.EventQueue.init(allocator, IO_URING_ENTRIES);
    defer event_queue.destroy() catch {@panic("Failed destroying the event queue");};

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
    try event_queue.addAsyncEvent(&termination_event);

    const connection_event: eq.Event = .{
        .ty = eq.EVENT_TYPE.CONNECTION,
        .fd = listener.stream.handle,
    };
    try event_queue.addAsyncEvent(&connection_event);

    var config = std.ArrayList(struct{[]const u8,[]const u8}).init(allocator);

    // TODO: I don't need a full data model for my options, a list of string pairs
    //  will suffice. I can take the parser out of clap and just use that part
    // First we specify what parameters our program can take.
    // We can use `parseParamsComptime` to parse a string into an array of `Param(Help)`.
    const params = comptime clap.parseParamsComptime(
        \\-h, --help             Display this help and exit.
        \\--dir <str>   An option parameter, which takes a value.
        \\--dbfilename <str>  An option parameter which can be specified multiple times.
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
        try config.append(.{
            "dir",dir
        });
    }

    if (res.args.dbfilename) |dbfilename| {
        try config.append(.{
            "dbfilename",dbfilename
        });
    }

    try config.append(.{
        "END",""
    });

    var instance = try db.Instance.init(allocator,config.allocatedSlice());
    defer instance.destroy(allocator);

    config.deinit();

    while (true) {
        try stdout.print("Waiting for something to come through\n", .{});
        const event = try event_queue.next();
        try stdout.print("Event fd/type/ptr: {} {} {*}\n", .{ event.fd, event.ty, event });
        switch (event.ty) {
            .CONNECTION => {
                const connection_socket = event.async_result.?;
                try stdout.print("accepted new connection\n", .{});
                const buffer = try allocator.alloc(u8, CLIENT_BUFFER_SIZE);
                try addReceiveCommandEvent(connection_socket, buffer, &event_queue, allocator);
                try event_queue.addAsyncEvent(&connection_event);
            },
            .RECEIVE_COMMAND => {
                const buffer = event.buffer.?;
                if (event.async_result.? == 0) { // Connection closed
                    destroyEvent(event, allocator);
                    continue;
                }
                if (event.async_result.? < 0) {
                    std.debug.print("Error: {} {d}\n", .{linux.E.init(@intCast(@as(u32,@bitCast(event.async_result.?)))), event.async_result.?});
                    destroyEvent(event, allocator);
                    continue;
                }
                const request, _ = resp.parseArray(allocator, buffer) catch |err| {
                    std.debug.print("Error: {}\n", .{err});
                    std.debug.print("Got: {any} (return value: {d})\n",.{ buffer, event.async_result.? });
                    // TODO: be more gracious if the client sends invalid commands
                    destroyEvent(event, allocator);
                    continue;
                };
                defer resp.destroyArray(allocator, request);
                const reply = instance.executeCommand(allocator,request) catch {
                    // TODO: what if the command fails? Should I close the connection and kill the event? Notify the client?
                    continue;
                };
                defer allocator.free(reply);
                event.buffer = event.buffer.?[0..reply.len]; // shrinking the buffer
                @memcpy(event.buffer.?, reply);
                event.ty = eq.EVENT_TYPE.SENT_RESPONSE;
                try event_queue.addAsyncEvent(event);
            },
            .SENT_RESPONSE => {
                event.buffer = (@as([*]u8,@ptrCast(event.buffer.?.ptr)))[0..CLIENT_BUFFER_SIZE]; // resizing the buffer
                event.ty = eq.EVENT_TYPE.RECEIVE_COMMAND;
                @memset(event.buffer.?, 0);
                try event_queue.addAsyncEvent(event);
            },
            .SIGTERM => {
                std.debug.print("Gracefully shutting down...\n",.{});
                break;
            }
        }
    }
}
