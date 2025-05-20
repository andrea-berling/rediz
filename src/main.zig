const std = @import("std");
const net = std.net;
const eq = @import("event_queue.zig");
const resp = @import("resp.zig");
const posix = std.posix;
const linux = std.os.linux;
const stdout = std.io.getStdOut().writer();

const READ_BUFFER_SIZE = 256;
const IO_URING_ENTRIES = 100;

inline fn add_receive_command_event(allocator: *std.mem.Allocator, fd: linux.socket_t, buffer: []u8, event_queue: *eq.EventQueue) !void {
    var receive_command_event = try allocator.create(eq.Event);
    @memset(buffer, 0);
    receive_command_event.ty = eq.EVENT_TYPE.RECEIVE_COMMAND;
    receive_command_event.fd = fd;
    receive_command_event.buffer = buffer;
    try event_queue.add_async_event(receive_command_event);
}

inline fn add_send_response_event(allocator: *std.mem.Allocator, fd: linux.socket_t, buffer: []u8, event_queue: *eq.EventQueue) !void {
    var send_response_event = try allocator.create(eq.Event);
    send_response_event.ty = eq.EVENT_TYPE.SENT_RESPONSE;
    send_response_event.fd = fd;
    send_response_event.buffer = buffer;
    try event_queue.add_async_event(send_response_event);
}

pub fn main() !void {

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    try stdout.print("Logs from your program will appear here!\n", .{});

    // Uncomment this block to pass the first stage

    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var listener = try address.listen(.{
        .reuse_address = true,
        .force_nonblocking = true,
    });
    defer listener.deinit();

    // TODO: error handling?
    // TODO: freeing memory up?
    const connection_event: eq.Event = .{
        .ty = eq.EVENT_TYPE.CONNECTION,
        .fd = listener.stream.handle,
    };

    var allocator = std.heap.page_allocator;

    var event_queue = try eq.EventQueue.init(&allocator, IO_URING_ENTRIES);
    defer event_queue.destroy(&allocator) catch {unreachable;};
    try event_queue.add_async_event(&connection_event);
    while (true) {
        try stdout.print("Waiting for something to come through\n", .{});
        const event = try event_queue.next();
        try stdout.print("Event fd/type/ptr: {} {} {*}\n", .{ event.fd, event.ty, event });
        switch (event.ty) {
            eq.EVENT_TYPE.CONNECTION => {
                const connection_socket = event.async_result.?;
                try stdout.print("accepted new connection\n", .{});
                const buffer = try allocator.alloc(u8, READ_BUFFER_SIZE);
                try add_receive_command_event(&allocator, connection_socket, buffer, &event_queue);
                try event_queue.add_async_event(&connection_event);
            },
            eq.EVENT_TYPE.RECEIVE_COMMAND => {
                const buffer = event.buffer.?;
                if (event.async_result.? == 0) {
                    posix.close(event.fd);
                    if (event.buffer) |recv_buffer| {
                        allocator.free(recv_buffer);
                    }
                    continue;
                }
                _ = try resp.parse_array(buffer, std.heap.page_allocator);
                allocator.free(buffer);
                const reply = "+PONG\r\n";
                const reply_buffer = try allocator.alloc(u8, reply.len);
                @memcpy(reply_buffer,reply);
                try add_send_response_event(&allocator, event.fd, reply_buffer, &event_queue);
            },
            eq.EVENT_TYPE.SENT_RESPONSE => {
                const buffer = try allocator.alloc(u8, READ_BUFFER_SIZE);
                try add_receive_command_event(&allocator, event.fd, buffer, &event_queue);
            },
        }
    }
}
