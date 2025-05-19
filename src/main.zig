const std = @import("std");
const net = std.net;
const posix = std.posix;
const epoll_event = std.posix.system.epoll_event;
const epoll_data = std.posix.system.epoll_data;
const linux = std.os.linux;
const stdout = std.io.getStdOut().writer();

const READ_BUFFER_SIZE = 256;
const MAX_DECIMAL_LEN = 10;

fn parse_decimal(bytes: []u8) !struct { u64, usize } {
    var i: usize = 0;
    var return_value: u64 = 0;

    while (std.ascii.isDigit(bytes[i]) and i < MAX_DECIMAL_LEN) : (i += 1) {
        return_value = return_value * 10 + bytes[i] - '0';
    }

    return .{ return_value, i };
}

fn parse_bulk_string(bytes: []u8, allocator: std.mem.Allocator) !struct { []u8, usize } {
    if (bytes[0] != '$') return error.InvalidRESPBulkString;
    var i: usize = 1;
    const string_length, const bytes_parsed = try parse_decimal(bytes[i..]);
    i += bytes_parsed;

    if (!std.mem.eql(u8, bytes[i .. i + 2], "\r\n")) return error.InvalidRESPBulkString;
    i += 2;
    const return_value = try allocator.alloc(u8, string_length);
    std.mem.copyBackwards(u8, return_value, bytes[i .. i + string_length]);
    i += string_length;
    if (!std.mem.eql(u8, bytes[i .. i + 2], "\r\n")) return error.InvalidRESPBulkString;
    i += 2;
    return .{ return_value, i };
}

fn parse_array(bytes: []u8, allocator: std.mem.Allocator) !struct { [][]u8, usize } {
    if (bytes[0] != '*') return error.InvalidRESPArray;
    var i: usize = 1;
    const n_elem, const bytes_parsed = try parse_decimal(bytes[i..]);
    i += bytes_parsed;
    if (!std.mem.eql(u8, bytes[i .. i + 2], "\r\n")) return error.InvalidRESPArray;
    i += 2;
    var elements = try allocator.alloc([]u8, n_elem);

    for (0..n_elem) |n| {
        const element, const parsed_bytes = try parse_bulk_string(bytes[i..], allocator);
        elements[n] = element;
        i += parsed_bytes;
    }
    return .{ elements, i };
}

pub const EVENT_TYPE = enum {
    CONNECTION,
    RECEIVED_COMMAND,
    SENT_RESPONSE,
};

pub const Event = struct {
    ty: EVENT_TYPE = EVENT_TYPE.CONNECTION,
    fd: posix.socket_t,
    buffer: ?[]u8 = null,
};

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
    const connection_event_data: Event = .{
        .ty = EVENT_TYPE.CONNECTION,
        .fd = listener.stream.handle,
    };

    const epoll_fd = try posix.epoll_create1(0);
    try posix.epoll_ctl(epoll_fd, linux.EPOLL.CTL_ADD, listener.stream.handle, @constCast(&epoll_event{
        .events = posix.POLL.IN,
        .data = epoll_data{ .ptr = @intFromPtr(&connection_event_data) },
    }));

    var allocator = std.heap.page_allocator;

    const events = try allocator.alloc(epoll_event, 10);
    defer allocator.free(events);
    while (true) {
        try stdout.print("Waiting for something to come through\n", .{});
        const ready_fds = posix.epoll_wait(epoll_fd, events, -1);
        for (0..ready_fds) |i| {
            const event_data: *Event = @ptrFromInt(events[i].data.ptr);
            try stdout.print("Event fd/type/ptr: {} {} {*}\n", .{ event_data.fd, event_data.ty, @as(*Event, @ptrFromInt(events[i].data.ptr)) });
            switch (event_data.ty) {
                EVENT_TYPE.CONNECTION => {
                    const connection_socket = try posix.accept(event_data.fd, null, null, posix.SOCK.NONBLOCK);
                    try stdout.print("accepted new connection\n", .{});
                    var buffer = [_]u8{0} ** READ_BUFFER_SIZE;
                    _ = posix.read(connection_socket, &buffer) catch |err| {
                        switch (err) {
                            posix.ReadError.WouldBlock => {
                                try stdout.print("Would block on read\n", .{});
                                var client_event_data = try allocator.create(Event);
                                client_event_data.ty = EVENT_TYPE.RECEIVED_COMMAND;
                                client_event_data.fd = connection_socket;
                                try posix.epoll_ctl(epoll_fd, linux.EPOLL.CTL_ADD, connection_socket, @constCast(&epoll_event{
                                    .events = posix.POLL.IN,
                                    .data = epoll_data{ .ptr = @intFromPtr(client_event_data) },
                                }));
                                continue;
                            },
                            else => {},
                        }
                    };

                    _ = try parse_array(&buffer, std.heap.page_allocator);
                    _ = posix.write(connection_socket, "+PONG\r\n"[0..]) catch |err| {
                        switch (err) {
                            posix.WriteError.WouldBlock => {
                                try stdout.print("Would block on write\n", .{});
                                var client_event_data = try allocator.create(Event);
                                client_event_data.ty = EVENT_TYPE.SENT_RESPONSE;
                                client_event_data.fd = connection_socket;
                                client_event_data.buffer = &buffer;
                                try posix.epoll_ctl(epoll_fd, linux.EPOLL.CTL_ADD, connection_socket, @constCast(&epoll_event{
                                    .events = posix.POLL.OUT,
                                    .data = epoll_data{ .ptr = @intFromPtr(client_event_data) },
                                }));
                                continue;
                            },
                            else => {},
                        }
                    };

                    var client_event_data = try allocator.create(Event);
                    client_event_data.ty = EVENT_TYPE.RECEIVED_COMMAND;
                    client_event_data.fd = connection_socket;
                    try posix.epoll_ctl(epoll_fd, linux.EPOLL.CTL_ADD, connection_socket, @constCast(&epoll_event{
                        .events = posix.POLL.IN,
                        .data = epoll_data{ .ptr = @intFromPtr(client_event_data) },
                    }));
                },
                EVENT_TYPE.RECEIVED_COMMAND => {
                    var buffer = [_]u8{0} ** READ_BUFFER_SIZE;
                    const n = try posix.read(event_data.fd, &buffer);
                    if (n == 0) {
                        try posix.epoll_ctl(epoll_fd, linux.EPOLL.CTL_DEL, event_data.fd, null);
                        posix.close(event_data.fd);
                        continue;
                    }
                    _ = try parse_array(&buffer, std.heap.page_allocator);
                    const reply = "+PONG\r\n";
                    var reply_buffer = [_]u8{0}**reply.len;
                    @memcpy(&reply_buffer,reply);
                    _ = posix.write(event_data.fd, &reply_buffer) catch |err| {
                        switch (err) {
                            posix.WriteError.WouldBlock => {
                                var client_event_data = try allocator.create(Event);
                                client_event_data.ty = EVENT_TYPE.SENT_RESPONSE;
                                client_event_data.fd = event_data.fd;
                                client_event_data.buffer = &reply_buffer;
                                try posix.epoll_ctl(epoll_fd, linux.EPOLL.CTL_ADD, event_data.fd, @constCast(&epoll_event{
                                    .events = posix.POLL.OUT,
                                    .data = epoll_data{ .ptr = @intFromPtr(client_event_data) },
                                }));
                            },
                            else => {},
                        }
                    };
                },
                EVENT_TYPE.SENT_RESPONSE => {
                    _ = try posix.write(event_data.fd, event_data.buffer.?);
                },
            }
        }
    }
}
