const std = @import("std");
const net = std.net;

const READ_BUFFER_SIZE = 1024;

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    try stdout.print("Logs from your program will appear here!", .{});

    // Uncomment this block to pass the first stage

    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();
        try stdout.print("accepted new connection\n", .{});

        var buffer = std.mem.zeroes([READ_BUFFER_SIZE]u8);
        _ = try connection.stream.read(&buffer);

        var commmands_iterator = std.mem.splitSequence(u8, &buffer, "\n");
        while (commmands_iterator.next()) |_| {
            _ = try connection.stream.write("+PONG\r\n");
        }

        connection.stream.close();
    }
}
