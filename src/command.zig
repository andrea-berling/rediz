const std = @import("std");
const resp = @import("resp.zig");

pub fn execute(allocator: *std.mem.Allocator, command: [][]u8) ![]u8 {
    if (std.ascii.eqlIgnoreCase(command[0], "PING")) {
        const response = try resp.encode_simple_string("PONG"[0..], allocator);
        return response;
    }
    else if (std.ascii.eqlIgnoreCase(command[0], "ECHO")) {
        const response = try resp.encode_simple_string(command[1], allocator);
        return response;
    }
    else
    {
        @panic("Unsupported command");
    }
}
