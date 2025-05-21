const std = @import("std");
const resp = @import("resp.zig");

const ValueType = enum {
    string,
};

const Value = union(ValueType) {
    string: []u8,
};

const Datum = struct {
    value: Value,
    created_at_ms: i64,
    ttl_ms: ?u64,
};

pub const Instance = struct {
    allocator: *std.mem.Allocator,
    data: std.StringHashMap(Datum),

    pub fn init(allocator: *std.mem.Allocator) Instance {
        return .{
            .allocator = allocator,
            .data = std.StringHashMap(Datum).init(allocator.*),
        };
    }

    pub fn destroy(self: *Instance) void {
        self.data.deinit();
    }

    pub fn execute_command(self: *Instance, command: [][]u8) ![]u8 {
        if (std.ascii.eqlIgnoreCase(command[0], "PING")) {
            const response = try resp.encode_simple_string("PONG"[0..], self.allocator);
            return response;
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "ECHO")) {
            const response = try resp.encode_simple_string(command[1], self.allocator);
            return response;
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "SET")) {
            try self.data.put(command[1], .{
                .created_at_ms = std.time.milliTimestamp(),
                .ttl_ms = if (command.len > 3 and std.ascii.eqlIgnoreCase(command[3], "PX")) try std.fmt.parseInt(u64, command[4], 10) else null,
                .value = .{
                    .string = command[2]
                }
            });
            const response = try resp.encode_simple_string("OK"[0..], self.allocator);
            return response;
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "GET")) {
            if ( self.data.get(command[1]) ) |data| {
                if ( data.ttl_ms ) |ttl_ms| {
                    if (data.created_at_ms + @as(i64,@bitCast(ttl_ms)) < std.time.milliTimestamp()) {
                        _ = self.data.remove(command[1]);
                        return try resp.encode_bulk_string(null, self.allocator);
                    }
                }
                const response = try resp.encode_bulk_string(data.value.string, self.allocator);
                return response;
            }
            else
                return try resp.encode_bulk_string(null, self.allocator);
        }
        else {
            std.debug.print("Unsupported command received: {s}\n",.{command[0]});
            return error.UnsupportedCommand;
        }
    }
};
