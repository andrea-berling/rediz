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
    expire_at_ms: ?i64,
};

pub const Instance = struct {
    allocator: *std.heap.ArenaAllocator,
    data: std.StringHashMap(Datum),
    config: std.StringHashMap([]u8),

    inline fn copy(self: *Instance, bytes: []const u8) ![]u8 {
        return try std.mem.Allocator.dupe(self.allocator.allocator(), u8, bytes);
    }

    pub fn init(allocator: std.mem.Allocator, config: ?[]struct{[]const u8,[]const u8}) !Instance {

        var instance: Instance = undefined;
        instance.allocator = try allocator.create(std.heap.ArenaAllocator);
        instance.allocator.* = std.heap.ArenaAllocator.init(allocator);
        instance.data = std.StringHashMap(Datum).init(instance.allocator.allocator());
        instance.config = std.StringHashMap([]u8).init(instance.allocator.allocator());

        const config_defaults: [2]struct{[]const u8,[]const u8} = .{
            .{ "dir", "."},
            .{ "dbfilename", "dump.rdb"}
        };

        for (config_defaults) |config_pair| {
            try instance.config.put(
                try instance.copy(config_pair[0]),
                try instance.copy(config_pair[1]),
            );
        }

        if (config) |pairs| {
            for (pairs) |config_pair| {
                if (std.mem.eql(u8, config_pair[0], "END")) break;
                try instance.config.put(
                    try instance.copy(config_pair[0]),
                    try instance.copy(config_pair[1]),
                );
            }
        }

        return instance;
    }

    pub fn destroy(self: *Instance, allocator: std.mem.Allocator) void {
        self.allocator.deinit();
        allocator.destroy(self.allocator);
    }

    pub fn executeCommand(self: *Instance, allocator: std.mem.Allocator, command: [][]u8) ![]u8 {
        if (std.ascii.eqlIgnoreCase(command[0], "PING")) {
            const response = try resp.encodeSimpleString(allocator, "PONG"[0..]);
            return response;
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "ECHO")) {
            const response = try resp.encodeSimpleString(allocator, command[1]);
            return response;
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "SET")) {
            try self.data.put(try self.copy(command[1]), .{
                .expire_at_ms = if (command.len > 3) {
                    if (std.ascii.eqlIgnoreCase(command[3], "PX")) {
                        std.time.milliTimestamp() + try std.fmt.parseInt(u64, command[4], 10);
                    } else if (std.ascii.eqlIgnoreCase(command[3], "PXAT")) {
                        try std.fmt.parseInt(u64, command[4], 10);
                    } else if (std.ascii.eqlIgnoreCase(command[3], "EX")) {
                        std.time.milliTimestamp() + try std.fmt.parseInt(u64, command[4], 10)*1000;
                    } else if (std.ascii.eqlIgnoreCase(command[3], "EXAT")) {
                        try std.fmt.parseInt(u64, command[4], 10)*1000;
                    }
                } else null,
                .value = .{
                    .string = try self.copy(command[2])
                }
            });
            const response = try resp.encodeSimpleString(allocator, "OK"[0..]);
            return response;
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "GET")) {
            if ( self.data.get(command[1]) ) |data| {
                if ( data.expire_at_ms ) |expire_at_ms| {
                    if (expire_at_ms >= std.time.milliTimestamp()) {
                        _ = self.data.remove(command[1]);
                        return try resp.encodeBulkString(allocator, null);
                    }
                }
                const response = try resp.encodeBulkString(allocator, data.value.string);
                return response;
            }
            else
                return try resp.encodeBulkString(allocator, null);
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "CONFIG")) {
            if (std.ascii.eqlIgnoreCase(command[1], "GET")) {
                if ( self.config.get(command[2]) ) |data| {
                    const response = try resp.encodeArray(allocator, &[_][]u8{ command[2], data });
                    return response;
                }
                else
                    return try resp.encodeBulkString(allocator, null);
            }
            else if (std.ascii.eqlIgnoreCase(command[1], "SET")) {
                try self.config.put(command[2], command[3]);
                const response = try resp.encodeSimpleString(allocator, "OK"[0..]);
                return response;
            }
            else
                return error.InvalidConfigCommand;
        }
        else {
            std.debug.print("Unsupported command received: {s}\n",.{command[0]});
            return error.UnsupportedCommand;
        }
    }
};
