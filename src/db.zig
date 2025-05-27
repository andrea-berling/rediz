const std = @import("std");
const resp = @import("resp.zig");
const rdb = @import("rdb.zig");

const RDB_FILE_SIZE_LIMIT = 100*1024*1024*1024;

const ValueType = enum {
    string,
};

const Value = union(ValueType) {
    string: []u8,
};

pub const Datum = struct {
    value: Value,
    expire_at_ms: ?i64,
};

pub const Instance = struct {
    allocator: *std.heap.ArenaAllocator,
    data: std.StringHashMap(Datum),
    config: std.StringHashMap([]u8),

    inline fn dupe(self: *Instance, bytes: []const u8) ![]u8 {
        return try self.allocator.allocator().dupe(u8,bytes);
    }

    pub fn init(allocator: std.mem.Allocator, config: ?[]struct{[]const u8,[]const u8}) !Instance {

        var instance: Instance = undefined;
        instance.allocator = try allocator.create(std.heap.ArenaAllocator);
        instance.allocator.* = std.heap.ArenaAllocator.init(allocator);
        instance.data = std.StringHashMap(Datum).init(instance.allocator.allocator());
        instance.config = std.StringHashMap([]u8).init(instance.allocator.allocator());

        const config_defaults = [_]struct{[]const u8,[]const u8}{
            .{ "dbfilename", "dump.rdb"}
        };

        for (config_defaults) |config_pair| {
            try instance.config.put(
                try instance.dupe(config_pair[0]),
                try instance.dupe(config_pair[1]),
            );
        }

        if (config) |pairs| {
            for (pairs) |config_pair| {
                if (std.mem.eql(u8, config_pair[0], "END")) break;
                try instance.config.put(
                    try instance.dupe(config_pair[0]),
                    try instance.dupe(config_pair[1]),
                );
            }
        }

        var rdb_directory = std.fs.cwd();

        if (instance.config.get("dir")) |dir| {
            rdb_directory = try std.fs.openDirAbsolute(dir, .{});
        }

        if (instance.config.get("dbfilename")) |dbfilename| {
            std.debug.print("Initializing from RDB file {s}...\n", .{dbfilename});
            if (rdb_directory.openFile(dbfilename, .{})) |file| {
                const db = try file.readToEndAlloc(instance.allocator.allocator(), RDB_FILE_SIZE_LIMIT);
                var temp_allocator = std.heap.ArenaAllocator.init(instance.allocator.allocator());
                defer temp_allocator.deinit();
                const data, _ = try rdb.parseData(db, temp_allocator.allocator());
                for (data) |pair| {
                    var new_datum: Datum = .{ .expire_at_ms = pair.value.expire_at_ms, .value = undefined };
                    std.debug.print("Key: {s} Expiration: {?}\n", .{ pair.key, pair.value.expire_at_ms });
                    new_datum.value.string = try instance.dupe(pair.value.value.string);
                    try instance.data.put(try instance.dupe(pair.key),new_datum);
                }
            } else |err| {
                if (err != error.FileNotFound) {
                    @panic("Error opening dbfilename");
                }
                std.debug.print("File {s} not found\n", .{dbfilename});
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
            try self.data.put(try self.dupe(command[1]), .{
                .expire_at_ms = if (command.len > 3) blk: {
                    if (std.ascii.eqlIgnoreCase(command[3], "PX")) {
                        break :blk std.time.milliTimestamp() + @as(i64,@bitCast(try std.fmt.parseInt(u64, command[4], 10)));
                    } else if (std.ascii.eqlIgnoreCase(command[3], "PXAT")) {
                        break :blk @as(i64,@bitCast(try std.fmt.parseInt(u64, command[4], 10)));
                    } else if (std.ascii.eqlIgnoreCase(command[3], "EX")) {
                        break :blk std.time.milliTimestamp() + @as(i64,@bitCast(try std.fmt.parseInt(u64, command[4], 10)))*1000;
                    } else if (std.ascii.eqlIgnoreCase(command[3], "EXAT")) {
                        break :blk @as(i64,@bitCast(try std.fmt.parseInt(u64, command[4], 10)))*1000;
                    } else break :blk null;
                } else null,
                .value = .{
                    .string = try self.dupe(command[2])
                }
            });
            const response = try resp.encodeSimpleString(allocator, "OK"[0..]);
            return response;
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "GET")) {
            if ( self.data.get(command[1]) ) |data| {
                if ( data.expire_at_ms ) |expire_at_ms| {
                    if (expire_at_ms <= std.time.milliTimestamp()) {
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
