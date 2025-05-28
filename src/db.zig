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
    master: ?std.net.Address = null,
    replid: []const u8,
    repl_offset: usize,

    inline fn dupe(self: *Instance, bytes: []const u8) ![]u8 {
        return try self.allocator.allocator().dupe(u8,bytes);
    }

    pub fn init(allocator: std.mem.Allocator, config: ?[]struct{[]const u8,[]const u8}) !Instance {

        var instance: Instance = undefined;
        instance.allocator = try allocator.create(std.heap.ArenaAllocator);
        instance.allocator.* = std.heap.ArenaAllocator.init(allocator);
        instance.data = std.StringHashMap(Datum).init(instance.allocator.allocator());
        instance.config = std.StringHashMap([]u8).init(instance.allocator.allocator());
        instance.master = null;
        instance.repl_offset = 0;

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
                    if (pair.value.expire_at_ms) |expire_at_ms| {
                        if (expire_at_ms <= std.time.milliTimestamp()) {
                            std.debug.print("Key {s} already expired at {d}, not adding it\n", .{ pair.key, expire_at_ms });
                            continue;
                        }
                    }
                    var new_datum: Datum = .{ .expire_at_ms = pair.value.expire_at_ms, .value = undefined };
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

        instance.replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
        if (instance.config.get("master")) |master| {
            var it = std.mem.splitScalar(u8, master, ' ');
            var address = it.next().?;
            // TODO: DNS
            if (std.mem.eql(u8,address,"localhost")) {
                address = "127.0.0.1";
            }
            const port = try std.fmt.parseInt(u16, it.next().?, 10);
            instance.master = .{
                .in = try std.net.Ip4Address.resolveIp(address, port)
            };
        } else {
            // TODO: initialize the replid as a random 40 character alphanumeric string
        }

        return instance;
    }

    pub fn destroy(self: *Instance, allocator: std.mem.Allocator) void {
        self.allocator.deinit();
        allocator.destroy(self.allocator);
    }

    pub fn executeCommand(self: *Instance, allocator: std.mem.Allocator, command: [][]u8) ![]u8 {
        if (std.ascii.eqlIgnoreCase(command[0], "PING")) {
            return try resp.encodeSimpleString(allocator, "PONG"[0..]);
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "ECHO")) {
            return try resp.encodeSimpleString(allocator, command[1]);
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
            return try resp.encodeSimpleString(allocator, "OK"[0..]);
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "GET")) {
            if ( self.data.get(command[1]) ) |data| {
                if ( data.expire_at_ms ) |expire_at_ms| {
                    if (expire_at_ms <= std.time.milliTimestamp()) {
                        _ = self.data.remove(command[1]);
                        return try resp.encodeBulkString(allocator, null);
                    }
                }
                return  try resp.encodeBulkString(allocator, data.value.string);
            }
            else
                return try resp.encodeBulkString(allocator, null);
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "KEYS")) {
            // TODO: support patterns
            if ( command[1][0] == '*' ) {
                var keys = try allocator.alloc([]const u8,self.data.count());

                var keys_iterator = self.data.keyIterator();
                var i: usize = 0;
                while (keys_iterator.next()) |key|: (i += 1) {
                    keys[i] = key.*;
                }
                return try resp.encodeArray(allocator, keys);
            }
            else
                return try resp.encodeBulkString(allocator, null);
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "CONFIG")) {
            if (std.ascii.eqlIgnoreCase(command[1], "GET")) {
                if ( self.config.get(command[2]) ) |data| {
                    return try resp.encodeArray(allocator, &[_][]u8{ command[2], data });
                }
                else
                    return try resp.encodeBulkString(allocator, null);
            }
            else if (std.ascii.eqlIgnoreCase(command[1], "SET")) {
                try self.config.put(command[2], command[3]);
                return try resp.encodeSimpleString(allocator, "OK"[0..]);
            }
            else
                return error.InvalidConfigCommand;
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "INFO")) {
            if (std.ascii.eqlIgnoreCase(command[1], "REPLICATION")) {
                var response = std.ArrayList(u8).init(allocator);
                try response.appendSlice(if (self.master) |_| "role:slave\n" else "role:master\n");
                try std.fmt.format(response.writer(),"master_replid:{s}\n",.{self.replid});
                try std.fmt.format(response.writer(),"master_repl_offset:{d}\n",.{self.repl_offset});
                return try resp.encodeBulkString(allocator, try response.toOwnedSlice());
            }
            else
                return error.InvalidInfoCommand;
        }
        else {
            std.debug.print("Unsupported command received: {s}\n",.{command[0]});
            return error.UnsupportedCommand;
        }
    }
};
