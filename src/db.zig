const std = @import("std");
const posix = std.posix;
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
    arena_allocator: *std.heap.ArenaAllocator,
    data: std.StringHashMap(Datum),
    config: std.StringHashMap([]u8),
    master: ?posix.socket_t = null,
    replid: []const u8,
    repl_offset: usize,

    inline fn dupe(self: *Instance, bytes: []const u8) ![]u8 {
        return try self.arena_allocator.allocator().dupe(u8,bytes);
    }

    pub fn init(allocator: std.mem.Allocator, config: ?[]struct{[]const u8,[]const u8}) !Instance {

        var instance: Instance = undefined;
        instance.arena_allocator = try allocator.create(std.heap.ArenaAllocator);
        instance.arena_allocator.* = std.heap.ArenaAllocator.init(allocator);
        instance.data = std.StringHashMap(Datum).init(instance.arena_allocator.allocator());
        instance.config = std.StringHashMap([]u8).init(instance.arena_allocator.allocator());
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
                const db = try file.readToEndAlloc(instance.arena_allocator.allocator(), RDB_FILE_SIZE_LIMIT);
                var temp_allocator = std.heap.ArenaAllocator.init(instance.arena_allocator.allocator());
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
            instance.master = try instance.handshake_and_sync(address, port);
        } else {
            // TODO: initialize the replid as a random 40 character alphanumeric string
        }

        return instance;
    }

    fn handshake_and_sync(self: *Instance, address: []const u8, port: u16) !posix.socket_t {
        var temp_allocator = std.heap.ArenaAllocator.init(self.arena_allocator.allocator());
        defer temp_allocator.deinit();
        var stream = try std.net.tcpConnectToHost(temp_allocator.allocator(),address,port);
        _ = try stream.write(try resp.encodeArray(temp_allocator.allocator(), &[_][]const u8{"PING"}));
        const buffer = try temp_allocator.allocator().alloc(u8, 1024);
        var n = try stream.read(buffer);
        std.debug.assert(std.ascii.eqlIgnoreCase(buffer[0..n], "+PONG\r\n"));
        _ = try stream.write(try resp.encodeArray(temp_allocator.allocator(), &[_][]const u8{"REPLCONF", "listening-port", self.config.get("listening-port").? }));
        n = try stream.read(buffer);
        std.debug.assert(std.ascii.eqlIgnoreCase(buffer[0..n], "+OK\r\n"));
        _ = try stream.write(try resp.encodeArray(temp_allocator.allocator(), &[_][]const u8{"REPLCONF", "capa", "psync2" }));
        n = try stream.read(buffer);
        std.debug.assert(std.ascii.eqlIgnoreCase(buffer[0..n], "+OK\r\n"));
        _ = try stream.write(try resp.encodeArray(temp_allocator.allocator(), &[_][]const u8{"PSYNC", "?", "-1" }));
        n = try stream.read(buffer);
        _ = try stream.read(buffer[n..]);
        std.debug.print("Handshake and sync with {s}:{d} was succesful!\n",.{address,port});
        return stream.handle;
    }

    pub fn destroy(self: *Instance, allocator: std.mem.Allocator) void {
        self.arena_allocator.deinit();
        allocator.destroy(self.arena_allocator);
    }

    pub fn executeCommand(self: *Instance, allocator: std.mem.Allocator, command: [][]u8) !struct { []u8, bool } {
        if (std.ascii.eqlIgnoreCase(command[0], "PING")) {
            return .{ try resp.encodeSimpleString(allocator, "PONG"[0..]), false };
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "ECHO")) {
            return .{ try resp.encodeSimpleString(allocator, command[1]), false };
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
            return .{ try resp.encodeSimpleString(allocator, "OK"[0..]), true };
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "GET")) {
            if ( self.data.get(command[1]) ) |data| {
                if ( data.expire_at_ms ) |expire_at_ms| {
                    if (expire_at_ms <= std.time.milliTimestamp()) {
                        _ = self.data.remove(command[1]);
                        return .{ try resp.encodeBulkString(allocator, null), false };
                    }
                }
                return  .{ try resp.encodeBulkString(allocator, data.value.string), false };
            }
            else
                return .{ try resp.encodeBulkString(allocator, null), false };
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
                return .{ try resp.encodeArray(allocator, keys), false };
            }
            else
                return .{ try resp.encodeBulkString(allocator, null), false };
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "CONFIG")) {
            if (std.ascii.eqlIgnoreCase(command[1], "GET")) {
                if ( self.config.get(command[2]) ) |data| {
                    return .{ try resp.encodeArray(allocator, &[_][]u8{ command[2], data }), false };
                }
                else
                    return .{ try resp.encodeBulkString(allocator, null), false };
            }
            else if (std.ascii.eqlIgnoreCase(command[1], "SET")) {
                try self.config.put(command[2], command[3]);
                return .{ try resp.encodeSimpleString(allocator, "OK"[0..]), false };
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
                return .{ try resp.encodeBulkString(allocator, try response.toOwnedSlice()), false };
            }
            else
                return error.InvalidInfoCommand;
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "REPLCONF")) {
            return .{ try resp.encodeSimpleString(allocator, "OK"[0..]), false };
        }
        else if (std.ascii.eqlIgnoreCase(command[0], "PSYNC")) {
            var response = std.ArrayList(u8).init(allocator);
            try std.fmt.format(response.writer(),"FULLRESYNC {s} {d}", .{self.replid, self.repl_offset});
            return .{ try resp.encodeSimpleString(allocator, try response.toOwnedSlice()), false };
        }
        else {
            std.debug.print("Unsupported command received: {s}\n",.{command[0]});
            return error.UnsupportedCommand;
        }
    }

    pub fn dumpToBuffer(_: *Instance, buffer: []u8) !usize {
        // TODO: Actually turn the in-memory representation to a RDB dump
        const empty_file = [_]u8 {
            0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72,  //  |REDIS0011..redis|
            0x65, 0x64, 0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32,  //  |-ver.7.2.0..redi|
            0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69,  //  |s-bits.@..ctime.|
            0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2,  //  |m..e..used-mem..|
            0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d,  //  |.....aof-base...|
            0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66,  //  |.n;...Z.|
            0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe,
            0xc0, 0xff, 0x5a, 0xa2
        };
        @memcpy(buffer[0..empty_file.len], &empty_file);
        return empty_file.len;
    }
};
