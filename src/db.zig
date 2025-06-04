const std = @import("std");
const posix = std.posix;
const resp = @import("resp.zig");
const rdb = @import("rdb.zig");

const RDB_FILE_SIZE_LIMIT = 100 * 1024 * 1024 * 1024;

const ValueType = enum { string, stream };

const Stream = std.AutoArrayHashMap(u64, std.StringHashMap([]u8));

pub const Value = union(ValueType) { string: []u8, stream: Stream };

pub const Datum = struct {
    value: Value,
    expire_at_ms: ?i64,
};

fn parseStreamEntryId(string: []const u8) !u64 {
    if (std.mem.eql(u8, string, "*"))
        return ~@as(u64, 0);
    const separator_pos = std.mem.indexOf(u8, string, "-") orelse string.len;
    const timestamp: u48 = try std.fmt.parseInt(u48, string[0..separator_pos], 10);
    if (separator_pos == string.len) {
        return @as(u64, timestamp) << 16;
    }
    const sequence_number: u16 = if (std.mem.eql(u8, string[separator_pos + 1 ..], "*")) 0xffff else try std.fmt.parseInt(u16, string[separator_pos + 1 ..], 10);
    return (@as(u64, timestamp) << 16) | sequence_number;
}

pub const Instance = struct {
    arena_allocator: *std.heap.ArenaAllocator,
    data: std.StringHashMap(Datum),
    config: std.StringHashMap([]u8),
    master: ?posix.socket_t = null,
    replid: []const u8,
    repl_offset: usize,
    n_slaves: i64,
    rng: std.Random.DefaultPrng,

    inline fn dupe(self: *Instance, bytes: []const u8) ![]u8 {
        return try self.arena_allocator.allocator().dupe(u8, bytes);
    }

    pub fn init(allocator: std.mem.Allocator, config: ?[]struct { []const u8, []const u8 }) !Instance {
        var instance: Instance = undefined;
        instance.arena_allocator = try allocator.create(std.heap.ArenaAllocator);
        instance.arena_allocator.* = std.heap.ArenaAllocator.init(allocator);
        instance.data = std.StringHashMap(Datum).init(instance.arena_allocator.allocator());
        instance.config = std.StringHashMap([]u8).init(instance.arena_allocator.allocator());
        instance.master = null;
        instance.repl_offset = 0;
        instance.n_slaves = 0;
        instance.rng = std.Random.DefaultPrng.init(@bitCast(std.time.microTimestamp()));

        const config_defaults = [_]struct { []const u8, []const u8 }{.{ "dbfilename", "dump.rdb" }};

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
                var temp_allocator = std.heap.ArenaAllocator.init(instance.arena_allocator.allocator());
                defer temp_allocator.deinit();
                const db = try file.readToEndAlloc(temp_allocator.allocator(), RDB_FILE_SIZE_LIMIT);
                const data, _ = try rdb.parseData(db, temp_allocator.allocator());
                for (data) |pair| {
                    if (pair.value.expire_at_ms) |expire_at_ms| {
                        if (expire_at_ms <= std.time.milliTimestamp()) {
                            std.debug.print("Key {s} already expired at {d}, not adding it\n", .{ pair.key, expire_at_ms });
                            continue;
                        }
                    }
                    var new_datum: Datum = .{ .expire_at_ms = pair.value.expire_at_ms, .value = undefined };
                    new_datum.value = .{ .string = try instance.dupe(pair.value.value.string) };
                    try instance.data.put(try instance.dupe(pair.key), new_datum);
                }
            } else |err| {
                if (err != error.FileNotFound) {
                    @panic("Error opening dbfilename");
                }
                std.debug.print("File {s} not found\n", .{dbfilename});
            }
        }

        if (instance.config.get("master")) |master| {
            var it = std.mem.splitScalar(u8, master, ' ');
            var address = it.next().?;
            // TODO: DNS
            if (std.mem.eql(u8, address, "localhost")) {
                address = "127.0.0.1";
            }
            const port = try std.fmt.parseInt(u16, it.next().?, 10);
            instance.master, instance.replid = try instance.handshake_and_sync(address, port);
        } else {
            instance.replid = try std.fmt.allocPrint(instance.arena_allocator.allocator(), "{x}{x}", .{ instance.rng.random().int(u128), instance.rng.random().int(u32) });
        }

        return instance;
    }

    fn handshake_and_sync(self: *Instance, address: []const u8, port: u16) !struct { posix.socket_t, []u8 } {
        var temp_allocator = std.heap.ArenaAllocator.init(self.arena_allocator.allocator());
        defer temp_allocator.deinit();
        var stream = try std.net.tcpConnectToHost(temp_allocator.allocator(), address, port);
        _ = try stream.write(try resp.encodeMessage(temp_allocator.allocator(), &[_][]const u8{"PING"}));
        const buffer = try temp_allocator.allocator().alloc(u8, 1024);
        var n = try stream.read(buffer);
        std.debug.assert(std.ascii.eqlIgnoreCase(buffer[0..n], "+PONG\r\n"));
        _ = try stream.write(try resp.encodeMessage(temp_allocator.allocator(), &[_][]const u8{ "REPLCONF", "listening-port", self.config.get("listening-port").? }));
        n = try stream.read(buffer);
        std.debug.assert(std.ascii.eqlIgnoreCase(buffer[0..n], "+OK\r\n"));
        _ = try stream.write(try resp.encodeMessage(temp_allocator.allocator(), &[_][]const u8{ "REPLCONF", "capa", "psync2" }));
        n = try stream.read(buffer);
        std.debug.assert(std.ascii.eqlIgnoreCase(buffer[0..n], "+OK\r\n"));
        _ = try stream.write(try resp.encodeMessage(temp_allocator.allocator(), &[_][]const u8{ "PSYNC", "?", "-1" }));

        var timeout = posix.timeval{
            .sec = 0,
            .usec = 100 * 1000,
        };
        try posix.setsockopt(stream.handle, posix.SOL.SOCKET, posix.SO.RCVTIMEO, (@as([*]u8, @ptrCast(&timeout)))[0..@sizeOf(posix.timeval)]);

        n = 0;
        while (stream.read(buffer[n..])) |read_bytes| : (n += read_bytes) {
            if (read_bytes == 0) {
                break;
            }
        } else |err| {
            if (err != error.WouldBlock) {
                return err;
            }
        }

        timeout.usec = 0;

        try posix.setsockopt(stream.handle, posix.SOL.SOCKET, posix.SO.RCVTIMEO, (@as([*]u8, @ptrCast(&timeout)))[0..@sizeOf(posix.timeval)]);

        const fullsync, const fullsync_parsed_bytes = try resp.parseSimpleString(temp_allocator.allocator(), buffer); // FULLSYNC
        var fullsync_it = std.mem.splitScalar(u8, fullsync, ' ');
        _ = fullsync_it.next();
        const replid = try self.dupe(fullsync_it.next().?);
        _, const db_parsed_bytes = try resp.parseBulkString(temp_allocator.allocator(), buffer[fullsync_parsed_bytes..], false);
        var parsed_bytes = fullsync_parsed_bytes + db_parsed_bytes;
        while (n > parsed_bytes) {
            const request, const command_bytes = try resp.parseArray(temp_allocator.allocator(), buffer[parsed_bytes..]);
            const reply, _ = try self.executeCommand(temp_allocator.allocator(), request);
            if (std.ascii.eqlIgnoreCase(request[0], "REPLCONF") and
                std.ascii.eqlIgnoreCase(request[1], "GETACK"))
            {
                _ = try stream.write(reply);
            }
            parsed_bytes += command_bytes;
        }

        std.debug.print("Handshake and sync with {s}:{d} was succesful!\n", .{ address, port });
        return .{ stream.handle, replid };
    }

    pub fn destroy(self: *Instance, allocator: std.mem.Allocator) void {
        self.arena_allocator.deinit();
        allocator.destroy(self.arena_allocator);
    }

    pub fn executeCommand(self: *Instance, allocator: std.mem.Allocator, command: []const []const u8) !struct { []u8, bool } {
        if (std.ascii.eqlIgnoreCase(command[0], "PING")) {
            return .{ try resp.encodeSimpleString(allocator, "PONG"[0..]), false };
        } else if (std.ascii.eqlIgnoreCase(command[0], "ECHO")) {
            return .{ try resp.encodeSimpleString(allocator, command[1]), false };
        } else if (std.ascii.eqlIgnoreCase(command[0], "SET")) {
            try self.data.put(try self.dupe(command[1]), .{ .expire_at_ms = if (command.len > 3) blk: {
                if (std.ascii.eqlIgnoreCase(command[3], "PX")) {
                    break :blk std.time.milliTimestamp() + @as(i64, @bitCast(try std.fmt.parseInt(u64, command[4], 10)));
                } else if (std.ascii.eqlIgnoreCase(command[3], "PXAT")) {
                    break :blk @as(i64, @bitCast(try std.fmt.parseInt(u64, command[4], 10)));
                } else if (std.ascii.eqlIgnoreCase(command[3], "EX")) {
                    break :blk std.time.milliTimestamp() + @as(i64, @bitCast(try std.fmt.parseInt(u64, command[4], 10))) * 1000;
                } else if (std.ascii.eqlIgnoreCase(command[3], "EXAT")) {
                    break :blk @as(i64, @bitCast(try std.fmt.parseInt(u64, command[4], 10))) * 1000;
                } else break :blk null;
            } else null, .value = .{ .string = try self.dupe(command[2]) } });
            return .{ try resp.encodeSimpleString(allocator, "OK"[0..]), true };
        } else if (std.ascii.eqlIgnoreCase(command[0], "GET")) {
            if (self.data.get(command[1])) |data| {
                if (data.expire_at_ms) |expire_at_ms| {
                    if (expire_at_ms <= std.time.milliTimestamp()) {
                        _ = self.data.remove(command[1]);
                        return .{ try resp.encodeBulkString(allocator, null), false };
                    }
                }
                return .{ try resp.encodeBulkString(allocator, data.value.string), false };
            } else return .{ try resp.encodeBulkString(allocator, null), false };
        } else if (std.ascii.eqlIgnoreCase(command[0], "KEYS")) {
            // TODO: support patterns
            if (command[1][0] == '*') {
                var keys = try allocator.alloc([]const u8, self.data.count());

                var keys_iterator = self.data.keyIterator();
                var i: usize = 0;
                while (keys_iterator.next()) |key| : (i += 1) {
                    keys[i] = key.*;
                }
                return .{ try resp.encodeMessage(allocator, keys), false };
            } else return .{ try resp.encodeBulkString(allocator, null), false };
        } else if (std.ascii.eqlIgnoreCase(command[0], "XADD")) {
            var request_entry_id = try parseStreamEntryId(command[2]);
            // TODO: proper error handling
            if (request_entry_id == 0)
                return .{ try resp.encodeSimpleError(allocator, "The ID specified in XADD must be greater than 0-0"), false };

            const should_generate_timestamp = request_entry_id == ~@as(u64, 0);
            const should_generate_sequence_number = request_entry_id & 0xffff == 0xffff;

            if (should_generate_timestamp)
                request_entry_id = @as(u64, @bitCast(std.time.milliTimestamp())) << 16;

            if (should_generate_sequence_number)
                request_entry_id = (request_entry_id & ~@as(u64, 0xffff)) | (@as(u16, 1) - std.math.clamp(@as(u16, @truncate(request_entry_id >> 16)), 0, 1));

            const stream_datum = try self.data.getOrPut(try self.dupe(command[1]));
            if (!stream_datum.found_existing) {
                stream_datum.value_ptr.*.value.stream = Stream.init(self.arena_allocator.allocator());
            }

            var stream = &stream_datum.value_ptr.value.stream;
            const stream_keys = stream.keys();

            if (stream_keys.len > 0) {
                const latest_entry_id = stream_keys[stream_keys.len - 1];

                if (should_generate_sequence_number and latest_entry_id >> 16 == request_entry_id >> 16) {
                    request_entry_id = latest_entry_id + 1;
                }

                if (request_entry_id <= latest_entry_id) {
                    return .{ try resp.encodeSimpleError(allocator, "The ID specified in XADD is equal or smaller than the target stream top item"), false };
                }
            }

            var stream_entry = try stream.getOrPut(request_entry_id);

            if (!stream_entry.found_existing) {
                stream_entry.value_ptr.* = std.StringHashMap([]u8).init(self.arena_allocator.allocator());
            }

            var i: usize = 3;

            while (i < command.len) : (i += 2) {
                try stream_entry.value_ptr.put(try self.dupe(command[i]), try self.dupe(command[i + 1]));
            }

            return .{ try resp.encodeBulkString(allocator, try std.fmt.allocPrint(allocator, "{d}-{d}", .{ request_entry_id >> 16, request_entry_id & 0xffff })), true };
        } else if (std.ascii.eqlIgnoreCase(command[0], "XRANGE")) {
            if (self.data.get(command[1])) |datum| {
                switch (datum.value) {
                    .stream => |stream| {
                        const stream_keys = stream.keys();

                        var start_index: usize = 0;
                        if (!std.mem.eql(u8, command[2], "-")) {
                            const request_entry_id = try parseStreamEntryId(command[2]);
                            while (request_entry_id > stream_keys[start_index] and start_index < stream_keys.len) : (start_index += 1) {}
                        }

                        var end_index = stream_keys.len - 1;

                        if (!std.mem.eql(u8, command[3], "+")) {
                            const request_entry_id = try parseStreamEntryId(command[3]);
                            while (request_entry_id < stream_keys[end_index] and end_index > 0) : (end_index -= 1) {}
                        }

                        if (end_index < start_index) {
                            return .{ try resp.encodeSimpleError(allocator, "Invalid range"), false };
                        }

                        var temp_allocator = std.heap.ArenaAllocator.init(self.arena_allocator.allocator());
                        defer temp_allocator.deinit();
                        var response = std.ArrayList(resp.Value).init(temp_allocator.allocator());

                        for (start_index..end_index + 1) |index| {
                            var entry_entries_it = stream.get(stream_keys[index]).?.iterator();
                            var entry_elements = std.ArrayList(resp.Value).init(temp_allocator.allocator());
                            while (entry_entries_it.next()) |keyval_pair| {
                                try entry_elements.append(resp.BulkString(keyval_pair.key_ptr.*));
                                try entry_elements.append(resp.BulkString(keyval_pair.value_ptr.*));
                            }

                            var tmp_array = try temp_allocator.allocator().alloc(resp.Value, 2);
                            tmp_array[0] = resp.BulkString(try std.fmt.allocPrint(temp_allocator.allocator(), "{d}-{d}", .{ stream_keys[index] >> 16, stream_keys[index] & 0xffff }));
                            tmp_array[1] = resp.Array(try entry_elements.toOwnedSlice());
                            try response.append(resp.Array(tmp_array));
                        }

                        return .{ try resp.encodeArray(allocator, try response.toOwnedSlice()), false };
                    },
                    else => {
                        return .{ try resp.encodeBulkString(allocator, null), false };
                    },
                }
            } else return .{ try resp.encodeBulkString(allocator, null), false };
        } else if (std.ascii.eqlIgnoreCase(command[0], "XREAD")) {
            var i: usize = 2;
            while ((parseStreamEntryId(command[i]) catch ~@as(u64, 0)) == ~@as(u64, 0)) : (i += 1) {}

            const offset = i - 2;
            i = 2;

            var temp_allocator = std.heap.ArenaAllocator.init(self.arena_allocator.allocator());
            defer temp_allocator.deinit();

            var response = std.ArrayList(resp.Value).init(temp_allocator.allocator());
            while (i + offset < command.len) : (i += 1) {
                if (self.data.get(command[i])) |datum| {
                    switch (datum.value) {
                        .stream => |stream| {
                            var new_response_entry = std.ArrayList(resp.Value).init(temp_allocator.allocator());
                            try new_response_entry.append(resp.BulkString(command[i]));

                            var stream_entries = std.ArrayList(resp.Value).init(temp_allocator.allocator());

                            const stream_keys = stream.keys();

                            const request_entry_id = try parseStreamEntryId(command[i + offset]);
                            var start_index: usize = 0;
                            while (request_entry_id >= stream_keys[start_index] and start_index < stream_keys.len) : (start_index += 1) {}

                            for (start_index..stream_keys.len) |index| {
                                var entry_entries_it = stream.get(stream_keys[index]).?.iterator();
                                var entry_elements = std.ArrayList(resp.Value).init(temp_allocator.allocator());
                                while (entry_entries_it.next()) |keyval_pair| {
                                    try entry_elements.append(resp.BulkString(keyval_pair.key_ptr.*));
                                    try entry_elements.append(resp.BulkString(keyval_pair.value_ptr.*));
                                }

                                var tmp_array = try temp_allocator.allocator().alloc(resp.Value, 2);
                                tmp_array[0] = resp.BulkString(try std.fmt.allocPrint(temp_allocator.allocator(), "{d}-{d}", .{ stream_keys[index] >> 16, stream_keys[index] & 0xffff }));
                                tmp_array[1] = resp.Array(try entry_elements.toOwnedSlice());
                                try stream_entries.append(resp.Array(tmp_array));
                            }
                            try new_response_entry.append(resp.Array(try stream_entries.toOwnedSlice()));
                            try response.append(resp.Array(try new_response_entry.toOwnedSlice()));
                        },
                        else => {
                            return .{ try resp.encodeBulkString(allocator, null), false };
                        },
                    }
                } else return .{ try resp.encodeBulkString(allocator, null), false };
            }
            return .{ try resp.encodeArray(allocator, try response.toOwnedSlice()), false };
        } else if (std.ascii.eqlIgnoreCase(command[0], "CONFIG")) {
            if (std.ascii.eqlIgnoreCase(command[1], "GET")) {
                if (self.config.get(command[2])) |data| {
                    return .{ try resp.encodeMessage(allocator, &[_][]const u8{ command[2], data }), false };
                } else return .{ try resp.encodeBulkString(allocator, null), false };
            } else if (std.ascii.eqlIgnoreCase(command[1], "SET")) {
                try self.config.put(try self.dupe(command[2]), try self.dupe(command[3]));
                return .{ try resp.encodeSimpleString(allocator, "OK"[0..]), false };
            } else return error.InvalidConfigCommand;
        } else if (std.ascii.eqlIgnoreCase(command[0], "INFO")) {
            if (std.ascii.eqlIgnoreCase(command[1], "REPLICATION")) {
                var response = std.ArrayList(u8).init(allocator);
                try response.appendSlice(if (self.master) |_| "role:slave\n" else "role:master\n");
                try std.fmt.format(response.writer(), "master_replid:{s}\n", .{self.replid});
                try std.fmt.format(response.writer(), "master_repl_offset:{d}\n", .{self.repl_offset});
                return .{ try resp.encodeBulkString(allocator, try response.toOwnedSlice()), false };
            } else return error.InvalidInfoCommand;
        } else if (std.ascii.eqlIgnoreCase(command[0], "REPLCONF")) {
            if (std.ascii.eqlIgnoreCase(command[1], "GETACK")) {
                return .{ try resp.encodeMessage(allocator, &[_][]const u8{ "REPLCONF", "ACK", try std.fmt.allocPrint(allocator, "{d}", .{self.repl_offset}) }), false };
            } else return .{ try resp.encodeSimpleString(allocator, "OK"[0..]), false };
        } else if (std.ascii.eqlIgnoreCase(command[0], "PSYNC")) {
            var response = std.ArrayList(u8).init(allocator);
            try std.fmt.format(response.writer(), "FULLRESYNC {s} {d}", .{ self.replid, self.repl_offset });
            return .{ try resp.encodeSimpleString(allocator, try response.toOwnedSlice()), false };
        } else if (std.ascii.eqlIgnoreCase(command[0], "WAIT")) {
            return .{ try resp.encodeInteger(allocator, self.n_slaves), false };
        } else if (std.ascii.eqlIgnoreCase(command[0], "TYPE")) {
            if (self.data.get(command[1])) |data| {
                switch (data.value) {
                    .string => {
                        return .{ try resp.encodeSimpleString(allocator, "string"), false };
                    },
                    .stream => {
                        return .{ try resp.encodeSimpleString(allocator, "stream"), false };
                    },
                }
            } else return .{ try resp.encodeSimpleString(allocator, "none"), false };
        } else {
            std.debug.print("Unsupported command received: {s}\n", .{command[0]});
            return error.UnsupportedCommand;
        }
    }

    pub fn dumpToBuffer(_: *Instance, buffer: []u8) !usize {
        // TODO: Actually turn the in-memory representation to a RDB dump
        const empty_file = [_]u8{
            0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, //  |REDIS0011..redis|
            0x65, 0x64, 0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, //  |-ver.7.2.0..redi|
            0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, //  |s-bits.@..ctime.|
            0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2, //  |m..e..used-mem..|
            0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, //  |.....aof-base...|
            0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, //  |.n;...Z.|
            0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe,
            0xc0, 0xff, 0x5a, 0xa2,
        };
        @memcpy(buffer[0..empty_file.len], &empty_file);
        return empty_file.len;
    }
};
