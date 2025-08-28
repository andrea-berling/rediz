const std = @import("std");
const posix = std.posix;
const resp = @import("resp.zig");
const rdb = @import("rdb.zig");
const Command = @import("command.zig").Command;
const cfg = @import("config.zig");
const sset = @import("sorted_set.zig");

const RDB_FILE_SIZE_LIMIT = 100 * 1024 * 1024 * 1024;

const logger = std.log.scoped(.database);

pub const StreamEntryID = packed struct(u64) {
    pub const TIMESTAMP_ANY: u48 = ~@as(u48, 0);
    pub const SEQUENCE_NUMBER_ANY: u48 = 0xffff;

    timestamp: u48 = TIMESTAMP_ANY,
    sequence_number: u16 = SEQUENCE_NUMBER_ANY,

    const Self = @This();

    pub fn isZero(self: Self) bool {
        return self.timestamp == 0 and self.sequence_number == 0;
    }

    pub fn isLessThanOrEqual(self: Self, other: Self) bool {
        return self.timestamp < other.timestamp or (self.timestamp == other.timestamp and self.sequence_number <= other.sequence_number);
    }

    pub fn format(value: @This(), comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;
        try std.fmt.format(writer, "{d}-{d}", .{ value.timestamp, value.sequence_number });
    }
};

const Stream = std.AutoArrayHashMap(StreamEntryID, std.StringHashMap([]u8));

pub const Value = union(enum) {
    string: []u8,
    stream: Stream,
    list: std.DoublyLinkedList([]u8),
    sorted_set: sset.SortedSet,
};

pub const Datum = struct {
    value: Value,
    expire_at_ms: ?i64 = null,
};

pub const Instance = struct {
    arena_allocator: *std.heap.ArenaAllocator,
    data: std.StringHashMap(Datum),
    config: std.StringHashMap([]u8),
    master: ?posix.socket_t = null,
    replid: []const u8,
    /// Number of commands bytes processed so far
    repl_offset: usize,
    rng: std.Random.DefaultPrng,
    diewithmaster: bool,

    inline fn dupe(self: *Instance, bytes: []const u8) ![]u8 {
        return try self.arena_allocator.allocator().dupe(u8, bytes);
    }

    pub fn init(allocator: std.mem.Allocator, init_config: ?std.StringArrayHashMap([]const u8)) !Instance {
        var instance: Instance = undefined;
        instance.arena_allocator = try allocator.create(std.heap.ArenaAllocator);
        instance.arena_allocator.* = std.heap.ArenaAllocator.init(allocator);
        instance.data = std.StringHashMap(Datum).init(instance.arena_allocator.allocator());
        instance.config = std.StringHashMap([]u8).init(instance.arena_allocator.allocator());
        instance.master = null;
        instance.repl_offset = 0;
        instance.rng = std.Random.DefaultPrng.init(@bitCast(std.time.microTimestamp()));
        instance.diewithmaster = false;

        errdefer instance.destroy(allocator);

        const config_defaults = [_]struct { name: []const u8, value: []const u8 }{.{ .name = "dbfilename", .value = "dump.rdb" }};

        for (config_defaults) |config_pair| {
            try instance.config.put(
                try instance.dupe(config_pair.name),
                try instance.dupe(config_pair.value),
            );
        }

        if (init_config) |config| {
            var entry_it = config.iterator();
            while (entry_it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, "diewithmaster")) {
                    instance.diewithmaster = true;
                    continue;
                }
                try instance.config.put(
                    try instance.dupe(entry.key_ptr.*),
                    try instance.dupe(entry.value_ptr.*),
                );
            }
        }

        var rdb_directory = std.fs.cwd();

        if (instance.config.get("dir")) |dir| {
            rdb_directory = try std.fs.openDirAbsolute(dir, .{});
        }

        if (instance.config.get("dbfilename")) |dbfilename| {
            logger.info("Initializing from RDB file {s}...", .{dbfilename});
            if (rdb_directory.openFile(dbfilename, .{})) |file| {
                var temp_allocator = std.heap.ArenaAllocator.init(instance.arena_allocator.allocator());
                defer temp_allocator.deinit();
                const db = try file.readToEndAlloc(temp_allocator.allocator(), RDB_FILE_SIZE_LIMIT);
                const data, _ = try rdb.parseData(db, temp_allocator.allocator());
                for (data) |pair| {
                    if (pair.value.expire_at_ms) |expire_at_ms| {
                        if (expire_at_ms <= std.time.milliTimestamp()) {
                            logger.debug("Key {s} already expired at {d}, not adding it", .{ pair.key, expire_at_ms });
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
                logger.warn("File {s} not found", .{dbfilename});
            }
        }

        if (instance.config.get("master")) |master| {
            var it = std.mem.splitScalar(u8, master, ' ');
            var address = it.next().?;
            if (std.mem.eql(u8, address, "localhost")) {
                address = "127.0.0.1";
            }
            const port = try std.fmt.parseInt(u16, it.next().?, 10);
            instance.master, instance.replid = instance.handshakeAndSync(address, port) catch |err| {
                logger.err("Handshake failed: {!}", .{err});
                return error.HandshakeWithMasterFailed;
            };
        } else {
            instance.replid = try std.fmt.allocPrint(instance.arena_allocator.allocator(), "{x}{x}", .{ instance.rng.random().int(u128), instance.rng.random().int(u32) });
        }

        return instance;
    }

    fn handshakeAndSync(self: *Instance, address: []const u8, port: u16) !struct { posix.socket_t, []u8 } {
        var temp_allocator = std.heap.ArenaAllocator.init(self.arena_allocator.allocator());
        defer temp_allocator.deinit();
        var tcp_stream = std.net.tcpConnectToHost(temp_allocator.allocator(), address, port) catch |err| {
            logger.err("Error connecting to {s}:{d}: {!}", .{ address, port, err });
            return err;
        };
        _ = try tcp_stream.write(try resp.Array(&[_]resp.Value{resp.BulkString("PING")}).encode(temp_allocator.allocator()));
        const buffer = try temp_allocator.allocator().alloc(u8, 1024);
        var n = try tcp_stream.read(buffer);
        var response, _ = resp.Value.parse(buffer, temp_allocator.allocator()) catch |err| {
            logger.err("Handshake 1: Invalid response from master", .{});
            logger.debug("Data returned by master: {x}", .{buffer[0..n]});
            return err;
        };
        std.debug.assert(response == .simple_string and std.ascii.eqlIgnoreCase(response.simple_string, "PONG"));
        _ = try tcp_stream.write(try resp.Array(&[_]resp.Value{ resp.BulkString("REPLCONF"), resp.BulkString("listening-port"), resp.BulkString(self.config.get("listening-port").?) }).encode(temp_allocator.allocator()));
        n = try tcp_stream.read(buffer);
        response, _ = resp.Value.parse(buffer, temp_allocator.allocator()) catch |err| {
            logger.err("Handshake 2: Invalid response from master", .{});
            logger.debug("Data returned by master: {x}", .{buffer[0..n]});
            return err;
        };
        std.debug.assert(response == .simple_string and std.ascii.eqlIgnoreCase(response.simple_string, "OK"));
        _ = try tcp_stream.write(try resp.Array(&[_]resp.Value{ resp.BulkString("REPLCONF"), resp.BulkString("capa"), resp.BulkString("psync2") }).encode(temp_allocator.allocator()));
        n = try tcp_stream.read(buffer);
        response, _ = resp.Value.parse(buffer, temp_allocator.allocator()) catch |err| {
            logger.err("Handshake 3: Invalid response from master", .{});
            logger.debug("Data returned by master: {x}", .{buffer[0..n]});
            return err;
        };
        std.debug.assert(response == .simple_string and std.ascii.eqlIgnoreCase(response.simple_string, "OK"));
        _ = try tcp_stream.write(try resp.Array(&[_]resp.Value{ resp.BulkString("PSYNC"), resp.BulkString("?"), resp.BulkString("-1") }).encode(temp_allocator.allocator()));

        var timeout = posix.timeval{
            .sec = 0,
            .usec = 100 * 1000,
        };
        try posix.setsockopt(tcp_stream.handle, posix.SOL.SOCKET, posix.SO.RCVTIMEO, (@as([*]u8, @ptrCast(&timeout)))[0..@sizeOf(posix.timeval)]);

        n = 0;
        while (tcp_stream.read(buffer[n..])) |read_bytes| : (n += read_bytes) {
            if (read_bytes == 0) {
                break;
            }
        } else |err| {
            if (err != error.WouldBlock) {
                return err;
            }
        }

        timeout.usec = 0;

        try posix.setsockopt(tcp_stream.handle, posix.SOL.SOCKET, posix.SO.RCVTIMEO, (@as([*]u8, @ptrCast(&timeout)))[0..@sizeOf(posix.timeval)]);

        const fullsync, const fullsync_parsed_bytes = try resp.Value.parse(buffer, temp_allocator.allocator()); // FULLSYNC
        var fullsync_it = std.mem.splitScalar(u8, fullsync.simple_string, ' ');
        _ = fullsync_it.next();
        const replid = try self.dupe(fullsync_it.next() orelse {
            logger.err("Handshake 4: Invalid response from master", .{});
            logger.debug("Data returned by master: {x}", .{buffer[0..fullsync_parsed_bytes]});
            return error.InvalidResponseFromMaster;
        });
        const db_parsed_bytes = rdb.parseDump(buffer[fullsync_parsed_bytes..]) catch |err| {
            logger.err("Handshake 5: Received invalid dump from master", .{});
            logger.debug("Data returned by master: {x}", .{buffer[fullsync_parsed_bytes..]});
            return err;
        };
        var parsed_bytes = fullsync_parsed_bytes + db_parsed_bytes;
        while (n > parsed_bytes) {
            const command, const command_bytes = try Command.parse(buffer[parsed_bytes..], temp_allocator.allocator());
            const reply = try self.executeCommand(temp_allocator.allocator(), &command);
            if (command.type == .replconf and command.type.replconf == .getack) {
                _ = try tcp_stream.write(try reply.encode(temp_allocator.allocator()));
            }
            parsed_bytes += command_bytes;
        }

        logger.info("Handshake and sync with {s}:{d} was succesful!", .{ address, port });
        return .{ tcp_stream.handle, replid };
    }

    pub fn destroy(self: *Instance, allocator: std.mem.Allocator) void {
        self.arena_allocator.deinit();
        allocator.destroy(self.arena_allocator);
    }

    /// Arrays returned are dynamically allocated and should be freed by the
    /// called, strings should not, as they point to the given command
    pub fn executeCommand(self: *Instance, allocator: std.mem.Allocator, command: *const Command) !resp.Value {
        switch (command.type) {
            .ping => return resp.SimpleString("PONG"),
            .echo => |string| return resp.SimpleString(string),
            .get => |key| {
                if (self.data.get(key)) |data| {
                    if (data.expire_at_ms) |expire_at_ms| {
                        if (expire_at_ms <= std.time.milliTimestamp()) {
                            _ = self.data.remove(key);
                            return resp.Null;
                        }
                    }
                    return resp.BulkString(data.value.string);
                } else return resp.Null;
            },
            .set => |set_command| {
                var datum: Datum = .{ .value = .{ .string = try self.dupe(set_command.value) } };
                if (set_command.expire_at_ms) |expiration| {
                    datum.expire_at_ms = expiration;
                }
                try self.data.put(try self.dupe(set_command.key), datum);
                return resp.Ok;
            },
            .keys => {
                var keys = try allocator.alloc(resp.Value, self.data.count());

                var keys_iterator = self.data.keyIterator();
                var i: usize = 0;
                while (keys_iterator.next()) |key| : (i += 1) {
                    keys[i] = resp.BulkString(key.*);
                }
                return resp.Array(keys);
            },
            .xadd => |stream_add_command| {
                var request_entry_id = stream_add_command.entry_id;
                if (request_entry_id.isZero())
                    return resp.SimpleError("The ID specified in XADD must be greater than 0-0");

                const should_generate_sequence_number = request_entry_id.sequence_number == StreamEntryID.SEQUENCE_NUMBER_ANY;
                const should_generate_timestamp = should_generate_sequence_number and request_entry_id.timestamp == StreamEntryID.TIMESTAMP_ANY;

                if (should_generate_timestamp)
                    request_entry_id.timestamp = @as(u48, @truncate(@as(u64, @bitCast(std.time.milliTimestamp()))));

                if (should_generate_sequence_number)
                    request_entry_id.sequence_number = if (request_entry_id.timestamp == 0) 1 else 0;

                const stream_datum = try self.data.getOrPut(try self.dupe(stream_add_command.stream_key));
                if (!stream_datum.found_existing) {
                    stream_datum.value_ptr.*.value = .{ .stream = Stream.init(self.arena_allocator.allocator()) };
                }

                var stream = &stream_datum.value_ptr.value.stream;
                const stream_keys = stream.keys();

                if (stream_keys.len > 0) {
                    const latest_entry_id = stream_keys[stream_keys.len - 1];

                    if (should_generate_sequence_number and latest_entry_id.timestamp == request_entry_id.timestamp) {
                        request_entry_id.sequence_number = latest_entry_id.sequence_number + 1;
                    }

                    if (request_entry_id.isLessThanOrEqual(latest_entry_id)) {
                        return resp.SimpleError("The ID specified in XADD is equal or smaller than the target stream top item");
                    }
                }

                var stream_entry = try stream.getOrPut(request_entry_id);

                if (!stream_entry.found_existing) {
                    stream_entry.value_ptr.* = std.StringHashMap([]u8).init(self.arena_allocator.allocator());
                }

                for (stream_add_command.key_value_pairs) |pair|
                    try stream_entry.value_ptr.put(try self.dupe(pair.key), try self.dupe(pair.value));

                return resp.BulkString(try std.fmt.allocPrint(allocator, "{}", .{request_entry_id}));
            },
            .xrange => |stream_range_command| {
                if (self.data.get(stream_range_command.stream_key)) |datum| {
                    switch (datum.value) {
                        .stream => |stream| {
                            const stream_keys = stream.keys();

                            const start_index: usize = blk: switch (stream_range_command.start_entry_id) {
                                .minus => {
                                    break :blk 0;
                                },
                                .entry_id => |entry_id| {
                                    var i: usize = 0;
                                    // TODO: bisect
                                    while (!entry_id.isLessThanOrEqual(stream_keys[i]) and i < stream_keys.len) : (i += 1) {}
                                    break :blk i;
                                },
                                else => {
                                    return resp.SimpleError("Invalid start ID");
                                },
                            };

                            const end_index: usize = blk: switch (stream_range_command.end_entry_id) {
                                .plus => {
                                    break :blk stream_keys.len - 1;
                                },
                                .entry_id => |entry_id| {
                                    var i: usize = stream_keys.len - 1;
                                    while (entry_id.isLessThanOrEqual(stream_keys[i]) and entry_id != stream_keys[i] and i > 0) : (i -= 1) {}
                                    break :blk i;
                                },
                                else => {
                                    return resp.SimpleError("Invalid end ID");
                                },
                            };

                            if (end_index < start_index) {
                                return resp.SimpleError("Invalid range");
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
                                tmp_array[0] = resp.BulkString(try std.fmt.allocPrint(temp_allocator.allocator(), "{}", .{stream_keys[index]}));
                                tmp_array[1] = resp.Array(try entry_elements.toOwnedSlice());
                                try response.append(resp.Array(tmp_array));
                            }

                            return resp.Array(try response.toOwnedSlice());
                        },
                        else => {
                            return resp.Null;
                        },
                    }
                } else return resp.Null;
            },
            .xread => |stream_read_request| {
                var temp_allocator = std.heap.ArenaAllocator.init(self.arena_allocator.allocator());
                defer temp_allocator.deinit();

                var response = std.ArrayList(resp.Value).init(temp_allocator.allocator());
                var available_data = false;
                for (stream_read_request.requests) |request| {
                    if (request.start_entry_id == .new_data) {
                        return resp.Null;
                    }
                    if (self.data.get(request.stream_key)) |datum| {
                        switch (datum.value) {
                            .stream => |stream| {
                                var new_response_entry = std.ArrayList(resp.Value).init(temp_allocator.allocator());
                                try new_response_entry.append(resp.BulkString(request.stream_key));

                                var stream_entries = std.ArrayList(resp.Value).init(temp_allocator.allocator());

                                const stream_keys = stream.keys();

                                const request_entry_id = request.start_entry_id.entry_id;
                                var start_index: usize = 0;
                                while (start_index < stream_keys.len and (!request_entry_id.isLessThanOrEqual(stream_keys[start_index]) or request_entry_id == stream_keys[start_index])) : (start_index += 1) {}

                                for (start_index..stream_keys.len) |index| {
                                    available_data = true;
                                    var entry_entries_it = stream.get(stream_keys[index]).?.iterator();
                                    var entry_elements = std.ArrayList(resp.Value).init(temp_allocator.allocator());
                                    while (entry_entries_it.next()) |keyval_pair| {
                                        try entry_elements.append(resp.BulkString(keyval_pair.key_ptr.*));
                                        try entry_elements.append(resp.BulkString(keyval_pair.value_ptr.*));
                                    }

                                    var tmp_array = try temp_allocator.allocator().alloc(resp.Value, 2);
                                    tmp_array[0] = resp.BulkString(try std.fmt.allocPrint(temp_allocator.allocator(), "{}", .{stream_keys[index]}));
                                    tmp_array[1] = resp.Array(try entry_elements.toOwnedSlice());
                                    try stream_entries.append(resp.Array(tmp_array));
                                }
                                try new_response_entry.append(resp.Array(try stream_entries.toOwnedSlice()));
                                try response.append(resp.Array(try new_response_entry.toOwnedSlice()));
                            },
                            else => {
                                return resp.SimpleError(
                                    try std.fmt.allocPrint(temp_allocator.allocator(), "{s} does not denote a stream", .{request.stream_key}),
                                );
                            },
                        }
                    } else return resp.Null;
                }
                if (available_data) {
                    return resp.Array(try response.toOwnedSlice());
                } else {
                    return resp.Null;
                }
            },
            .config => |config_command| {
                switch (config_command) {
                    .get => |option| {
                        if (self.config.get(option)) |data| {
                            return resp.Array(try allocator.dupe(resp.Value, &[_]resp.Value{ resp.BulkString(option), resp.BulkString(data) }));
                        } else return resp.Null;
                    },
                    .set => |set_command| {
                        try self.config.put(try self.dupe(set_command.option), try self.dupe(set_command.value));
                        return resp.Ok;
                    },
                }
            },
            .info => |info_command| {
                switch (info_command) {
                    .replication => {
                        var temp_allocator = std.heap.ArenaAllocator.init(self.arena_allocator.allocator());
                        defer temp_allocator.deinit();

                        var response = std.ArrayList(u8).init(temp_allocator.allocator());
                        try response.appendSlice(if (self.master) |_| "role:slave\n" else "role:master\n");
                        try std.fmt.format(response.writer(), "master_replid:{s}\n", .{self.replid});
                        try std.fmt.format(response.writer(), "master_repl_offset:{d}\n", .{if (self.master) |_| self.repl_offset else 0});
                        return resp.BulkString(try response.toOwnedSlice());
                    },
                }
            },
            .replconf => |replica_config_command| {
                switch (replica_config_command) {
                    .getack => {
                        return resp.Array(try allocator.dupe(resp.Value, &[_]resp.Value{ resp.BulkString("REPLCONF"), resp.BulkString("ACK"), resp.BulkString(try std.fmt.allocPrint(allocator, "{d}", .{self.repl_offset})) }));
                    },
                    .capabilities => |capabilities| {
                        var temp_allocator = std.heap.ArenaAllocator.init(self.arena_allocator.allocator());
                        defer temp_allocator.deinit();
                        logger.debug("Slave advertised the following capabilities: {s}", .{try std.mem.join(temp_allocator.allocator(), ",", capabilities)});
                        return resp.Ok;
                    },
                    .listening_port => |listening_port| {
                        logger.debug("Slave advertised this listening port: {d}", .{listening_port});
                        return resp.Ok;
                    },
                }
            },
            .type => |key| {
                if (self.data.get(key)) |data| {
                    switch (data.value) {
                        .string => {
                            return resp.SimpleString("string");
                        },
                        .stream => {
                            return resp.SimpleString("stream");
                        },
                        .list => {
                            return resp.SimpleString("list");
                        },
                        .sorted_set => {
                            return resp.SimpleString("sorted set");
                        },
                    }
                } else return resp.SimpleString("none");
            },
            .incr => |key| {
                if (self.data.getEntry(key)) |data| {
                    switch (data.value_ptr.value) {
                        .string => |s| {
                            const n: i64 = std.fmt.parseInt(i64, s, 10) catch {
                                return resp.SimpleError("value is not an integer or out of range");
                            };
                            self.arena_allocator.allocator().free(data.value_ptr.value.string);
                            data.value_ptr.value = .{ .string = try std.fmt.allocPrint(self.arena_allocator.allocator(), "{}", .{n + 1}) };
                            return resp.Integer(n + 1);
                        },
                        else => return resp.SimpleError("value is not an integer or out of range"),
                    }
                } else {
                    const datum: Datum = .{ .value = .{ .string = try self.dupe("1") } };
                    try self.data.put(try self.dupe(key), datum);
                    return resp.Integer(1);
                }
            },
            .list_push => |push_command| {
                const list_entry = try self.data.getOrPut(try self.dupe(push_command.key));

                if (!list_entry.found_existing) {
                    list_entry.value_ptr.*.value = .{ .list = std.DoublyLinkedList([]u8){} };
                }
                for (push_command.values) |value| {
                    const new_node = try self.arena_allocator.allocator().create(std.DoublyLinkedList([]u8).Node);
                    new_node.data = try self.dupe(value);
                    if (push_command.type == .append) {
                        list_entry.value_ptr.*.value.list.append(new_node);
                    } else list_entry.value_ptr.*.value.list.prepend(new_node);
                }
                return resp.Integer(@intCast(list_entry.value_ptr.*.value.list.len));
            },
            .lrange => |lrange_command| {
                const empty_array = resp.Array(&[_]resp.Value{});
                const datum = self.data.get(lrange_command.key) orelse return empty_array;

                if (datum.value != .list) {
                    return error.InvalidCommand;
                }

                const l = datum.value.list.len;

                const normalized_start = if (lrange_command.start < 0) @max(0, @as(i64, @intCast(l)) + lrange_command.start) else lrange_command.start;
                const normalized_end = if (lrange_command.end < 0) @max(0, @as(i64, @intCast(l)) + lrange_command.end) else lrange_command.end;

                if (normalized_start >= l) return empty_array; // More robust start check

                const inclusive_end = @min(@as(i64, @intCast(l - 1)), normalized_end);

                if (normalized_start > inclusive_end) return empty_array;

                const n_elements: usize = @intCast(inclusive_end - normalized_start + 1);

                if (n_elements == 0) {
                    return empty_array;
                }

                var ret = try allocator.alloc(resp.Value, n_elements);

                var skip = normalized_start;

                var current_node = datum.value.list.first.?;

                while (skip > 0) : (skip -= 1) {
                    current_node = current_node.next orelse return empty_array;
                }

                var i: usize = 0;
                while (i < n_elements) : (i += 1) {
                    ret[i] = resp.BulkString(current_node.data);
                    current_node = current_node.next orelse break;
                }

                return resp.Array(ret);
            },
            .llen => |list| {
                const datum = self.data.get(list) orelse return resp.Integer(0);

                if (datum.value != .list) return error.InvalidArgument;

                return resp.Integer(@intCast(datum.value.list.len));
            },
            .lpop => |lpop_command| {
                const datum = self.data.getEntry(lpop_command.key) orelse return resp.Null;

                if (datum.value_ptr.*.value != .list) return error.InvalidArgument;
                if (datum.value_ptr.*.value.list.len == 0) return resp.Null;
                if (lpop_command.n) |n| {
                    const elements_to_pop = @min(n, datum.value_ptr.*.value.list.len);
                    var ret = try allocator.alloc(resp.Value, elements_to_pop);
                    for (0..elements_to_pop) |i| {
                        const value = datum.value_ptr.*.value.list.popFirst().?;
                        ret[i] = resp.BulkString(try allocator.dupe(u8, value.data));
                        self.arena_allocator.allocator().free(value.data);
                        self.arena_allocator.allocator().destroy(value);
                    }
                    return resp.Array(ret);
                } else {
                    const value = datum.value_ptr.*.value.list.popFirst().?;
                    const ret = resp.BulkString(try allocator.dupe(u8, value.data));
                    self.arena_allocator.allocator().free(value.data);
                    self.arena_allocator.allocator().destroy(value);
                    return ret;
                }
            },
            .zadd => |zadd_command| {
                var temp_allocator = std.heap.ArenaAllocator.init(self.arena_allocator.allocator());
                defer temp_allocator.deinit();

                var entry = try self.data.getOrPut(zadd_command.key);
                if (entry.found_existing and entry.value_ptr.*.value != .sorted_set) {
                    return resp.SimpleError(
                        try std.fmt.allocPrint(
                            temp_allocator.allocator(),
                            "{s} does not denote a sorted set",
                            .{zadd_command.key},
                        ),
                    );
                }

                if (!entry.found_existing) {
                    entry.key_ptr.* = try self.dupe(zadd_command.key);
                    entry.value_ptr.*.value = .{
                        .sorted_set = try sset.SortedSet.init(self.arena_allocator.allocator()),
                    };
                }

                const ret = @intFromBool(!entry.value_ptr.value.sorted_set.contains(zadd_command.name));

                try entry.value_ptr.value.sorted_set.put(
                    try self.dupe(zadd_command.name),
                    zadd_command.score,
                );
                return resp.Integer(ret);
            },
            .zrank => |zrank_command| {
                const datum = self.data.get(zrank_command.key) orelse return resp.Null;
                if (datum.value != .sorted_set) return resp.Null;

                const rank = datum.value.sorted_set.getRankByName(zrank_command.name) orelse return resp.Null;
                return resp.Integer(rank - 1);
            },
            .zrange => |zrange_command| {
                var temp_allocator = std.heap.ArenaAllocator.init(self.arena_allocator.allocator());
                defer temp_allocator.deinit();

                const datum = self.data.get(zrange_command.key) orelse return resp.Array(&[0]resp.Value{});
                if (datum.value != .sorted_set) return resp.Array(&[0]resp.Value{});

                const nodes = try datum.value.sorted_set.getRange(
                    zrange_command.range_start,
                    zrange_command.range_end,
                    temp_allocator.allocator(),
                );

                var ret = try allocator.alloc(resp.Value, nodes.len);
                for (nodes, 0..) |node, i| {
                    ret[i] = resp.BulkString(node.name);
                }

                return resp.Array(ret);
            },
            .zcard => |key| {
                const datum = self.data.get(key) orelse return resp.Null;
                if (datum.value != .sorted_set) return resp.Null;

                return resp.Integer(datum.value.sorted_set.n_items);
            },
            .zscore => |zscore_command| {
                const datum = self.data.get(zscore_command.key) orelse return resp.Null;
                if (datum.value != .sorted_set) return resp.Null;

                const score = datum.value.sorted_set.getScoreByName(zscore_command.name) orelse return resp.Null;
                return resp.BulkString(
                    try std.fmt.allocPrint(allocator, "{d}", .{score}),
                );
            },
            .zrem => |zrem_command| {
                var entry = self.data.getEntry(zrem_command.key) orelse return resp.Null;
                if (entry.value_ptr.value != .sorted_set) return resp.Null;

                const ret = @intFromBool(entry.value_ptr.value.sorted_set.contains(zrem_command.name));

                try entry.value_ptr.value.sorted_set.remove(zrem_command.name);
                return resp.Integer(ret);
            },

            else => {
                return error.InvalidCommand;
            },
        }
    }

    pub fn pop(self: *Instance, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
        const datum = self.data.getEntry(key) orelse return null;
        if (datum.value_ptr.*.value != .list) return error.InvalidArgument;

        if (datum.value_ptr.*.value.list.len == 0) return null;

        const value = datum.value_ptr.*.value.list.popFirst().?;
        defer self.arena_allocator.allocator().destroy(value);
        defer self.arena_allocator.allocator().free(value.data);
        return try allocator.dupe(u8, value.data);
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
