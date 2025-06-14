const std = @import("std");
const db = @import("db.zig");
const resp = @import("resp.zig");

pub const Error = error{ InvalidArgument, InvalidCommand, InvalidInput, InvalidStreamID, MissingArgument, NotEnoughArguments, UnsupportedCommand, WrongNumberOfArguments };

pub const Command = struct {
    bytes: []u8,
    type: union(enum) { ping, echo: []const u8, get: []const u8, set: SetCommand, keys: []const u8, xadd: StreamAddCommand, xrange: StreamRangeCommand, xread: struct { block_timeout_ms: ?usize, requests: []const StreamReadRequest }, config: ConfigCommand, info: InfoCommand, replconf: ReplicaConfigCommand, psync: PsyncConfigCommand, wait: WaitCommand, type: []const u8, incr: []const u8, multi, exec, discard },
    allocator: std.mem.Allocator,

    const Self = @This();

    fn keywordToIndex(string: []const u8) isize {
        const type_info = @typeInfo(@FieldType(@This(), "type"));
        const fields_info = comptime type_info.@"union".fields;
        const other_keywords = [_][]const u8{ "px", "pxat", "ex", "exat", "streams", "replication", "block" };
        var keywords: [fields_info.len + other_keywords.len][]const u8 = undefined;
        @setEvalBranchQuota(2000);
        inline for (fields_info, 0..) |field, i| {
            keywords[i] = field.name;
        }
        inline for (other_keywords, fields_info.len..) |keyword, i| {
            keywords[i] = keyword;
        }
        inline for (keywords, 0..) |keyword, i| {
            if (std.ascii.eqlIgnoreCase(string, keyword))
                return i;
        }
        return -1;
    }

    /// Slices of u8 have the same lifetime as the input bytes, arrays are dynamically allocated
    pub fn parse(bytes: []const u8, allocator: std.mem.Allocator) (Error || std.mem.Allocator.Error)!struct { Command, usize } {
        var temp_allocator = std.heap.ArenaAllocator.init(allocator);
        defer temp_allocator.deinit();

        const value, const parsed_bytes = resp.Value.parse(bytes, temp_allocator.allocator()) catch return Error.InvalidInput;
        if (value != .array) {
            return Error.InvalidInput;
        }

        var command: Command = undefined;
        command.allocator = allocator;
        command.bytes = try allocator.dupe(u8, bytes[0..parsed_bytes]);

        var array = try temp_allocator.allocator().alloc([]const u8, value.array.len);

        for (0..value.array.len) |i| {
            switch (value.array[i]) {
                .null => array[i] = "-1",
                .bulk_string => |string| array[i] = (command.bytes.ptr + (string.ptr - bytes.ptr))[0..string.len],
                else => return Error.InvalidInput,
            }
        }

        const k2idx = keywordToIndex;
        command.type = blk: switch (k2idx(array[0])) {
            k2idx("ping") => {
                break :blk .ping;
            },
            k2idx("echo") => {
                if (array.len == 1)
                    return Error.MissingArgument;
                if (array.len != 2)
                    return Error.WrongNumberOfArguments;
                break :blk .{ .echo = array[1] };
            },
            k2idx("get") => {
                if (array.len == 1)
                    return Error.MissingArgument;
                if (array.len != 2)
                    return Error.WrongNumberOfArguments;
                break :blk .{ .get = array[1] };
            },
            k2idx("set") => {
                if (array.len < 3)
                    return Error.NotEnoughArguments;
                var set_command: SetCommand = .{
                    .key = array[1],
                    .value = array[2],
                };
                if (array.len > 4) {
                    if (array.len != 5)
                        return Error.WrongNumberOfArguments;
                    const arg = @as(i64, @bitCast(std.fmt.parseInt(u64, array[4], 10) catch {
                        return Error.InvalidArgument;
                    }));
                    switch (k2idx(array[3])) {
                        k2idx("px") => {
                            set_command.expire_at_ms = std.time.milliTimestamp() + arg;
                        },
                        k2idx("pxat") => {
                            set_command.expire_at_ms = arg;
                        },
                        k2idx("ex") => {
                            set_command.expire_at_ms = std.time.milliTimestamp() + arg * 1000;
                        },
                        k2idx("exat") => {
                            set_command.expire_at_ms = arg * 1000;
                        },
                        else => {
                            return Error.InvalidCommand;
                        },
                    }
                }

                break :blk .{ .set = set_command };
            },
            k2idx("keys") => {
                if (array.len == 1)
                    return Error.MissingArgument;
                if (array.len != 2)
                    return Error.WrongNumberOfArguments;
                if (!std.mem.eql(u8, array[1], "*")) // only pattern we support for now
                    return Error.InvalidArgument;
                break :blk .{ .keys = array[1] };
            },
            k2idx("xadd") => {
                if (array.len < 5)
                    return Error.NotEnoughArguments;
                if ((array.len - 3) % 2 != 0)
                    return Error.WrongNumberOfArguments;
                var stream_add_command: StreamAddCommand = undefined;
                stream_add_command.stream_key = array[1];
                stream_add_command.entry_id = parseStreamEntryId(array[2]) catch {
                    return Error.InvalidStreamID;
                };
                stream_add_command.key_value_pairs = try allocator.alloc(@TypeOf(stream_add_command.key_value_pairs[0]), (array.len - 3) / 2);
                for (0..(array.len - 3) / 2) |i| {
                    stream_add_command.key_value_pairs[i] = .{ .key = array[2 * i + 3], .value = array[2 * i + 4] };
                }
                break :blk .{ .xadd = stream_add_command };
            },
            k2idx("xrange") => {
                if (array.len != 4)
                    return Error.WrongNumberOfArguments;
                var stream_range_command: StreamRangeCommand = undefined;
                stream_range_command.stream_key = array[1];
                stream_range_command.start_entry_id = if (std.mem.eql(u8, array[2], "-")) .minus else StreamRangeBound{ .entry_id = parseStreamEntryId(array[2]) catch {
                    return Error.InvalidStreamID;
                } };
                stream_range_command.end_entry_id = if (std.mem.eql(u8, array[3], "+")) .plus else StreamRangeBound{ .entry_id = parseStreamEntryId(array[3]) catch {
                    return Error.InvalidStreamID;
                } };
                break :blk .{ .xrange = stream_range_command };
            },
            k2idx("xread") => {
                if (array.len < 4)
                    return Error.NotEnoughArguments;
                var streams_index: usize = 1;
                var block_timeout_ms: ?usize = null;
                if (k2idx(array[1]) == k2idx("block")) {
                    block_timeout_ms = std.fmt.parseInt(usize, array[2], 10) catch {
                        return Error.InvalidArgument;
                    };
                    streams_index += 2;
                }
                if (k2idx(array[streams_index]) != k2idx("streams"))
                    return Error.InvalidCommand;
                const n_args = array.len - streams_index - 1;
                if (n_args % 2 != 0)
                    return Error.WrongNumberOfArguments;

                var stream_read_requests = try allocator.alloc(StreamReadRequest, (n_args) / 2);

                var first_range_index: usize = streams_index + 1;
                // Basically Zig's .is_err()
                // https://ziggit.dev/t/more-syntactically-cleaner-way-to-check-if-a-something-is-an-error-from-an-error-union/4312/3
                while (if (parseStreamEntryId(array[first_range_index])) |_| false else |_| if (std.mem.eql(u8, array[first_range_index], "$")) false else true) : (first_range_index += 1) {}

                const offset = first_range_index - streams_index - 1;

                for (0..(n_args) / 2) |i| {
                    stream_read_requests[i] = StreamReadRequest{
                        .stream_key = array[i + streams_index + 1],
                        .start_entry_id = if (std.mem.eql(u8, array[i + streams_index + 1 + offset], "$")) .new_data else .{ .entry_id = parseStreamEntryId(array[i + streams_index + 1 + offset]) catch {
                            return Error.InvalidStreamID;
                        } },
                    };
                }

                break :blk .{ .xread = .{ .block_timeout_ms = block_timeout_ms, .requests = stream_read_requests } };
            },
            k2idx("config") => {
                if (array.len < 2)
                    return Error.InvalidCommand;
                switch (k2idx(array[1])) {
                    k2idx("get") => {
                        if (array.len != 3)
                            return Error.WrongNumberOfArguments;
                        break :blk .{ .config = ConfigCommand{ .get = array[2] } };
                    },

                    k2idx("set") => {
                        if (array.len != 4)
                            return Error.WrongNumberOfArguments;
                        break :blk .{ .config = ConfigCommand{ .set = .{ .option = array[2], .value = array[3] } } };
                    },
                    else => return Error.InvalidCommand,
                }
            },
            k2idx("info") => {
                if (array.len != 2 or k2idx(array[1]) != k2idx("replication"))
                    return Error.InvalidCommand;
                break :blk .{ .info = .replication };
            },
            k2idx("replconf") => {
                if (array.len < 2)
                    return Error.InvalidCommand;
                var replconf_command: ReplicaConfigCommand = undefined;
                if (std.ascii.eqlIgnoreCase(array[1], "listening-port")) {
                    if (array.len != 3)
                        return Error.WrongNumberOfArguments;
                    replconf_command = ReplicaConfigCommand{ .listening_port = std.fmt.parseInt(u16, array[2], 10) catch {
                        return Error.InvalidArgument;
                    } };
                } else if (std.ascii.eqlIgnoreCase(array[1], "capa")) {
                    if (array.len < 3)
                        return Error.MissingArgument;
                    replconf_command = ReplicaConfigCommand{ .capabilities = try allocator.dupe([]const u8, array[2..]) };
                } else if (std.ascii.eqlIgnoreCase(array[1], "getack")) {
                    if (array.len != 3)
                        return Error.InvalidCommand;
                    replconf_command = .getack;
                } else {
                    return Error.InvalidCommand;
                }
                break :blk .{ .replconf = replconf_command };
            },
            k2idx("psync") => {
                if (array.len != 3)
                    return Error.InvalidCommand;
                break :blk .{ .psync = .{ .replication_id = array[1], .offset = std.fmt.parseInt(i64, array[2], 10) catch {
                    return Error.InvalidArgument;
                } } };
            },
            k2idx("wait") => {
                if (array.len != 3)
                    return Error.InvalidCommand;
                break :blk .{ .wait = .{ .num_replicas = std.fmt.parseInt(usize, array[1], 10) catch {
                    return Error.InvalidArgument;
                }, .timeout_ms = std.fmt.parseInt(usize, array[2], 10) catch {
                    return Error.InvalidArgument;
                } } };
            },
            k2idx("type") => {
                if (array.len < 2)
                    return Error.MissingArgument;
                if (array.len != 2)
                    return Error.WrongNumberOfArguments;
                break :blk .{ .type = array[1] };
            },
            k2idx("incr") => {
                if (array.len < 2)
                    return Error.MissingArgument;
                if (array.len != 2)
                    return Error.WrongNumberOfArguments;
                break :blk .{ .incr = array[1] };
            },
            k2idx("multi") => {
                break :blk .multi;
            },
            k2idx("exec") => {
                break :blk .exec;
            },
            k2idx("discard") => {
                break :blk .discard;
            },
            else => {
                return Error.UnsupportedCommand;
            },
        };

        return .{ command, parsed_bytes };
    }

    pub fn shouldPropagate(self: *const Self) bool {
        switch (self.*.type) {
            .ping, .echo, .get, .keys, .xrange, .xread, .config, .info, .replconf, .psync, .wait, .type => return false,
            .set, .xadd, .incr, .multi, .discard, .exec => return true,
        }
    }

    pub fn encode(self: *const Self, allocator: std.mem.Allocator) ![]u8 {
        var temp_allocator = std.heap.ArenaAllocator.init(allocator);
        defer temp_allocator.deinit();
        var response = std.ArrayList(resp.Value).init(temp_allocator.allocator());
        switch (self.*.type) {
            .ping => try response.append(resp.BulkString("PONG")),
            .echo => |string| try response.appendSlice(&[_]resp.Value{ resp.BulkString("ECHO"), resp.BulkString(string) }),
            .get => |key| try response.appendSlice(&[_]resp.Value{ resp.BulkString("GET"), resp.BulkString(key) }),
            .set => |set_command| {
                try response.appendSlice(&[_]resp.Value{ resp.BulkString("SET"), resp.BulkString(set_command.key), resp.BulkString(set_command.value) });
                if (set_command.expire_at_ms) |expiration| {
                    try response.appendSlice(&[_]resp.Value{ resp.BulkString("PXAT"), resp.BulkString(try std.fmt.allocPrint(allocator, "{d}", .{expiration})) });
                }
            },
            .keys => |pattern| try response.appendSlice(&[_]resp.Value{ resp.BulkString("KEYS"), resp.BulkString(pattern) }),
            .xadd => |stream_add_command| {
                try response.appendSlice(&[_]resp.Value{ resp.BulkString("XADD"), resp.BulkString(stream_add_command.stream_key), resp.BulkString(try std.fmt.allocPrint(allocator, "{}", .{stream_add_command.entry_id})) });
                for (stream_add_command.key_value_pairs) |pair| {
                    try response.append(resp.BulkString(pair.key));
                    try response.append(resp.BulkString(pair.value));
                }
            },
            .xrange => |stream_range_command| {
                try response.append(resp.BulkString("XRANGE"));
                try response.append(resp.BulkString(stream_range_command.stream_key));
                try response.append(resp.BulkString(try std.fmt.allocPrint(allocator, "{}", .{stream_range_command.start_entry_id})));
                try response.append(resp.BulkString(try std.fmt.allocPrint(allocator, "{}", .{stream_range_command.end_entry_id})));
            },
            .xread => |stream_read_requests| {
                try response.append(resp.BulkString("XREAD"));
                try response.append(resp.BulkString("streams"));
                for (stream_read_requests) |request| {
                    try response.append(resp.BulkString(request.stream_key));
                }
                for (stream_read_requests) |request| {
                    try response.append(resp.BulkString(try std.fmt.allocPrint(allocator, "{}", .{request.start_entry_id})));
                }
            },
            .config => |config_command| {
                try response.append(resp.BulkString("CONFIG"));
                switch (config_command) {
                    .get => |option| {
                        try response.append(resp.BulkString("GET"));
                        try response.append(resp.BulkString(option));
                    },
                    .set => |set_command| {
                        try response.append(resp.BulkString("SET"));
                        try response.append(resp.BulkString(set_command.option));
                        try response.append(resp.BulkString(set_command.value));
                    },
                }
            },
            .info => |info_command| {
                try response.append(resp.BulkString("CONFIG"));
                switch (info_command) {
                    .replication => {
                        try response.append(resp.BulkString("REPLICATION"));
                    },
                }
            },
            .replconf => |replica_config_command| {
                try response.append(resp.BulkString("REPLCONF"));
                switch (replica_config_command) {
                    .getack => {
                        try response.append(resp.BulkString("GETACK"));
                        try response.append(resp.BulkString("*"));
                    },
                    .capabilities => |capabilities| {
                        try response.append(resp.BulkString("capa"));
                        for (capabilities) |capability| {
                            try response.append(resp.BulkString(capability));
                        }
                    },
                    .listening_port => |listening_port| {
                        try response.append(resp.BulkString("listening-port"));
                        try response.append(resp.BulkString(try std.fmt.allocPrint(allocator, "{d}", .{listening_port})));
                    },
                }
            },
            .wait => {
                try response.append(resp.BulkString("WAIT"));
            },
            .type => |key| {
                try response.append(resp.BulkString("TYPE"));
                try response.append(resp.BulkString(key));
            },
            else => {
                return error.InvalidCommand;
            },
        }
        return try resp.Array(try response.toOwnedSlice()).encode(allocator);
    }

    pub fn deinit(self: *Command) void {
        switch (self.*.type) {
            .ping, .echo, .set, .get, .psync, .keys, .xrange, .config, .info, .wait, .type, .incr, .multi, .exec, .discard => {},
            .xadd => |stream_add_command| {
                self.allocator.free(stream_add_command.key_value_pairs);
            },
            .xread => |stream_read_requests| {
                self.allocator.free(stream_read_requests.requests);
            },
            .replconf => |replica_config_command| {
                if (replica_config_command == .capabilities) {
                    self.allocator.free(replica_config_command.capabilities);
                }
            },
        }
        self.allocator.free(self.bytes);
        self.* = undefined;
    }
};

pub const SetCommand = struct { key: []const u8, value: []const u8, expire_at_ms: ?i64 = null };

pub const StreamAddCommand = struct {
    stream_key: []const u8,
    entry_id: db.StreamEntryID,
    key_value_pairs: []struct { key: []const u8, value: []const u8 },
};

pub const StreamRangeBound = union(enum) {
    entry_id: db.StreamEntryID,
    plus,
    minus,

    pub fn format(value: @This(), comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;

        switch (value) {
            .entry_id => try std.fmt.format(writer, "{}", .{value.entry_id}),
            .plus => _ = try writer.write("+"),
            .minus => _ = try writer.write("-"),
        }
    }
};

pub const StreamRangeCommand = struct {
    stream_key: []const u8,
    start_entry_id: StreamRangeBound,
    end_entry_id: StreamRangeBound,
};

pub const StreamReadRequest = struct { stream_key: []const u8, start_entry_id: union(enum) { entry_id: db.StreamEntryID, new_data } };

pub const ConfigCommand = union(enum) {
    get: []const u8,
    set: struct { option: []const u8, value: []const u8 },
};

pub const InfoCommand = union(enum) {
    replication,
};

pub const ReplicaConfigCommand = union(enum) {
    getack,
    capabilities: []const []const u8,
    listening_port: u16,
};

pub const PsyncConfigCommand = struct {
    replication_id: []const u8,
    offset: i64,
};

pub const WaitCommand = struct {
    num_replicas: usize,
    timeout_ms: usize,
};

fn parseStreamEntryId(string: []const u8) !db.StreamEntryID {
    var stream_entry_id = db.StreamEntryID{};
    if (!std.mem.eql(u8, string, "*")) {
        const separator_pos = std.mem.indexOf(u8, string, "-") orelse string.len;
        stream_entry_id.timestamp = try std.fmt.parseInt(u48, string[0..separator_pos], 10);
        if (separator_pos != string.len and !std.mem.eql(u8, string[separator_pos + 1 ..], "*")) {
            stream_entry_id.sequence_number = try std.fmt.parseInt(u16, string[separator_pos + 1 ..], 10);
        }
    }
    return stream_entry_id;
}

pub fn errorToString(err: (Error || std.mem.Allocator.Error)) ![]const u8 {
    return switch (err) {
        Error.InvalidArgument => "invalid argument",
        Error.InvalidCommand => "invalid command",
        Error.InvalidInput => "invalid bytes on the wire",
        Error.InvalidStreamID => "invalid stream ID",
        Error.MissingArgument => "missing argument",
        Error.NotEnoughArguments => "not enough arguments",
        Error.UnsupportedCommand => "unsupported command",
        Error.WrongNumberOfArguments => "wrong number of arguments",
        else => err,
    };
}
