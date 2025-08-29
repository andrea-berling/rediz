const std = @import("std");
const db = @import("db.zig");
const resp = @import("resp.zig");

pub const Error = error{ InvalidArgument, InvalidCommand, InvalidInput, InvalidStreamID, MissingArgument, NotEnoughArguments, UnsupportedCommand, WrongNumberOfArguments };

pub const Command = struct {
    bytes: []u8,
    type: union(enum) {
        ping,
        echo: []const u8,
        get: []const u8,
        set: SetCommand,
        keys: []const u8,
        xadd: StreamAddCommand,
        xrange: StreamRangeCommand,
        xread: struct {
            block_timeout_ms: ?usize,
            requests: []const StreamReadRequest,
        },
        config: ConfigCommand,
        info: InfoCommand,
        replconf: ReplicaConfigCommand,
        psync: PsyncConfigCommand,
        wait: WaitCommand,
        type: []const u8,
        incr: []const u8,
        multi,
        exec,
        discard,
        list_push: PushCommand,
        lrange: struct {
            key: []const u8,
            start: i64,
            end: i64,
        },
        llen: []const u8,
        lpop: struct {
            key: []const u8,
            n: ?usize,
        },
        blpop: struct {
            key: []const u8,
            timeout_s: f64,
        },
        subscribe: []const u8,
        publish: struct {
            chan: []const u8,
            message: []const u8,
        },
        unsubscribe: []const u8,
        zadd: struct {
            key: []const u8,
            score: f64,
            name: []const u8,
        },
        zrank: struct {
            key: []const u8,
            name: []const u8,
        },
        zrange: struct {
            key: []const u8,
            range_start: i32,
            range_end: i32,
        },
        zcard: []const u8,
        zscore: struct {
            key: []const u8,
            name: []const u8,
        },
        zrem: struct {
            key: []const u8,
            name: []const u8,
        },
        geoadd: struct {
            key: []const u8,
            longitude: f64,
            latitude: f64,
            name: []const u8,
        },
        geopos: struct {
            key: []const u8,
            locations: []const []const u8,
        },
        geodist: struct {
            key: []const u8,
            starting_location: []const u8,
            destination: []const u8,
        },
        geosearch: struct {
            key: []const u8,
            center_latitude: f64,
            center_longitude: f64,
            radius: f64,
            radius_unit: enum {
                meters,
            },
        },
    },
    allocator: std.mem.Allocator,

    const Self = @This();

    fn keywordToIndex(string: []const u8) isize {
        const type_info = @typeInfo(@FieldType(@This(), "type"));
        const fields_info = comptime type_info.@"union".fields;
        const other_keywords = [_][]const u8{
            "px",
            "pxat",
            "ex",
            "exat",
            "streams",
            "replication",
            "block",
            "rpush",
            "lpush",
            "fromlonat",
            "byradius",
            "m",
        };
        var keywords: [fields_info.len + other_keywords.len][]const u8 = undefined;
        @setEvalBranchQuota(10000);
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
        if (value != .array or value.array == null) {
            return Error.InvalidInput;
        }

        var command: Command = undefined;
        command.allocator = allocator;
        command.bytes = try allocator.dupe(u8, bytes[0..parsed_bytes]);
        errdefer allocator.free(command.bytes);

        var array = try temp_allocator.allocator().alloc([]const u8, value.array.?.len);

        for (0..value.array.?.len) |i| {
            switch (value.array.?[i]) {
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
            k2idx("rpush"), k2idx("lpush") => {
                if (array.len < 3)
                    return Error.NotEnoughArguments;
                break :blk .{
                    .list_push = .{
                        .key = array[1],
                        .values = try allocator.dupe([]const u8, array[2..]),
                        .type = if (k2idx(array[0]) == k2idx("rpush")) .append else .prepend,
                    },
                };
            },
            k2idx("lrange") => {
                if (array.len < 4)
                    return Error.NotEnoughArguments;
                const start = std.fmt.parseInt(i64, array[2], 10) catch return error.InvalidArgument;
                const end = std.fmt.parseInt(i64, array[3], 10) catch return error.InvalidArgument;
                break :blk .{ .lrange = .{ .key = array[1], .start = start, .end = end } };
            },
            k2idx("llen") => {
                if (array.len != 2)
                    return Error.WrongNumberOfArguments;
                break :blk .{ .llen = array[1] };
            },
            k2idx("lpop") => {
                if (array.len < 2)
                    return Error.NotEnoughArguments;
                if (array.len > 4)
                    return Error.WrongNumberOfArguments;
                const n_elements = if (array.len > 2) n: {
                    break :n std.fmt.parseInt(usize, array[2], 10) catch return error.InvalidArgument;
                } else null;
                break :blk .{ .lpop = .{ .key = array[1], .n = n_elements } };
            },
            k2idx("blpop") => {
                if (array.len < 3)
                    return Error.NotEnoughArguments;
                const timeout_s = std.fmt.parseFloat(f64, array[2]) catch return error.InvalidArgument;
                break :blk .{ .blpop = .{ .key = array[1], .timeout_s = timeout_s } };
            },
            k2idx("subscribe") => {
                if (array.len < 2)
                    return Error.NotEnoughArguments;
                break :blk .{ .subscribe = array[1] };
            },
            k2idx("publish") => {
                if (array.len != 3)
                    return Error.WrongNumberOfArguments;
                break :blk .{ .publish = .{
                    .chan = array[1],
                    .message = array[2],
                } };
            },
            k2idx("unsubscribe") => {
                if (array.len != 2)
                    return Error.WrongNumberOfArguments;
                break :blk .{ .unsubscribe = array[1] };
            },
            k2idx("zadd") => {
                if (array.len != 4)
                    return Error.WrongNumberOfArguments;
                const score = std.fmt.parseFloat(f64, array[2]) catch return error.InvalidArgument;
                break :blk .{
                    .zadd = .{
                        .key = array[1],
                        .score = score,
                        .name = array[3],
                    },
                };
            },
            k2idx("zrank") => {
                if (array.len != 3)
                    return Error.WrongNumberOfArguments;
                break :blk .{
                    .zrank = .{
                        .key = array[1],
                        .name = array[2],
                    },
                };
            },
            k2idx("zrange") => {
                if (array.len != 4)
                    return Error.WrongNumberOfArguments;
                const range_start = std.fmt.parseInt(i32, array[2], 10) catch return error.InvalidArgument;
                const range_end = std.fmt.parseInt(i32, array[3], 10) catch return error.InvalidArgument;
                break :blk .{
                    .zrange = .{
                        .key = array[1],
                        .range_start = range_start,
                        .range_end = range_end,
                    },
                };
            },
            k2idx("zcard") => {
                if (array.len != 2)
                    return Error.WrongNumberOfArguments;
                break :blk .{
                    .zcard = array[1],
                };
            },
            k2idx("zscore") => {
                if (array.len != 3)
                    return Error.WrongNumberOfArguments;
                break :blk .{
                    .zscore = .{
                        .key = array[1],
                        .name = array[2],
                    },
                };
            },
            k2idx("zrem") => {
                if (array.len != 3)
                    return Error.WrongNumberOfArguments;
                break :blk .{
                    .zrem = .{
                        .key = array[1],
                        .name = array[2],
                    },
                };
            },
            k2idx("geoadd") => {
                if (array.len != 5)
                    return Error.WrongNumberOfArguments;

                const longitude = std.fmt.parseFloat(f64, array[2]) catch return error.InvalidArgument;
                const latitude = std.fmt.parseFloat(f64, array[3]) catch return error.InvalidArgument;
                break :blk .{
                    .geoadd = .{
                        .key = array[1],
                        .longitude = longitude,
                        .latitude = latitude,
                        .name = array[4],
                    },
                };
            },
            k2idx("geopos") => {
                if (array.len < 3)
                    return Error.WrongNumberOfArguments;

                break :blk .{
                    .geopos = .{
                        .key = array[1],
                        .locations = try allocator.dupe([]const u8, array[2..]),
                    },
                };
            },
            k2idx("geodist") => {
                if (array.len != 4)
                    return Error.WrongNumberOfArguments;

                break :blk .{
                    .geodist = .{
                        .key = array[1],
                        .starting_location = array[2],
                        .destination = array[3],
                    },
                };
            },
            k2idx("geosearch") => {
                if (array.len < 3)
                    return Error.WrongNumberOfArguments;

                var ret: @FieldType(@This(), "type") = .{
                    .geosearch = .{
                        .key = array[1],
                        .center_latitude = undefined,
                        .center_longitude = undefined,
                        .radius = undefined,
                        .radius_unit = undefined,
                    },
                };
                var i: usize = 2;

                while (i < array.len) {
                    switch (k2idx(array[i])) {
                        k2idx("fromlonlat") => {
                            i += 1;
                            if (array.len - i < 2) return Error.NotEnoughArguments;
                            ret.geosearch.center_latitude = std.fmt.parseFloat(f64, array[i + 1]) catch return error.InvalidArgument;
                            ret.geosearch.center_longitude = std.fmt.parseFloat(f64, array[i + 1]) catch return error.InvalidArgument;
                            i += 2;
                        },
                        k2idx("byradius") => {
                            i += 1;
                            if (array.len - i < 2) return Error.NotEnoughArguments;
                            ret.geosearch.radius = std.fmt.parseFloat(f64, array[i]) catch return error.InvalidArgument;

                            ret.geosearch.radius_unit = switch (k2idx(array[i + 1])) {
                                k2idx("m") => .meters,
                                else => return error.InvalidArgument,
                            };
                        },
                        else => return Error.InvalidArgument,
                    }
                }

                break :blk ret;
            },
            else => {
                return Error.UnsupportedCommand;
            },
        };

        return .{ command, parsed_bytes };
    }

    pub fn shouldPropagate(self: *const Self) bool {
        switch (self.*.type) {
            .ping,
            .echo,
            .get,
            .keys,
            .xrange,
            .xread,
            .config,
            .info,
            .replconf,
            .psync,
            .wait,
            .type,
            .lrange,
            .llen,
            .subscribe,
            .unsubscribe,
            .zcard,
            .zrange,
            .zrank,
            .zscore,
            .geopos,
            .geodist,
            .geosearch,
            => return false,
            .set,
            .xadd,
            .incr,
            .multi,
            .discard,
            .exec,
            .list_push,
            .lpop,
            .blpop,
            .publish,
            .zadd,
            .zrem,
            .geoadd,
            => return true,
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
            .list_push => |push_command| {
                if (push_command.type == .append) {
                    try response.append(resp.BulkString("RPUSH"));
                } else try response.append(resp.BulkString("LPUSH"));
                try response.append(resp.BulkString(push_command.key));
                for (push_command.values) |value| {
                    try response.append(resp.BulkString(value));
                }
            },
            .lrange => |lrange_command| {
                try response.append(resp.BulkString("LRANGE"));
                try response.append(resp.BulkString(lrange_command.key));
                try response.append(resp.BulkString(try std.fmt.allocPrint(allocator, "{d}", .{lrange_command.start})));
                try response.append(resp.BulkString(try std.fmt.allocPrint(allocator, "{d}", .{lrange_command.end})));
            },
            .llen => |list| {
                try response.append(resp.BulkString("LLEN"));
                try response.append(resp.BulkString(list));
            },
            .lpop => |lpop_command| {
                try response.append(resp.BulkString("LPOP"));
                try response.append(resp.BulkString(lpop_command.key));
                if (lpop_command.n) |n| {
                    try response.append(resp.BulkString(try std.fmt.allocPrint(allocator, "{d}", .{n})));
                }
            },
            .blpop => |blpop_command| {
                try response.append(resp.BulkString("BLPOP"));
                try response.append(resp.BulkString(blpop_command.key));
                try response.append(resp.BulkString(try std.fmt.allocPrint(allocator, "{e}", .{blpop_command.timeout_s})));
                if (blpop_command.timeout_ms) |n| {
                    try response.append(resp.BulkString(try std.fmt.allocPrint(allocator, "{d}", .{n})));
                }
            },
            .subscribe => |chan| {
                try response.append(resp.BulkString("SUBSCRIBE"));
                try response.append(resp.BulkString(chan));
            },
            .publish => |publish_command| {
                try response.append(resp.BulkString("PUBLISH"));
                try response.append(resp.BulkString(publish_command.chan));
                try response.append(resp.BulkString(publish_command.message));
            },
            .unsubscribe => |chan| {
                try response.append(resp.BulkString("UNSUBSCRIBE"));
                try response.append(resp.BulkString(chan));
            },
            .zadd => |zadd_command| {
                try response.append(resp.BulkString("ZADD"));
                try response.append(resp.BulkString(zadd_command.key));
                try response.append(resp.BulkString(
                    try std.fmt.allocPrint(temp_allocator.allocator(), "{d}", .{zadd_command.score}),
                ));
                try response.append(resp.BulkString(zadd_command.name));
            },
            .zrank => |zrank_command| {
                try response.append(resp.BulkString("ZRANK"));
                try response.append(resp.BulkString(zrank_command.key));
                try response.append(resp.BulkString(zrank_command.name));
            },
            .zrange => |zrange_command| {
                try response.append(resp.BulkString("ZRANGE"));
                try response.append(resp.BulkString(zrange_command.key));
                try response.append(resp.BulkString(
                    try std.fmt.allocPrint(temp_allocator.allocator(), "{d}", .{zrange_command.range_start}),
                ));
                try response.append(resp.BulkString(
                    try std.fmt.allocPrint(temp_allocator.allocator(), "{d}", .{zrange_command.range_end}),
                ));
            },
            .zcard => |zcard_command| {
                try response.append(resp.BulkString("ZCARD"));
                try response.append(resp.BulkString(zcard_command.key));
            },
            .zscore => |zscore_command| {
                try response.append(resp.BulkString("ZSCORE"));
                try response.append(resp.BulkString(zscore_command.key));
                try response.append(resp.BulkString(zscore_command.name));
            },
            .zrem => |zrem_command| {
                try response.append(resp.BulkString("ZSCORE"));
                try response.append(resp.BulkString(zrem_command.key));
                try response.append(resp.BulkString(zrem_command.name));
            },
            .geoadd => |command| {
                try response.append(resp.BulkString("GEOADD"));
                try response.append(resp.BulkString(command.key));
                try response.append(resp.BulkString(
                    try std.fmt.allocPrint(temp_allocator.allocator(), "{d}", .{command.longitude}),
                ));
                try response.append(resp.BulkString(
                    try std.fmt.allocPrint(temp_allocator.allocator(), "{d}", .{command.latitude}),
                ));
                try response.append(resp.BulkString(command.name));
            },
            .geopos => |command| {
                try response.append(resp.BulkString("GEOPOS"));
                try response.append(resp.BulkString(command.key));
                try response.append(resp.BulkString(command.name));
            },
            .geodist => |command| {
                try response.append(resp.BulkString("GEODIST"));
                try response.append(resp.BulkString(command.key));
                try response.append(resp.BulkString(command.starting_location));
                try response.append(resp.BulkString(command.destination));
            },
            .geosearch => |command| {
                try response.append(resp.BulkString("GEOSEARCH"));
                try response.append(resp.BulkString(command.key));
                try response.append(resp.BulkString("FROMLONLAT"));
                try response.append(resp.BulkString(command.center_longitude));
                try response.append(resp.BulkString(command.center_latitude));
                try response.append(resp.BulkString("BYRADIUS"));
                try response.append(resp.BulkString(command.radius));
                try response.append(resp.BulkString(
                    switch (command.radius_unit) {
                        .meters => "m",
                    },
                ));
            },
        }
        return try resp.Array(try response.toOwnedSlice()).encode(allocator);
    }

    pub fn name(self: *const Command) []const u8 {
        return @tagName(self.type);
    }

    pub fn deinit(self: *Command) void {
        switch (self.*.type) {
            .ping,
            .echo,
            .set,
            .get,
            .psync,
            .keys,
            .xrange,
            .config,
            .info,
            .wait,
            .type,
            .incr,
            .multi,
            .exec,
            .discard,
            .lrange,
            .llen,
            .lpop,
            .blpop,
            .subscribe,
            .publish,
            .unsubscribe,
            .zrem,
            .zadd,
            .zscore,
            .zrank,
            .zrange,
            .zcard,
            .geoadd,
            .geodist,
            .geosearch,
            => {},
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
            .list_push => |push_command| {
                self.allocator.free(push_command.values);
            },
            .geopos => |args| {
                self.allocator.free(args.locations);
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

pub const PushCommand = struct { type: union(enum) { append, prepend }, key: []const u8, values: []const []const u8 };

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
