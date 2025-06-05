const std = @import("std");
const db = @import("db.zig");
const resp = @import("resp.zig");

pub const Error = error{ InvalidArgument, InvalidCommand, InvalidInput, InvalidStreamID, MissingArgument, NotEnoughArguments, UnsupportedCommand, WrongNumberOfArguments };

pub const Command = union(enum) {
    ping,
    echo: []const u8,
    get: []const u8,
    set: SetCommand,
    keys: []const u8,
    xadd: StreamAddCommand,
    xrange: StreamRangeCommand,
    xread: []const StreamReadRequest,
    config: ConfigCommand,
    info: InfoCommand,
    replconf: ReplicaConfigCommand,
    psync: PsyncConfigCommand,
    wait: WaitCommand,
    type: []const u8,

    const Self = @This();

    fn keywordToIndex(string: []const u8) isize {
        const type_info = @typeInfo(@This());
        const fields_info = comptime type_info.@"union".fields;
        const other_keywords = [_][]const u8{ "px", "pxat", "ex", "exat", "streams", "replication" };
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

    pub fn parse(bytes: []const u8, allocator: std.mem.Allocator) (Error || std.mem.Allocator.Error)!struct { Command, usize } {
        const array, const parsed_bytes = resp.parseArray(allocator, bytes) catch return Error.InvalidInput;
        const k2idx = keywordToIndex;
        const command: Command = blk: switch (k2idx(array[0])) {
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

                break :blk Self{ .set = set_command };
            },
            k2idx("keys") => {
                if (array.len == 1)
                    return Error.MissingArgument;
                if (array.len != 2)
                    return Error.WrongNumberOfArguments;
                if (!std.mem.eql(u8, array[1], "*")) // only pattern we support for now
                    return Error.InvalidArgument;
                break :blk Self{ .keys = array[1] };
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
                break :blk Self{ .xadd = stream_add_command };
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
                break :blk Self{ .xrange = stream_range_command };
            },
            k2idx("xread") => {
                if (array.len < 4)
                    return Error.NotEnoughArguments;
                if (k2idx(array[1]) != k2idx("streams"))
                    return Error.InvalidCommand;
                if ((array.len - 2) % 2 != 0)
                    return Error.WrongNumberOfArguments;

                var stream_read_requests = try allocator.alloc(StreamReadRequest, (array.len - 2) / 2);

                var first_range_index: usize = 2;
                // Basically Zig's is_err()
                // https://ziggit.dev/t/more-syntactically-cleaner-way-to-check-if-a-something-is-an-error-from-an-error-union/4312/3
                while (if (parseStreamEntryId(array[first_range_index])) |_| false else |_| true) : (first_range_index += 1) {}

                const offset = first_range_index - 2;

                for (0..(array.len - 2) / 2) |i| {
                    stream_read_requests[i] = StreamReadRequest{
                        .stream_key = array[i + 2],
                        .start_entry_id = parseStreamEntryId(array[i + 2 + offset]) catch {
                            return Error.InvalidStreamID;
                        },
                    };
                }

                break :blk Self{ .xread = stream_read_requests };
            },
            k2idx("config") => {
                if (array.len < 2)
                    return Error.InvalidCommand;
                switch (k2idx(array[1])) {
                    k2idx("get") => {
                        if (array.len != 3)
                            return Error.WrongNumberOfArguments;
                        break :blk Self{ .config = ConfigCommand{ .get = array[2] } };
                    },

                    k2idx("set") => {
                        if (array.len != 4)
                            return Error.WrongNumberOfArguments;
                        break :blk Self{ .config = ConfigCommand{ .set = .{ .option = array[2], .value = array[3] } } };
                    },
                    else => return Error.InvalidCommand,
                }
            },
            k2idx("info") => {
                if (array.len != 2 and k2idx(array[1]) != k2idx("replication"))
                    return Error.InvalidCommand;
                break :blk Self{ .info = .replication };
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
                    replconf_command = ReplicaConfigCommand{ .capabilities = array[2..] };
                } else if (std.ascii.eqlIgnoreCase(array[1], "getack")) {
                    if (array.len != 3)
                        return Error.InvalidCommand;
                    replconf_command = .getack;
                } else {
                    return Error.InvalidCommand;
                }
                break :blk Self{ .replconf = replconf_command };
            },
            k2idx("psync") => {
                if (array.len != 3)
                    return Error.InvalidCommand;
                var command = Self{ .psync = undefined };
                command.psync.replication_id = array[1];
                command.psync.offset = std.fmt.parseInt(i64, array[2], 10) catch {
                    return Error.InvalidArgument;
                };
                break :blk command;
            },
            k2idx("wait") => {
                if (array.len != 3)
                    return Error.InvalidCommand;
                var command = Self{ .wait = undefined };
                command.wait.num_replicas = std.fmt.parseInt(usize, array[1], 10) catch {
                    return Error.InvalidArgument;
                };
                command.wait.timeout_ms = std.fmt.parseInt(usize, array[2], 10) catch {
                    return Error.InvalidArgument;
                };
                break :blk command;
            },
            k2idx("type") => {
                if (array.len < 2)
                    return Error.MissingArgument;
                if (array.len != 2)
                    return Error.WrongNumberOfArguments;
                break :blk Self{ .type = array[1] };
            },
            else => {
                return Error.UnsupportedCommand;
            },
        };

        return .{ command, parsed_bytes };
    }

    pub fn shouldPropagate(self: *const Self) bool {
        switch (self.*) {
            .ping, .echo, .get, .keys, .xrange, .xread, .config, .info, .replconf, .psync, .wait, .type => return false,
            .set, .xadd => return true,
        }
    }
};

pub const SetCommand = struct { key: []const u8, value: []const u8, expire_at_ms: ?i64 = null };

pub const StreamAddCommand = struct {
    stream_key: []const u8,
    entry_id: db.StreamEntryID,
    key_value_pairs: []struct { key: []const u8, value: []const u8 },
};

pub const StreamRangeBound = union(enum) { entry_id: db.StreamEntryID, plus, minus };

pub const StreamRangeCommand = struct {
    stream_key: []const u8,
    start_entry_id: StreamRangeBound,
    end_entry_id: StreamRangeBound,
};

pub const StreamReadRequest = struct { stream_key: []const u8, start_entry_id: db.StreamEntryID };

pub const ConfigCommand = union(enum) {
    get: []const u8,
    set: struct { option: []const u8, value: []const u8 },
};

pub const InfoCommand = union(enum) {
    replication,
};

pub const ReplicaConfigCommand = union(enum) {
    getack,
    capabilities: []const []u8,
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
