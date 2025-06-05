const std = @import("std");

pub const Value = union(enum) {
    simple_string: []const u8,
    bulk_string: []const u8,
    integer: i64,
    simple_error: []const u8,
    array: []const Value,
    null,

    pub fn encode(self: *const Value, allocator: std.mem.Allocator) ![]u8 {
        switch (self.*) {
            .simple_string => |string| {
                return try std.fmt.allocPrint(allocator, "+{s}\r\n", .{string});
            },
            .bulk_string => |string| {
                return try std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{ string.len, string });
            },
            .null => {
                return allocator.dupe(u8, "$-1\r\n");
            },
            .integer => |i| {
                var response = std.ArrayList(u8).init(allocator);
                try response.append(':');
                if (i < 0) {
                    try response.append('-');
                }
                try std.fmt.format(response.writer(), "{d}\r\n", .{i});
                return try response.toOwnedSlice();
            },
            .simple_error => |msg| {
                return try std.fmt.allocPrint(allocator, "-ERR {s}\r\n", .{msg});
            },
            .array => |array| {
                var response = std.ArrayList(u8).init(allocator);
                try std.fmt.format(response.writer(), "*{d}\r\n", .{array.len});
                for (array) |value| {
                    const encoded_element = try value.encode(allocator);
                    try response.appendSlice(encoded_element);
                }
                return response.toOwnedSlice();
            },
        }
    }

    const MAX_DECIMAL_LEN = 10;

    fn parseDecimal(bytes: []const u8) !struct { u64, usize } {
        var i: usize = 0;
        var return_value: u64 = 0;

        while (i < bytes.len and std.ascii.isDigit(bytes[i]) and i < MAX_DECIMAL_LEN) : (i += 1) {
            return_value = return_value * 10 + bytes[i] - '0';
        }

        if (i == 0)
            return error.InvalidDecimal;

        return .{ return_value, i };
    }

    inline fn cr_nl(bytes: []const u8) bool {
        return bytes.len > 2 and bytes[0] == '\r' and bytes[1] == '\n';
    }

    /// If the returned value is a bulk string, simple string, or simple error,
    /// it points to a slice of the input byte slice (i.e. it lifetime depends
    /// on the input)
    /// If the returned value is an array, the array is dynamically allocated
    pub fn parse(bytes: []const u8, allocator: std.mem.Allocator) !struct { Value, usize } {
        if (bytes.len <= 1)
            return error.InvalidInput;
        switch (bytes[0]) {
            '*' => {
                if (bytes.len < 3)
                    return error.InvalidArray;
                var i: usize = 1;
                const n_elem, const bytes_parsed = try parseDecimal(bytes[i..]);
                i += bytes_parsed;
                if (!cr_nl(bytes[i..])) return error.InvalidArray;
                i += 2;
                var elements = try allocator.alloc(Value, n_elem);

                for (0..n_elem) |n| {
                    elements[n], const parsed_bytes = try Value.parse(bytes[i..], allocator);
                    i += parsed_bytes;
                }
                return .{ Value{ .array = elements }, i };
            },
            '+' => {
                if (bytes.len < 3) return error.InvalidSimpleString;
                var i: usize = 1;

                while (!cr_nl(bytes[i..])) : (i += 1) {
                    if (i >= bytes.len - 2) return error.UnterminatedSimpleString;
                }

                return .{ Value{ .simple_string = bytes[1..i] }, i + 2 };
            },
            '$' => {
                var i: usize = 1;
                const string_length, const bytes_parsed = try parseDecimal(bytes[i..]);
                i += bytes_parsed;

                if (!cr_nl(bytes[i..]) or !cr_nl(bytes[i + 2 + string_length ..])) return error.InvalidRESPBulkString;

                if (std.mem.eql(u8, bytes[i + 2 ..][0..string_length], "-1")) {
                    return .{ .null, i + 2 + string_length + 2 };
                } else return .{ Value{ .bulk_string = bytes[i + 2 ..][0..string_length] }, i + 2 + string_length + 2 };
            },
            else => return error.InvalidInput,
        }
    }
};

pub inline fn BulkString(string: []const u8) Value {
    return Value{ .bulk_string = string };
}

pub inline fn Array(array: []const Value) Value {
    return Value{ .array = array };
}

pub inline fn SimpleString(string: []const u8) Value {
    return Value{ .simple_string = string };
}

pub inline fn SimpleError(msg: []const u8) Value {
    return Value{ .simple_error = msg };
}

pub inline fn Integer(i: i64) Value {
    return Value{ .integer = i };
}

pub const Null: Value = .null;

pub const Ok: Value = .{ .simple_string = "OK" };

// TODO: tests for encoding and decoding
