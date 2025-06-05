const std = @import("std");

const ValueType = enum { simple_string, bulk_string, integer, simple_error, array };

pub const Value = union(ValueType) {
    simple_string: []const u8,
    bulk_string: []const u8,
    integer: i64,
    simple_error: []const u8,
    array: []const Value,

    pub fn encode(self: *const Value, allocator: std.mem.Allocator) anyerror![]u8 {
        var encoded_element: []u8 = undefined;
        switch (self.*) {
            .simple_string => {
                encoded_element = try encodeSimpleString(allocator, self.simple_string);
            },
            .bulk_string => {
                encoded_element = try encodeBulkString(allocator, self.bulk_string);
            },
            .integer => {
                encoded_element = try encodeInteger(allocator, self.integer);
            },
            .simple_error => {
                encoded_element = try encodeSimpleError(allocator, self.simple_error);
            },
            .array => {
                encoded_element = try encodeArray(allocator, self.array);
            },
        }
        return encoded_element;
    }
};

pub inline fn BulkString(string: []const u8) Value {
    return Value{ .bulk_string = string };
}

pub inline fn Array(array: []const Value) Value {
    return Value{ .array = array };
}

const MAX_DECIMAL_LEN = 10;

pub fn parseDecimal(bytes: []const u8) !struct { u64, usize } {
    var i: usize = 0;
    var return_value: u64 = 0;

    while (std.ascii.isDigit(bytes[i]) and i < MAX_DECIMAL_LEN) : (i += 1) {
        return_value = return_value * 10 + bytes[i] - '0';
    }

    return .{ return_value, i };
}

pub fn parseSimpleString(allocator: std.mem.Allocator, bytes: []u8) !struct { []u8, usize } {
    if (bytes[0] != '+') return error.InvalidRESPSimpleString;
    var i: usize = 1;

    var return_value = std.ArrayList(u8).init(allocator);

    while (bytes[i] != '\r') : (i += 1) {
        try return_value.append(bytes[i]);
    }

    if (!std.mem.eql(u8, bytes[i .. i + 2], "\r\n")) return error.InvalidRESPSimpleString;

    i += 2;

    return .{ try return_value.toOwnedSlice(), i };
}

pub fn parseBulkString(allocator: std.mem.Allocator, bytes: []const u8, cr_nl_terminated: bool) !struct { []u8, usize } {
    if (bytes[0] != '$') return error.InvalidRESPBulkString;
    var i: usize = 1;
    const string_length, const bytes_parsed = try parseDecimal(bytes[i..]);
    i += bytes_parsed;

    if (!std.mem.eql(u8, bytes[i .. i + 2], "\r\n")) return error.InvalidRESPBulkString;
    i += 2;
    const return_value = try allocator.alloc(u8, string_length);
    std.mem.copyBackwards(u8, return_value, bytes[i .. i + string_length]);
    i += string_length;
    if (cr_nl_terminated) {
        if (!std.mem.eql(u8, bytes[i .. i + 2], "\r\n")) return error.InvalidRESPBulkString;
        i += 2;
    }
    return .{ return_value, i };
}

pub fn parseArray(allocator: std.mem.Allocator, bytes: []const u8) !struct { [][]u8, usize } {
    if (bytes[0] != '*') return error.InvalidRESPArray;
    var i: usize = 1;
    const n_elem, const bytes_parsed = try parseDecimal(bytes[i..]);
    i += bytes_parsed;
    if (!std.mem.eql(u8, bytes[i .. i + 2], "\r\n")) return error.InvalidRESPArray;
    i += 2;
    var elements = try allocator.alloc([]u8, n_elem);

    for (0..n_elem) |n| {
        const element, const parsed_bytes = try parseBulkString(allocator, bytes[i..], true);
        elements[n] = element;
        i += parsed_bytes;
    }
    return .{ elements, i };
}

pub fn destroyArray(allocator: std.mem.Allocator, array: [][]u8) void {
    for (array) |element| {
        allocator.free(element);
    }
    allocator.free(array);
}

pub fn encodeBulkString(allocator: std.mem.Allocator, maybe_s: ?[]const u8) ![]u8 {
    var response = std.ArrayList(u8).init(allocator);
    if (maybe_s) |s| {
        try std.fmt.format(response.writer(), "${d}\r\n", .{s.len});
        _ = try response.writer().write(s);
        _ = try response.writer().write("\r\n");
        return response.toOwnedSlice();
    } else {
        _ = try response.writer().write("$-1\r\n");
        return response.toOwnedSlice();
    }
}

pub fn encodeInteger(allocator: std.mem.Allocator, n: i64) ![]u8 {
    var response = std.ArrayList(u8).init(allocator);
    try response.append(':');
    if (n < 0) {
        try response.append('-');
    }
    try std.fmt.format(response.writer(), "{d}\r\n", .{n});
    return response.toOwnedSlice();
}

pub inline fn encodeSimpleString(allocator: std.mem.Allocator, string: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, "+{s}\r\n", .{string});
}

pub inline fn encodeSimpleError(allocator: std.mem.Allocator, msg: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, "-ERR {s}\r\n", .{msg});
}

pub fn encodeMessage(allocator: std.mem.Allocator, array: []const []const u8) ![]u8 {
    var message_elements = std.ArrayList(Value).init(allocator);
    defer message_elements.deinit();
    for (array) |part| {
        try message_elements.append(BulkString(part));
    }
    return try encodeArray(allocator, try message_elements.toOwnedSlice());
}

pub fn encodeArray(allocator: std.mem.Allocator, array: []const Value) ![]u8 {
    var response = std.ArrayList(u8).init(allocator);
    try std.fmt.format(response.writer(), "*{d}\r\n", .{array.len});
    for (array) |value| {
        const encoded_element = try value.encode(allocator);
        defer allocator.free(encoded_element);
        try response.appendSlice(encoded_element);
    }
    return response.toOwnedSlice();
}

// TODO: tests for encoding and decoding
