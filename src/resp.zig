const std = @import("std");

const MAX_DECIMAL_LEN = 10;

pub fn parse_decimal(bytes: []u8) !struct { u64, usize } {
    var i: usize = 0;
    var return_value: u64 = 0;

    while (std.ascii.isDigit(bytes[i]) and i < MAX_DECIMAL_LEN) : (i += 1) {
        return_value = return_value * 10 + bytes[i] - '0';
    }

    return .{ return_value, i };
}

pub fn parse_bulk_string(allocator: std.mem.Allocator, bytes: []u8) !struct { []u8, usize } {
    if (bytes[0] != '$') return error.InvalidRESPBulkString;
    var i: usize = 1;
    const string_length, const bytes_parsed = try parse_decimal(bytes[i..]);
    i += bytes_parsed;

    if (!std.mem.eql(u8, bytes[i .. i + 2], "\r\n")) return error.InvalidRESPBulkString;
    i += 2;
    const return_value = try allocator.alloc(u8, string_length);
    std.mem.copyBackwards(u8, return_value, bytes[i .. i + string_length]);
    i += string_length;
    if (!std.mem.eql(u8, bytes[i .. i + 2], "\r\n")) return error.InvalidRESPBulkString;
    i += 2;
    return .{ return_value, i };
}

pub fn parse_array(allocator: std.mem.Allocator, bytes: []u8) !struct { [][]u8, usize } {
    if (bytes[0] != '*') return error.InvalidRESPArray;
    var i: usize = 1;
    const n_elem, const bytes_parsed = try parse_decimal(bytes[i..]);
    i += bytes_parsed;
    if (!std.mem.eql(u8, bytes[i .. i + 2], "\r\n")) return error.InvalidRESPArray;
    i += 2;
    var elements = try allocator.alloc([]u8, n_elem);

    for (0..n_elem) |n| {
        const element, const parsed_bytes = try parse_bulk_string(allocator, bytes[i..]);
        elements[n] = element;
        i += parsed_bytes;
    }
    return .{ elements, i };
}

pub fn destroy_array(allocator: std.mem.Allocator, array: [][]u8) void {
    for (array) |element| {
        allocator.free(element);
    }
    allocator.free(array);
}

pub fn encode_bulk_string(allocator: std.mem.Allocator,maybe_s: ?[]const u8) ![]u8 {
    var response = std.ArrayList(u8).init(allocator);
    if (maybe_s) |s| {
        try std.fmt.format(response.writer(), "${d}\r\n", .{s.len});
        _ = try response.writer().write(s);
        _ = try response.writer().write("\r\n");
        return response.toOwnedSlice();
    }
    else {
        _ = try response.writer().write( "$-1\r\n");
        return response.toOwnedSlice();
    }
}

pub fn encode_simple_string(allocator: std.mem.Allocator ,s: []const u8) ![]u8 {
    var response = std.ArrayList(u8).init(allocator);
    try std.fmt.format(response.writer(), "+{s}\r\n", .{s});
    return response.toOwnedSlice();
}

pub fn encode_array(allocator: std.mem.Allocator, array: []const []const u8) ![]u8 {
    var response = std.ArrayList(u8).init(allocator);
    try std.fmt.format(response.writer(), "*{d}\r\n", .{array.len});
    for (array) |string| {
        const element = try encode_bulk_string(allocator, string);
        defer allocator.free(element);
        _ = try response.writer().write(element);
    }
    return response.toOwnedSlice();
}
