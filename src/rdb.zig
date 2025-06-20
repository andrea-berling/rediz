const std = @import("std");
const db = @import("db.zig");

const Allocator = std.mem.Allocator;

const MarkerByte = enum(u8) {
    METADATA = 0xfa,
    DATABASE = 0xfe,
    HASH_TABLE_SIZE_INFO = 0xfb,
    EOF = 0xff,
    KEY_TYPE_STRING = 0x00,
    KEY_EXPIRE_MS = 0xfc,
    KEY_EXPIRE_S = 0xfd,
};

pub fn parseSize(bytes: []const u8) !struct { usize, u3 } {
    const marker = bytes[0] >> 6;
    var size: usize = 0;
    var parsed_bytes: u3 = 0;
    switch (marker) {
        0b00 => { // 1 byte len
            size = @intCast(bytes[0]);
            parsed_bytes = 1;
        },
        0b01 => { // 2 bytes len + ASCII
            size = @intCast((@as(u16, (bytes[0] & 0x3f)) << 8) | bytes[1]);
            parsed_bytes = 2;
        },
        0b10 => { // 4 bytes len + ASCII (skip the first)
            inline for (0..@sizeOf(u32)) |i| {
                size = (size << 8) | bytes[i + 1];
            }
            parsed_bytes = 5;
        },
        else => {
            return error.InvalidSizeEncoding;
        },
    }
    return .{ size, parsed_bytes };
}

pub fn parseString(bytes: []const u8, alloc: Allocator) !struct { []u8, usize } {
    const marker = bytes[0] >> 6;
    switch (marker) {
        0b00, 0b01, 0b10 => {
            const length, const parsed_bytes = try parseSize(bytes);
            return .{ try alloc.dupe(u8, bytes[parsed_bytes..][0..length]), parsed_bytes + length };
        },
        0b11 => {
            if (bytes[0] & 0x3f == 0xc3) { // LZF-compressed string
                @panic("LZF-compressed string are not supported");
            }
            const byte_length: u8 = @as(u8, 1) << @as(u2, @truncate(bytes[0] & 0x0f));
            var value: u32 = 0;
            for (0..byte_length) |i| { // Little endian
                value = value | (@as(u32, bytes[i + 1]) << @as(u5, @truncate(8 * @as(u8, @truncate(i)))));
            }
            var new_string = std.ArrayList(u8).init(alloc);
            try std.fmt.format(new_string.writer(), "{d}", .{value});
            return .{ try new_string.toOwnedSlice(), 1 + byte_length };
        },
        else => {
            unreachable;
        },
    }
}

pub fn parseMetadataSection(bytes: []const u8, alloc: Allocator) !struct { std.StringHashMap([]u8), usize } {
    var marker = @as(MarkerByte, @enumFromInt(bytes[0]));
    std.debug.assert(marker == .METADATA);
    var i: usize = 0;
    var attributes = std.StringHashMap([]u8).init(alloc);
    while (marker == .METADATA) {
        i += 1;
        const key, const key_bytes = try parseString(bytes[i..], alloc);
        i += key_bytes;
        const value, const value_bytes = try parseString(bytes[i..], alloc);
        try attributes.put(key, @constCast(value));
        i += value_bytes;
        marker = @as(MarkerByte, @enumFromInt(bytes[i]));
    }
    return .{ attributes, i };
}

pub fn parseKeyValuePair(bytes: []const u8, alloc: Allocator) !struct { struct { key: []u8, value: db.Datum }, usize } {
    var parsed_bytes: usize = 0;
    var expire_timestamp_ms: i64 = 0;
    var datum: db.Datum = undefined;
    datum.expire_at_ms = null;
    var key: []u8 = undefined;
    while (true) {
        switch (@as(MarkerByte, @enumFromInt(bytes[parsed_bytes]))) {
            .KEY_EXPIRE_MS, .KEY_EXPIRE_S => |marker| {
                const timestamp_size: usize = switch (marker) {
                    MarkerByte.KEY_EXPIRE_MS => 8,
                    MarkerByte.KEY_EXPIRE_S => 4,
                    else => unreachable,
                };
                for (0..timestamp_size) |i| { // Little endian
                    expire_timestamp_ms = expire_timestamp_ms | (@as(i64, bytes[i + 1]) << @as(u6, @truncate(8 * @as(u8, @truncate(i)))));
                }
                if (marker == .KEY_EXPIRE_S) expire_timestamp_ms *= 1000;
                datum.expire_at_ms = expire_timestamp_ms;
                parsed_bytes += 1 + timestamp_size;
            },
            .KEY_TYPE_STRING => {
                parsed_bytes += 1;
                key, const key_bytes = try parseString(bytes[parsed_bytes..], alloc);
                parsed_bytes += key_bytes;
                const string_value, const value_bytes = try parseString(bytes[parsed_bytes..], alloc);
                datum.value = .{ .string = string_value };
                parsed_bytes += value_bytes;
                break;
            },
            else => {
                break;
            },
        }
    }

    return .{ .{ .key = key, .value = datum }, parsed_bytes };
}

const KeyValuePair = struct { key: []u8, value: db.Datum };

pub fn parseDatabaseSection(bytes: []const u8, alloc: Allocator) !struct { []KeyValuePair, usize } {
    var marker = @as(MarkerByte, @enumFromInt(bytes[0]));
    std.debug.assert(marker == .DATABASE);
    // NOTE: Simplifying assumption: there is only one database, and we don't care about the number
    marker = @as(MarkerByte, @enumFromInt(bytes[2]));
    std.debug.assert(marker == .HASH_TABLE_SIZE_INFO);
    const key_value_hash_table_size, const key_value_hash_table_size_n_parsed_bytes = try parseSize(bytes[3..]);
    _, const expires_hash_table_size_n_parsed_bytes = try parseSize(bytes[3 + key_value_hash_table_size_n_parsed_bytes ..]);
    var pairs = try alloc.alloc(KeyValuePair, key_value_hash_table_size);
    var cursor: usize = 3 + key_value_hash_table_size_n_parsed_bytes + expires_hash_table_size_n_parsed_bytes;
    for (0..key_value_hash_table_size) |i| {
        const pair, const n_parsed_bytes = try parseKeyValuePair(bytes[cursor..], alloc);
        pairs[i].key = pair.key;
        pairs[i].value = pair.value;
        cursor += n_parsed_bytes;
    }
    return .{ pairs, cursor };
}

pub fn parseData(bytes: []const u8, alloc: Allocator) !struct { []KeyValuePair, usize } {
    var offset = "REDIS0011".len;
    _, const metadata_parsed_btyes = try parseMetadataSection(bytes[offset..], alloc);
    offset += metadata_parsed_btyes;
    var data = std.ArrayList(KeyValuePair).init(alloc);
    if (@as(MarkerByte, @enumFromInt(bytes[offset])) == .DATABASE) {
        const keyval_pairs, const data_parsed_bytes = try parseDatabaseSection(bytes[offset..], alloc);
        defer alloc.free(keyval_pairs);
        try data.appendSlice(keyval_pairs);
        offset += data_parsed_bytes;
    }
    return .{ try data.toOwnedSlice(), offset };
}

// for now it just verifies the dump is valid, in the future we can also make a
// nice struct with methods and such
pub fn parseDump(bytes: []const u8) !usize {
    if (bytes[0] != '$') return error.InvalidDump;

    var i: usize = 1;
    var dump_length: usize = 0;

    while (i < bytes.len - 2 and std.ascii.isDigit(bytes[i])) : (i += 1) {
        dump_length = dump_length * 10 + bytes[i] - '0';
    }

    if (!(bytes[i] == '\r' and bytes[i + 1] == '\n')) return error.InvalidDump;

    i += 2;

    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    defer _ = gpa.deinit();
    var temp_allocator = std.heap.ArenaAllocator.init(gpa.allocator());
    defer temp_allocator.deinit();
    _, const parsed_bytes = try parseData(bytes[i..][0..dump_length], temp_allocator.allocator());
    i += parsed_bytes;
    if (@as(MarkerByte, @enumFromInt(bytes[i])) != .EOF or dump_length - parsed_bytes - 1 != 8)
        return error.InvalidDump;

    return i + 1 + 8;
}

test "parse ascii strings" {
    var allocator = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer allocator.deinit();
    const short_string = "\x0a" ++ "valkey-ver";
    const parsed_short_string, const parsed_bytes1 = try parseString(short_string, allocator.allocator());
    try std.testing.expectEqualStrings(short_string[1..], parsed_short_string);
    try std.testing.expectEqual(11, parsed_bytes1);
    const long_string = "\x42\xbc" ++ "hello" ** 140;
    const parsed_long_string, const parsed_bytes2 = try parseString(long_string, allocator.allocator());
    try std.testing.expectEqualStrings(long_string[2..], parsed_long_string);
    try std.testing.expectEqual(702, parsed_bytes2);
    const long_long_string = "\x80\x00\x00\x42\x68" ++ "hello" ** 3400;
    const parsed_long_long_string, const parsed_bytes3 = try parseString(long_long_string, allocator.allocator());
    try std.testing.expectEqualStrings(long_long_string[5..], parsed_long_long_string);
    try std.testing.expectEqual(17005, parsed_bytes3);
}

test "parse special string-encoded values" {
    var allocator = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer allocator.deinit();
    const short_string = "\xc0\x7b"; // 0x7b = 123
    const parsed_short_string, const parsed_bytes1 = try parseString(short_string, allocator.allocator());
    try std.testing.expectEqualStrings("123", parsed_short_string);
    try std.testing.expectEqual(2, parsed_bytes1);
    const long_string = "\xc1\x39\x30"; // 0x3930 = 12345
    const parsed_long_string, const parsed_bytes2 = try parseString(long_string, allocator.allocator());
    try std.testing.expectEqualStrings("12345", parsed_long_string);
    try std.testing.expectEqual(3, parsed_bytes2);
    const long_long_string = "\xc2\x87\xd6\x12\x00"; // 0x0012d687 = 1234567
    const parsed_long_long_string, const parsed_bytes3 = try parseString(long_long_string, allocator.allocator());
    try std.testing.expectEqualStrings("1234567", parsed_long_long_string);
    try std.testing.expectEqual(5, parsed_bytes3);
}

const dump = [_]u8{
    0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x0a, 0x76, // |REDIS0011..valke|
    0x61, 0x6c, 0x6b, 0x65, 0x79, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x38, 0x2e, // |y-ver.8.0.3..red|
    0x30, 0x2e, 0x33, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, // |is-bits.@..ctime|
    0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, // |...5h..used-mem.|
    0xc2, 0xde, 0x95, 0x35, 0x68, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, // |.o....aof-base..|
    0x6d, 0x65, 0x6d, 0xc2, 0xa0, 0x6f, 0x0f, 0x00, 0xfa, 0x08, 0x61, 0x6f, // |.......apple.red|
    0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xfe, 0x00, 0xfb, 0x03, // |..banana.yellow.|
    0x01, 0x00, 0x05, 0x61, 0x70, 0x70, 0x6c, 0x65, 0x03, 0x72, 0x65, 0x64, // |.0R.......brocco|
    0x00, 0x06, 0x62, 0x61, 0x6e, 0x61, 0x6e, 0x61, 0x06, 0x79, 0x65, 0x6c, // |li.green..._...f|
    0x6c, 0x6f, 0x77, 0xfc, 0xd9, 0x30, 0x52, 0x11, 0x97, 0x01, 0x00, 0x00, // |.|
    0x00, 0x08, 0x62, 0x72, 0x6f, 0x63, 0x63, 0x6f, 0x6c, 0x69, 0x05, 0x67,
    0x72, 0x65, 0x65, 0x6e, 0xff, 0xc3, 0x1a, 0x5f, 0x97, 0xd4, 0xbe, 0x66,
    0x10,
};

test "parse metadata" {
    var allocator = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer allocator.deinit();
    const metadata, const parsed_bytes = try parseMetadataSection(dump["REDIS0011".len..], allocator.allocator());
    try std.testing.expectEqualStrings("8.0.3", metadata.get("valkey-ver").?);
    try std.testing.expectEqualStrings("64", metadata.get("redis-bits").?);
    try std.testing.expectEqualStrings("1748342238", metadata.get("ctime").?);
    try std.testing.expectEqualStrings("0", metadata.get("aof-base").?);
    try std.testing.expectEqual(71, parsed_bytes);
}

test "parse database" {
    var allocator = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer allocator.deinit();
    var offset = "REDIS0011".len;
    _, const metadata_parsed_btyes = try parseMetadataSection(dump[offset..], allocator.allocator());
    offset += metadata_parsed_btyes;
    const data, const data_parsed_bytes = try parseDatabaseSection(dump[offset..], allocator.allocator());
    try std.testing.expectEqualStrings("apple", data[0].key);
    try std.testing.expectEqualStrings("red", data[0].value.value.string);
    try std.testing.expectEqualStrings("banana", data[1].key);
    try std.testing.expectEqualStrings("yellow", data[1].value.value.string);
    try std.testing.expectEqualStrings("broccoli", data[2].key);
    try std.testing.expectEqualStrings("green", data[2].value.value.string);
    try std.testing.expectEqual(1748342288601, data[2].value.expire_at_ms);
    try std.testing.expectEqual(56, data_parsed_bytes);
}
