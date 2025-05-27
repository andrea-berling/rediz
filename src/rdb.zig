const std = @import("std");
const Allocator = std.mem.Allocator;

const METADATA_SUBSECTION_MARKER = 0xfa;
const DATABASE_SUBSECTION_MARKER = 0xfe;
const EOF_SECTION_MARKER = 0xff;

pub fn parse_string(bytes: []const u8, alloc: Allocator) !struct { []const u8, usize } {
    const marker = bytes[0] >> 6;
    switch ( marker ) {
        0b00 => { // 1 byte len + ASCII
            const length: u8 = @bitCast(bytes[0]);
            return .{ bytes[1..][0..length], length + 1 };
        },
        0b01 => { // 2 bytes len + ASCII
            const length: u16 = @bitCast((@as(u16,(bytes[0] & 0x3f)) << 8) | bytes[1]);
            return .{ bytes[2..][0..length], length + 2 };
        },
        0b10 => { // 4 bytes len + ASCII (skip the first)
            var length: usize = 0;
            inline for (0..@sizeOf(u32)) |i| {
                length = (length << 8) | bytes[i+1];
            }
            return .{ bytes[5..][0..length], length + 5 };
        },
        0b11 => {
            if (bytes[0] & 0x3f == 0xc3) { // LZF-compressed string
                @panic("LZF-compressed string are not supported");
            }
            const byte_length: u8 = @as(u8,1) << @as(u2,@truncate(bytes[0] & 0x0f));
            var value: u32 = 0;
            for (0..byte_length) |i| {
                value = value | (@as(u32,bytes[i+1]) << @as(u5,@truncate(8*@as(u8,@truncate(i)))));
            }
            var new_string = std.ArrayList(u8).init(alloc);
            try std.fmt.format(new_string.writer(), "{d}", .{value});
            return .{ try new_string.toOwnedSlice(), 1 + byte_length };
        },
        else => { unreachable; }
    }
}

pub fn parse_metadata_section(bytes: []const u8, alloc: Allocator) !struct { std.StringHashMap([]u8), usize } {
    std.debug.assert(bytes[0] == METADATA_SUBSECTION_MARKER);
    var i: usize = 0;
    var attributes = std.StringHashMap([]u8).init(alloc);
    while (bytes[i] == METADATA_SUBSECTION_MARKER) {
        i += 1;
        const key, const key_bytes = try parse_string(bytes[i..], alloc);
        i += key_bytes;
        const value, const value_bytes = try parse_string(bytes[i..], alloc);
        try attributes.put(key, @constCast(value));
        i += value_bytes;
    }
    return .{ attributes, i };
}

test "parse ascii strings" {
    var allocator = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer allocator.deinit();
    const short_string = "\x0a" ++ "valkey-ver";
    const parsed_short_string, const parsed_bytes1 = try parse_string(short_string[0..], allocator.allocator());
    try std.testing.expectEqualStrings(short_string[1..], parsed_short_string);
    try std.testing.expectEqual(11, parsed_bytes1);
    const long_string = "\x42\xbc" ++ "hello" ** 140;
    const parsed_long_string, const parsed_bytes2 = try parse_string(long_string[0..], allocator.allocator());
    try std.testing.expectEqualStrings(long_string[2..], parsed_long_string);
    try std.testing.expectEqual(702, parsed_bytes2);
    const long_long_string = "\x80\x00\x00\x42\x68" ++ "hello" ** 3400;
    const parsed_long_long_string, const parsed_bytes3 = try parse_string(long_long_string[0..], allocator.allocator());
    try std.testing.expectEqualStrings(long_long_string[5..], parsed_long_long_string);
    try std.testing.expectEqual(17005, parsed_bytes3);
}

test "parse special string-encoded values" {
    var allocator = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer allocator.deinit();
    const short_string = "\xc0\x7b"; // 0x7b = 123
    const parsed_short_string, const parsed_bytes1 = try parse_string(short_string[0..], allocator.allocator());
    try std.testing.expectEqualStrings("123", parsed_short_string);
    try std.testing.expectEqual(2, parsed_bytes1);
    const long_string = "\xc1\x39\x30"; // 0x3930 = 12345
    const parsed_long_string, const parsed_bytes2 = try parse_string(long_string[0..], allocator.allocator());
    try std.testing.expectEqualStrings("12345", parsed_long_string);
    try std.testing.expectEqual(3, parsed_bytes2);
    const long_long_string = "\xc2\x87\xd6\x12\x00"; // 0x0012d687 = 1234567
    const parsed_long_long_string, const parsed_bytes3 = try parse_string(long_long_string[0..], allocator.allocator());
    try std.testing.expectEqualStrings("1234567", parsed_long_long_string);
    try std.testing.expectEqual(5, parsed_bytes3);
}

const dump = [_]u8{
    0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x0a, 0x76,     // |REDIS0011..valke|
    0x61, 0x6c, 0x6b, 0x65, 0x79, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x38, 0x2e,     // |y-ver.8.0.3..red|
    0x30, 0x2e, 0x33, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62,     // |is-bits.@..ctime|
    0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65,     // |...4h..used-mem.|
    0xc2, 0xfc, 0xb4, 0x34, 0x68, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d,     // |.m....aof-base..|
    0x6d, 0x65, 0x6d, 0xc2, 0xd0, 0x6d, 0x0f, 0x00, 0xfa, 0x08, 0x61, 0x6f,     // |.......broccoli.|
    0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xfe, 0x00, 0xfb, 0x03,     // |green..apple.red|
    0x00, 0x00, 0x08, 0x62, 0x72, 0x6f, 0x63, 0x63, 0x6f, 0x6c, 0x69, 0x05,     // |..banana.yellow.|
    0x67, 0x72, 0x65, 0x65, 0x6e, 0x00, 0x05, 0x61, 0x70, 0x70, 0x6c, 0x65,     // |..+.1...|
    0x03, 0x72, 0x65, 0x64, 0x00, 0x06, 0x62, 0x61, 0x6e, 0x61, 0x6e, 0x61,
    0x06, 0x79, 0x65, 0x6c, 0x6c, 0x6f, 0x77, 0xff, 0xe6, 0xd8, 0x2b, 0x90,
    0x31, 0x09, 0x07, 0xb0
};

test "parse metadata" {
    var allocator = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer allocator.deinit();
    const metadata, const parsed_bytes = try parse_metadata_section(dump[("REDIS0011"[0..]).len..], allocator.allocator());
    try std.testing.expectEqualStrings(metadata.get("valkey-ver").?, "8.0.3");
    try std.testing.expectEqualStrings(metadata.get("redis-bits").?, "64");
    try std.testing.expectEqualStrings(metadata.get("ctime").?, "1748284668");
    try std.testing.expectEqualStrings(metadata.get("aof-base").?, "0");
    try std.testing.expectEqual(71,parsed_bytes);
}
