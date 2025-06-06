pub inline fn resizeBuffer(buffer: *[]u8, new_size: usize) void {
    buffer.* = (@as([*]u8, @ptrCast(buffer.ptr)))[0..new_size];
}
