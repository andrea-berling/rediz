const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;

pub inline fn resizeBuffer(buffer: *[]u8, new_size: usize) void {
    buffer.* = (@as([*]u8, @ptrCast(buffer.ptr)))[0..new_size];
}

pub fn setupTimer(timeout_ms: usize) !posix.fd_t {
    const timeout_fd = try posix.timerfd_create(posix.timerfd_clockid_t.REALTIME, linux.TFD{});

    const time_to_wait = @as(isize, @bitCast(timeout_ms)) * 1_000_000;
    const timeout = linux.itimerspec{
        .it_interval = linux.timespec{
            .sec = 0,
            .nsec = 0,
        },
        .it_value = linux.timespec{ .sec = @divFloor(time_to_wait, 1_000_000_000), .nsec = @rem(time_to_wait, 1_000_000_000) },
    };

    try posix.timerfd_settime(timeout_fd, linux.TFD.TIMER{}, &timeout, null);

    return timeout_fd;
}
