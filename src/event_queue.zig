const std = @import("std");
const linux = std.os.linux;
const posix = std.posix;

fn handleError(errno: i64) !void {
    var error_string = std.ArrayList(u8).init(std.heap.page_allocator);
    try std.fmt.format(error_string.writer(), "Errno: {d}. Error: {any}", .{errno, linux.E.init(@bitCast(errno))});
    @panic(try error_string.toOwnedSlice());
}

fn ioUringSetup(entries: u32, p: *linux.io_uring_params) !linux.fd_t {
    const fd: i64 = @bitCast(linux.io_uring_setup(entries,p));
    if ( fd < 0 ) {
        try handleError(fd);
    }
    return @truncate(fd);
}

fn ioUringEnter(fd: i32, to_submit: u32, min_complete: u32, flags: u32, sig: ?*linux.sigset_t) !usize {
    const consumed_io_ops: i64 = @bitCast(linux.io_uring_enter(fd, to_submit, min_complete, flags, sig));

    if (consumed_io_ops < 0) {
        try handleError(fd);
    }
    return @bitCast(consumed_io_ops);
}

pub const EVENT_TYPE = enum {
    CONNECTION,
    RECEIVE_COMMAND,
    SENT_RESPONSE,
    SIGTERM
};

pub const Event = struct {
    ty: EVENT_TYPE = EVENT_TYPE.CONNECTION,
    fd: posix.socket_t,
    buffer: ?[]u8 = null,
    async_result: ?i32 = null,
};

const SQRing = struct {
    array: []c_uint,
    head: *c_uint,
    tail: *c_uint,
    dropped: *c_uint,
    ring_mask: *c_uint,
    sqes: []linux.io_uring_sqe,
};

const CQRing = struct {
    head: *c_uint,
    tail: *c_uint,
    ring_mask: *c_uint,
    cqes: []linux.io_uring_cqe,
};

pub const EventQueue = struct {
    allocator: std.mem.Allocator,
    io_uring_fd : linux.fd_t,
    params: *linux.io_uring_params,
    sqring : SQRing,
    cqring : CQRing,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, entries: u32) !EventQueue {

        var params = try allocator.create(linux.io_uring_params);
        params.flags = linux.IORING_SETUP_SINGLE_ISSUER;
        @memset(&params.resv, 0);
        const io_uring_fd = try ioUringSetup(entries,params);


        const sqring_ptr = try posix.mmap(null, params.sq_off.array + params.sq_entries * @sizeOf(u32),
                           linux.PROT.READ|linux.PROT.WRITE, .{ .TYPE = linux.MAP_TYPE.SHARED, .POPULATE = true},
                           io_uring_fd, linux.IORING_OFF_SQ_RING);

        const sqes = try posix.mmap(null, params.sq_entries * @sizeOf(linux.io_uring_sqe),
                            linux.PROT.READ|linux.PROT.WRITE, .{ .TYPE = linux.MAP_TYPE.SHARED, .POPULATE = true},
                            io_uring_fd, linux.IORING_OFF_SQES);

        const sqring: SQRing = .{
            .array = @as([*]c_uint,@alignCast(@ptrCast(&sqring_ptr[params.sq_off.array])))[0..params.sq_entries],
            .head = @alignCast(@ptrCast(&sqring_ptr.ptr[params.sq_off.head])),
            .tail = @alignCast(@ptrCast(&sqring_ptr.ptr[params.sq_off.tail])),
            .dropped = @alignCast(@ptrCast(&sqring_ptr.ptr[params.sq_off.dropped])),
            .sqes = @as([*]linux.io_uring_sqe,@alignCast(@ptrCast(sqes)))[0..params.sq_entries],
            .ring_mask = @alignCast(@ptrCast(&sqring_ptr.ptr[params.sq_off.ring_mask])),
        };

        const cqring_ptr = try posix.mmap(null, params.cq_off.cqes + params.cq_entries * @sizeOf(linux.io_uring_cqe),
                           linux.PROT.READ|linux.PROT.WRITE, .{ .TYPE = linux.MAP_TYPE.SHARED, .POPULATE = true}, io_uring_fd,
                           linux.IORING_OFF_CQ_RING);

        const cqring: CQRing = .{
            .head = @alignCast(@ptrCast(&cqring_ptr.ptr[params.cq_off.head])),
            .tail = @alignCast(@ptrCast(&cqring_ptr.ptr[params.cq_off.tail])),
            .ring_mask = @alignCast(@ptrCast(&cqring_ptr.ptr[params.cq_off.ring_mask])),
            .cqes = @as([*]linux.io_uring_cqe,@alignCast(@ptrCast(&cqring_ptr.ptr[params.cq_off.cqes])))[0..params.cq_entries],
        };

        return .{
            .allocator = allocator,
            .io_uring_fd = io_uring_fd,
            .params = params,
            .sqring = sqring,
            .cqring = cqring,
        };
    }

    pub fn addAsyncEvent(self: *Self, event: *const Event) !void {
        const tail = self.sqring.tail.*;
        const index = tail & (self.sqring.ring_mask.*);
        const sqe = &self.sqring.sqes[index];
        switch (event.ty) {
            .CONNECTION => {
                sqe.prep_accept(event.fd, null, null, 0);
            },
            .RECEIVE_COMMAND => {
                sqe.prep_recv(event.fd, event.buffer.?, 0);
            },
            .SENT_RESPONSE => {
                sqe.prep_send(event.fd, event.buffer.?, 0);
            },
            .SIGTERM => {
                sqe.prep_poll_add(event.fd, posix.POLL.IN);
            }
        }
        sqe.user_data = @intFromPtr(event);
        self.sqring.array[index] = @intCast(index);
        @atomicStore(c_uint, self.sqring.tail, tail + 1, std.builtin.AtomicOrder.release);
        _ = try ioUringEnter(self.io_uring_fd, 1, 0, 0, null);
    }

    pub fn cancelAllPendingOps(self: *Self) !void {
        const tail = self.sqring.tail.*;
        const index = tail & (self.sqring.ring_mask.*);
        const sqe = &self.sqring.sqes[index];
        sqe.prep_cancel(0, linux.IORING_ASYNC_CANCEL_ANY);
        self.sqring.array[index] = @intCast(index);
        @atomicStore(c_uint, self.sqring.tail, tail + 1, std.builtin.AtomicOrder.release);
        _ = try ioUringEnter(self.io_uring_fd, 1, 1, linux.IORING_ENTER_GETEVENTS, null);
    }


    pub fn next(self: *Self) !*Event {
        const head = self.cqring.head.*;
        const tail = @atomicLoad(c_uint,self.cqring.tail,std.builtin.AtomicOrder.acquire);
        if (head == tail) {
            _  = try ioUringEnter(self.io_uring_fd, 0, 1, linux.IORING_ENTER_GETEVENTS, null);
        }
        const index = head & (self.cqring.ring_mask.*);
        const cqe = &self.cqring.cqes[index];
        const event: *Event = @ptrFromInt(cqe.user_data);
        event.async_result = cqe.res;
        @atomicStore(c_uint, self.cqring.head, head + 1, std.builtin.AtomicOrder.release);
        return event;
    }

    pub fn destroy(self: *Self) !void {
        try self.cancelAllPendingOps();
        self.allocator.destroy(self.params);
        posix.close(self.io_uring_fd);
    }
};

