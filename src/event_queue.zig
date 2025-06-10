const std = @import("std");
const linux = std.os.linux;
const posix = std.posix;
const root = @import("root");

fn handleError(errno: i64) !void {
    var error_string = std.ArrayList(u8).init(std.heap.page_allocator);
    try std.fmt.format(error_string.writer(), "Errno: {d}. Error: {any}", .{ errno, linux.E.init(@bitCast(errno)) });
    @panic(try error_string.toOwnedSlice());
}

fn ioUringSetup(entries: u32, p: *linux.io_uring_params) !linux.fd_t {
    const fd: i64 = @bitCast(linux.io_uring_setup(entries, p));
    if (fd < 0) {
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

pub fn Event(comptime T: type) type {
    return struct {
        type: union(enum) {
            send: EventWithBuffer,
            recv: EventWithBuffer,
            read: EventWithBuffer,
            notify: struct { fd: posix.fd_t, n: u64 = 1 },
            accept: posix.socket_t,
            pollin: posix.socket_t,
        },
        user_data: *T,
    };
}

pub const EventWithBuffer = struct {
    fd: posix.socket_t,
    buffer: []u8,
};

pub fn CompletedEvent(comptime T: type) type {
    return struct {
        async_result: i32,
        awaited_event: Event(T),

        pub fn format(value: @This(), comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
            _ = fmt;
            switch (value.awaited_event.type) {
                .send => |ev| {
                    try writer.writeAll("send on fd ");
                    try std.fmt.formatInt(ev.fd, 10, .lower, options, writer);
                },
                .recv => |ev| {
                    try writer.writeAll("recv on fd ");
                    try std.fmt.formatInt(ev.fd, 10, .lower, options, writer);
                },
                .read => |ev| {
                    try writer.writeAll("read on fd ");
                    try std.fmt.formatInt(ev.fd, 10, .lower, options, writer);
                },
                .notify => |notification| {
                    try std.fmt.format(writer, "notify +{} on fd {}", .{ notification.n, notification.fd });
                },
                .accept => |fd| {
                    try writer.writeAll("accept on fd ");
                    try std.fmt.formatInt(fd, 10, .lower, options, writer);
                },
                .pollin => |fd| {
                    try writer.writeAll("pollin on fd ");
                    try std.fmt.formatInt(fd, 10, .lower, options, writer);
                },
            }
            try writer.writeAll(" with result ");
            try std.fmt.formatInt(value.async_result, 10, .lower, options, writer);
        }
    };
}

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

pub fn EventQueue(comptime T: type) type {
    return struct {
        allocator: std.mem.Allocator,
        io_uring_fd: linux.fd_t,
        params: *linux.io_uring_params,
        sqring: SQRing,
        cqring: CQRing,
        pending_events: std.AutoHashMap(*const Event(T), void),
        // Goddamn write to eventfd needing a buffer
        notification_values: [10]u64,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, entries: u32) !EventQueue(T) {
            var params = try allocator.create(linux.io_uring_params);
            params.flags = linux.IORING_SETUP_SINGLE_ISSUER;
            @memset(&params.resv, 0);
            const io_uring_fd = try ioUringSetup(entries, params);

            const sqring_ptr = try posix.mmap(null, params.sq_off.array + params.sq_entries * @sizeOf(u32), linux.PROT.READ | linux.PROT.WRITE, .{ .TYPE = linux.MAP_TYPE.SHARED, .POPULATE = true }, io_uring_fd, linux.IORING_OFF_SQ_RING);

            const sqes = try posix.mmap(null, params.sq_entries * @sizeOf(linux.io_uring_sqe), linux.PROT.READ | linux.PROT.WRITE, .{ .TYPE = linux.MAP_TYPE.SHARED, .POPULATE = true }, io_uring_fd, linux.IORING_OFF_SQES);

            const sqring: SQRing = .{
                .array = @as([*]c_uint, @alignCast(@ptrCast(&sqring_ptr[params.sq_off.array])))[0..params.sq_entries],
                .head = @alignCast(@ptrCast(&sqring_ptr.ptr[params.sq_off.head])),
                .tail = @alignCast(@ptrCast(&sqring_ptr.ptr[params.sq_off.tail])),
                .dropped = @alignCast(@ptrCast(&sqring_ptr.ptr[params.sq_off.dropped])),
                .sqes = @as([*]linux.io_uring_sqe, @alignCast(@ptrCast(sqes)))[0..params.sq_entries],
                .ring_mask = @alignCast(@ptrCast(&sqring_ptr.ptr[params.sq_off.ring_mask])),
            };

            const cqring_ptr = try posix.mmap(null, params.cq_off.cqes + params.cq_entries * @sizeOf(linux.io_uring_cqe), linux.PROT.READ | linux.PROT.WRITE, .{ .TYPE = linux.MAP_TYPE.SHARED, .POPULATE = true }, io_uring_fd, linux.IORING_OFF_CQ_RING);

            const cqring: CQRing = .{
                .head = @alignCast(@ptrCast(&cqring_ptr.ptr[params.cq_off.head])),
                .tail = @alignCast(@ptrCast(&cqring_ptr.ptr[params.cq_off.tail])),
                .ring_mask = @alignCast(@ptrCast(&cqring_ptr.ptr[params.cq_off.ring_mask])),
                .cqes = @as([*]linux.io_uring_cqe, @alignCast(@ptrCast(&cqring_ptr.ptr[params.cq_off.cqes])))[0..params.cq_entries],
            };

            return .{ .allocator = allocator, .io_uring_fd = io_uring_fd, .params = params, .sqring = sqring, .cqring = cqring, .pending_events = std.AutoHashMap(*const Event(T), void).init(allocator), .notification_values = .{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 } };
        }

        pub fn addAsyncEvent(self: *Self, event: Event(T)) !void {
            const tail = self.sqring.tail.*;
            const index = tail & (self.sqring.ring_mask.*);
            const sqe = &self.sqring.sqes[index];
            switch (event.type) {
                .recv => |recv_event| {
                    sqe.prep_recv(recv_event.fd, recv_event.buffer, 0);
                },
                .send => |send_event| {
                    sqe.prep_send(send_event.fd, send_event.buffer, 0);
                },
                .read => |read_event| {
                    sqe.prep_read(read_event.fd, read_event.buffer, 0);
                },
                .notify => |notification| {
                    sqe.prep_write(notification.fd, std.mem.asBytes(&self.notification_values[notification.n]), 0);
                },
                .accept => |fd| {
                    sqe.prep_accept(fd, null, null, 0);
                },
                .pollin => |fd| {
                    sqe.prep_poll_add(fd, posix.POLL.IN);
                },
            }
            const new_event = try self.allocator.create(Event(T));
            new_event.* = event;
            sqe.user_data = @intFromPtr(new_event);
            try self.pending_events.put(new_event, undefined);
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

        pub fn next(self: *Self) !CompletedEvent(T) {
            const head = self.cqring.head.*;
            const tail = @atomicLoad(c_uint, self.cqring.tail, std.builtin.AtomicOrder.acquire);
            if (head == tail) {
                _ = try ioUringEnter(self.io_uring_fd, 0, 1, linux.IORING_ENTER_GETEVENTS, null);
            }
            const index = head & (self.cqring.ring_mask.*);
            const cqe = &self.cqring.cqes[index];
            const input_event: *Event(T) = @ptrFromInt(cqe.user_data);
            const completed_event = CompletedEvent(T){
                .async_result = cqe.res,
                .awaited_event = input_event.*,
            };
            _ = self.pending_events.remove(input_event);
            self.allocator.destroy(input_event);
            @atomicStore(c_uint, self.cqring.head, head + 1, std.builtin.AtomicOrder.release);

            return completed_event;
        }

        pub fn destroy(self: *Self, opts: struct { user_data_destroyer: ?fn (*T) void = null }) !void {
            try self.cancelAllPendingOps();

            var temp_allocator = std.heap.ArenaAllocator.init(self.allocator);
            defer temp_allocator.deinit();

            var pending_events_it = self.pending_events.keyIterator();
            var already_destroyed_user_data = std.AutoHashMap(*const T, void).init(temp_allocator.allocator());

            while (pending_events_it.next()) |pending_event| {
                if (opts.user_data_destroyer) |destroyer| {
                    if (!already_destroyed_user_data.contains(pending_event.*.user_data)) {
                        destroyer(pending_event.*.user_data);
                        try already_destroyed_user_data.put(pending_event.*.user_data, {});
                    }
                }
                self.allocator.destroy(pending_event.*);
            }
            self.pending_events.deinit();
            self.allocator.destroy(self.params);
            posix.close(self.io_uring_fd);
        }

        pub fn removePendingEvents(self: *Self, user_data: *const T) !void {
            var event_it = self.pending_events.keyIterator();
            var temp_allocator = std.heap.ArenaAllocator.init(self.allocator);
            defer temp_allocator.deinit();
            var events_to_remove = std.ArrayList(*const Event(T)).init(temp_allocator.allocator());
            while (event_it.next()) |event| {
                if (event.*.user_data == user_data) {
                    try events_to_remove.append(event.*);
                }
            }

            for (events_to_remove.items) |event| {
                _ = self.pending_events.remove(event);
            }
        }
    };
}
