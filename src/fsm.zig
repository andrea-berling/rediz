const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const Allocator = std.mem.Allocator;
const util = @import("util.zig");
const cmd = @import("command.zig");

pub const CLIENT_BUFFER_SIZE = 1024;
pub const NOTIFICATION_BUFFER_SIZE = 8;

pub const Server = struct {
    socket: posix.socket_t,
    state: union(enum) { waiting, creating_connection, shutting_down },
};

pub const Connection = struct {
    fd: posix.socket_t,
    buffer: []u8,
    commands_to_execute: std.DoublyLinkedList(cmd.Command),
    new_commands_notification_fd: posix.fd_t,
    notification_val: u64,
    peer_type: enum {
        SLAVE,
        MASTER,
        GENERIC_CLIENT,
    } = .GENERIC_CLIENT,
    state: enum {
        in_sync,
        sending_response,
        waiting_for_commands,
        executing_commands,
        sending_dump,
        propagating_command,
        waiting_for_getack_to_be_sent_or_timeout,
        processing_slave_ack_response,
        processing_timeout_response,
    } = .waiting_for_commands,
    allocator: ?Allocator = null,

    const Self = @This();

    pub fn init(allocator: Allocator, fd: posix.socket_t) !Self {
        return Self{
            .fd = fd,
            .buffer = try allocator.alloc(u8, CLIENT_BUFFER_SIZE),
            .allocator = allocator,
            .commands_to_execute = std.DoublyLinkedList(cmd.Command){},
            .new_commands_notification_fd = try posix.eventfd(0, linux.EFD.SEMAPHORE),
            .notification_val = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        posix.close(self.fd);
        const maybe_allocator = self.allocator;
        if (maybe_allocator) |allocator| {
            util.resizeBuffer(&self.buffer, CLIENT_BUFFER_SIZE);
            allocator.free(self.buffer);
        }
    }
};

pub const FSM = struct { type: union(enum) {
    server: Server,
    connection: Connection,
}, global_data: *GlobalData };

pub const GlobalData = struct {
    slaves_repl_offsets: std.AutoHashMap(posix.socket_t, usize),
    slaves: std.AutoHashMap(*FSM, void),
    pending_waits: std.PriorityQueue(*PendingWait, void, PendingWait.order),
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return Self{ .slaves_repl_offsets = std.AutoHashMap(posix.socket_t, usize).init(allocator), .pending_waits = std.PriorityQueue(*PendingWait, void, PendingWait.order).init(allocator, undefined), .slaves = std.AutoHashMap(*FSM, void).init(allocator), .allocator = allocator };
    }

    pub fn deinit(self: *Self) void {
        self.slaves_repl_offsets.deinit();
        self.slaves.deinit();
        self.pending_waits.deinit();
    }
};

pub const PendingWait = struct {
    timeout: i64,
    client_connection_fsm: *FSM,
    threshold_offset: usize,
    expected_n_replicas: usize,
    actual_n_replicas: usize,

    fn order(context: void, a: *PendingWait, b: *PendingWait) std.math.Order {
        _ = context;
        return std.math.order(a.timeout, b.timeout);
    }
};
