const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;
const util = @import("util.zig");

pub const CLIENT_BUFFER_SIZE = 1024;

pub const Event = struct {
    fsm: *FSM,
    //type: union(enum) {
    //    connection_accepted: struct {
    //        server_socket: posix.socket_t,
    //    },
    //    connection_closed: posix.socket_t,
    //    received_command: NetworkIOEvent,
    //    sent_response: NetworkIOEvent,
    //    sent_command: NetworkIOEvent,
    //    received_response: NetworkIOEvent,
    //    propagated_command: NetworkIOEvent,
    //    sent_dump: NetworkIOEvent,
    //    received_fullsync_request: NetworkIOEvent,
    //    received_signal,
    //    sent_getack_request: NetworkIOEvent,
    //    received_getack_reponse: NetworkIOEvent,
    //    pending_wait_timeout,
    //}
};

pub const NetworkIOEvent = struct {
    peer_socket: posix.socket_t,
    buffer: []u8,
};

pub const Server = struct {
    socket: posix.socket_t,
    state: union(enum) { waiting, creating_connection, shutting_down },
};

pub const Connection = struct {
    fd: posix.socket_t,
    buffer: []u8,
    peer_type: enum {
        SLAVE,
        MASTER,
        GENERIC_CLIENT,
    } = .GENERIC_CLIENT,
    state: union(enum) {
        sending_response,
        waiting_for_command,
        executing_command,
        waiting_for_response_to_be_sent,
        waiting_for_getack_to_be_sent_or_timeout,
        processing_slave_ack_response,
        processing_timeout_response,
    } = .waiting_for_command,
    allocator: ?Allocator = null,

    const Self = @This();

    pub fn init(allocator: Allocator, fd: posix.socket_t) !Self {
        return Self{
            .fd = fd,
            .buffer = try allocator.alloc(u8, CLIENT_BUFFER_SIZE),
            .allocator = allocator,
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
    pending_waits: std.PriorityQueue(*PendingWait, void, PendingWait.order),
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return Self{ .slaves_repl_offsets = std.AutoHashMap(posix.socket_t, usize).init(allocator), .pending_waits = std.PriorityQueue(*PendingWait, void, PendingWait.order).init(allocator, undefined), .allocator = allocator };
    }

    pub fn deinit(self: *Self) void {
        self.slaves_repl_offsets.deinit();
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
