const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const Allocator = std.mem.Allocator;
const util = @import("util.zig");
const cmd = @import("command.zig");

pub const CLIENT_BUFFER_SIZE = 1024;

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
    peer_type: union(enum) {
        slave: struct { repl_offset: usize },
        master,
        generic_client,
    } = .generic_client,
    state: union(enum) { in_sync, sending_response, waiting_for_commands, executing_commands, sending_dump, propagating_command, sending_getack, waiting_for_ack, blocked: *PendingWait, waiting_for_new_data_on_stream: *BlockedStreamRead } = .waiting_for_commands,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, fd: posix.socket_t, peer_type: @FieldType(Connection, "peer_type")) !Self {
        return Self{ .fd = fd, .buffer = try allocator.alloc(u8, CLIENT_BUFFER_SIZE), .commands_to_execute = std.DoublyLinkedList(cmd.Command){}, .new_commands_notification_fd = try posix.eventfd(0, linux.EFD.SEMAPHORE), .notification_val = 0, .peer_type = peer_type, .allocator = allocator };
    }

    pub fn addCommand(self: *Self, command: cmd.Command) !void {
        var new_command = try self.allocator.create(std.DoublyLinkedList(cmd.Command).Node);
        new_command.data = command;
        self.commands_to_execute.append(new_command);
    }

    pub fn popCommand(self: *Self) ?cmd.Command {
        if (self.hasCommands()) {
            const node = self.commands_to_execute.popFirst().?;
            const command = node.data;
            self.allocator.destroy(node);
            return command;
        } else {
            return null;
        }
    }

    pub inline fn hasCommands(self: *Self) bool {
        return self.commands_to_execute.len > 0;
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        posix.close(self.fd);
        util.resizeBuffer(&self.buffer, CLIENT_BUFFER_SIZE);
        allocator.free(self.buffer);
    }
};

pub const FSM = struct {
    type: union(enum) {
        server: Server,
        connection: Connection,
    },
    global_data: *GlobalData,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn newServer(allocator: std.mem.Allocator, server_socket: posix.socket_t, global_data: *GlobalData) !*Self {
        const server_fsm = try allocator.create(FSM);
        server_fsm.* = FSM{ .global_data = global_data, .type = .{
            .server = Server{
                .socket = server_socket,
                .state = .waiting,
            },
        }, .allocator = allocator };
        return server_fsm;
    }

    pub fn newConnection(allocator: std.mem.Allocator, peer_socket: posix.socket_t, peer_type: @FieldType(Connection, "peer_type"), global_data: *GlobalData) !*Self {
        const connection_fsm = try allocator.create(FSM);
        connection_fsm.* = FSM{ .global_data = global_data, .type = .{ .connection = try Connection.init(allocator, peer_socket, peer_type) }, .allocator = allocator };
        return connection_fsm;
    }

    pub fn deinit(self: *Self) void {
        switch (self.type) {
            .server => {},
            .connection => |*fsm| {
                fsm.deinit(self.allocator);
            },
        }
        self.allocator.destroy(self);
    }
};

pub const GlobalData = struct {
    slaves: std.AutoHashMap(*FSM, void),
    pending_waits: std.PriorityQueue(*PendingWait, void, PendingWait.order),
    blocked_xreads: std.StringHashMap(*std.AutoArrayHashMap(*BlockedStreamRead, void)),
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return Self{ .pending_waits = std.PriorityQueue(*PendingWait, void, PendingWait.order).init(allocator, undefined), .blocked_xreads = std.StringHashMap(*std.AutoArrayHashMap(*BlockedStreamRead, void)).init(allocator), .slaves = std.AutoHashMap(*FSM, void).init(allocator), .allocator = allocator };
    }

    pub fn deinit(self: *Self) void {
        self.slaves.deinit();
        self.pending_waits.deinit();
        self.blocked_xreads.deinit();
    }
};

pub const PendingWait = struct {
    timeout: i64,
    client_connection_fsm: *FSM,
    threshold_offset: usize,
    expected_n_replicas: usize,
    actual_n_replicas: usize,
    timerfd: posix.fd_t,

    pub fn order(context: void, a: *PendingWait, b: *PendingWait) std.math.Order {
        _ = context;
        return std.math.order(a.timeout, b.timeout);
    }
};

pub const BlockedStreamRead = struct {
    client_connection_fsm: *FSM,
    streams: std.ArrayList([]const u8),
    timerfd: ?posix.fd_t,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, client_connection_fsm: *FSM, timerfd: ?posix.fd_t) Self {
        std.debug.assert(client_connection_fsm.type == .connection);
        return Self{
            .client_connection_fsm = client_connection_fsm,
            .streams = std.ArrayList([]const u8).init(allocator),
            .timerfd = timerfd,
            .allocator = allocator,
        };
    }

    pub fn addStream(self: *Self, stream: []const u8) !void {
        try self.streams.append(try self.allocator.dupe(u8, stream));
    }

    pub fn deinit(self: *Self) void {
        for (self.streams.items) |stream| {
            self.allocator.free(stream);
        }
        self.streams.deinit();
    }
};
