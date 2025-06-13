const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const Allocator = std.mem.Allocator;
const util = @import("util.zig");
const cmd = @import("command.zig");
const eq = @import("event_queue.zig");
const resp = @import("resp.zig");

pub const EventQueue = eq.EventQueue(FSM);
pub const Event = eq.Event(FSM);

pub const CLIENT_BUFFER_SIZE = 1024;

pub const Server = struct {
    state: union(enum) { waiting, creating_connection, shutting_down },
    listener: std.net.Server,
    socket: posix.socket_t,
    signalfd: posix.fd_t,
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
    state: union(enum) { in_sync, sending_response, waiting_for_commands, executing_commands, sending_dump, propagating_command, sending_getack, waiting_for_ack, blocked: *PendingWait, waiting_for_new_data_on_stream: *BlockedStreamRead, executing_transaction } = .waiting_for_commands,
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

    pub fn flushCommandsQueue(self: *Self) void {
        while (self.popCommand()) |*command| {
            @constCast(command).deinit();
        }
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

    pub fn newServer(allocator: std.mem.Allocator, address: []const u8, port: u16) !*Self {
        const server_fsm = try allocator.create(FSM);
        const ip_address = try std.net.Address.resolveIp(address, port);
        const listener = try ip_address.listen(.{
            .reuse_address = true,
        });

        // INFO: signalfd set up to catch signals (SIGINT & SIGTERM)
        var sigset = posix.empty_sigset;
        linux.sigaddset(&sigset, posix.SIG.TERM);
        linux.sigaddset(&sigset, posix.SIG.INT);
        posix.sigprocmask(posix.SIG.BLOCK, &sigset, null);
        const sfd = try posix.signalfd(-1, &sigset, linux.SFD.NONBLOCK | linux.SFD.CLOEXEC);

        const global_data = try allocator.create(GlobalData);
        global_data.* = GlobalData.init(allocator);

        server_fsm.* = FSM{ .global_data = global_data, .type = .{
            .server = Server{ .state = .waiting, .listener = listener, .socket = listener.stream.handle, .signalfd = sfd },
        }, .allocator = allocator };
        return server_fsm;
    }

    pub fn newConnection(self: *FSM, peer_socket: posix.socket_t, peer_type: @FieldType(Connection, "peer_type")) !*Self {
        const connection_fsm = try self.allocator.create(FSM);
        connection_fsm.* = FSM{ .global_data = self.global_data, .type = .{ .connection = try Connection.init(self.allocator, peer_socket, peer_type) }, .allocator = self.allocator };
        return connection_fsm;
    }

    pub fn waitForCommand(self: *FSM, event_queue: *EventQueue, new_state: struct { new_state: @TypeOf(self.type.connection.state) = .waiting_for_commands }) !void {
        if (self.type != .connection)
            return error.InvalidFSM;
        const connection_fsm = &self.type.connection;
        util.resizeBuffer(&connection_fsm.buffer, CLIENT_BUFFER_SIZE);
        @memset(connection_fsm.buffer, 0);
        connection_fsm.state = new_state.new_state;
        const new_command_event = Event{
            .type = .{ .recv = .{ .fd = connection_fsm.fd, .buffer = connection_fsm.buffer } },
            .user_data = self,
        };

        try event_queue.addAsyncEvent(new_command_event);
    }

    pub fn respondWith(self: *FSM, response: []const u8, event_queue: *EventQueue, new_state: struct { new_state: @TypeOf(self.type.connection.state) = .sending_response }) !void {
        if (self.type != .connection)
            return error.InvalidFSM;
        const connection_fsm = &self.type.connection;
        util.resizeBuffer(&connection_fsm.buffer, CLIENT_BUFFER_SIZE);
        connection_fsm.buffer = try std.fmt.bufPrint(connection_fsm.buffer, "{s}", .{response});
        connection_fsm.state = new_state.new_state;
        const response_sent_event = Event{
            .type = .{ .send = .{ .fd = connection_fsm.fd, .buffer = connection_fsm.buffer } },
            .user_data = self,
        };
        try event_queue.addAsyncEvent(response_sent_event);
    }

    pub fn notify(self: *FSM, event_queue: *EventQueue, n_new_commands: usize) !void {
        std.debug.assert(self.type == .connection);
        if (n_new_commands > 0) {
            try event_queue.addAsyncEvent(Event{
                .type = .{ .notify = .{ .fd = self.type.connection.new_commands_notification_fd, .n = n_new_commands } },
                .user_data = self,
            });
        }
    }

    pub fn executeCommands(self: *FSM, event_queue: *EventQueue) !void {
        std.debug.assert(self.type == .connection);
        const connection_fsm = &self.type.connection;
        const wakeup_event = Event{
            .type = .{ .read = .{ .fd = connection_fsm.new_commands_notification_fd, .buffer = std.mem.asBytes(&connection_fsm.notification_val) } },
            .user_data = self,
        };
        connection_fsm.state = .executing_commands;
        try event_queue.addAsyncEvent(wakeup_event);
    }

    pub fn requestOffset(self: *FSM, event_queue: *EventQueue) !void {
        std.debug.assert(self.type == .connection);
        std.debug.assert(self.type.connection.peer_type == .slave);
        var request_buffer = [_]u8{0} ** 256;
        var temp_allocator = std.heap.FixedBufferAllocator.init(&request_buffer);
        const slave_fsm = &self.type.connection;
        const ack_request = resp.Array(&[_]resp.Value{ resp.BulkString("REPLCONF"), resp.BulkString("GETACK"), resp.BulkString("*") });
        util.resizeBuffer(&slave_fsm.buffer, CLIENT_BUFFER_SIZE);
        slave_fsm.buffer = try std.fmt.bufPrint(slave_fsm.buffer, "{s}", .{try ack_request.encode(temp_allocator.allocator())});
        const getack_event = Event{ .type = .{ .send = .{ .buffer = slave_fsm.buffer, .fd = slave_fsm.fd } }, .user_data = self };
        slave_fsm.state = .sending_getack;
        try event_queue.addAsyncEvent(getack_event);
    }

    pub fn propagateCommand(self: *FSM, event_queue: *EventQueue, command: cmd.Command) !void {
        std.debug.assert(self.type == .connection);
        std.debug.assert(self.type.connection.peer_type == .slave);
        const slave_connection = &self.type.connection;
        slave_connection.state = .propagating_command;
        util.resizeBuffer(&slave_connection.buffer, CLIENT_BUFFER_SIZE);
        slave_connection.buffer = try std.fmt.bufPrint(slave_connection.buffer, "{s}", .{command.bytes});
        const propagation_event = Event{
            .type = .{ .send = .{ .fd = slave_connection.fd, .buffer = slave_connection.buffer } },
            .user_data = self,
        };
        try event_queue.addAsyncEvent(propagation_event);
    }

    pub fn deinit(self: *Self) void {
        switch (self.type) {
            .server => |*fsm| {
                self.global_data.deinit();
                fsm.listener.deinit();
                posix.close(fsm.signalfd);
                self.allocator.destroy(self.global_data);
            },
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
