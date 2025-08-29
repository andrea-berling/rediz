const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const Allocator = std.mem.Allocator;
const util = @import("util.zig");
const cmd = @import("command.zig");
const eq = @import("event_queue.zig");
const resp = @import("resp.zig");
const db = @import("db.zig");
const Rc = @import("rc.zig").Rc;

pub const RcFSM = Rc(FSM);
pub const EventQueue = eq.EventQueue(RcFSM);
pub const Event = eq.Event(RcFSM);

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
    /// Kind of peer on the other end of this connection
    peer_type: union(enum) {
        slave: struct { repl_offset: usize },
        master,
        generic_client,
    } = .generic_client,
    state: union(enum) {
        /// We heard back from this slave from our last REPLCONF GETACK
        /// request, and now we have the latest known ACK counter value
        in_sync,
        /// We executed a command and are now writing a response back to the
        /// other peer
        sending_response,
        /// We are waiting for bytes on the wire from a regular client to parse
        /// them into commands
        waiting_for_commands,
        /// We have some commands in our command queue that are pending to be
        /// executed
        executing_commands,
        /// This slave has requested a full dump, and we are now in the process
        /// of sending it to the wire
        sending_dump,
        /// We are sending a command that should be propagated to slaves to
        /// this slave
        propagating_command,
        /// We are sending a REPLCONF GETACK request to this slave
        sending_getack,
        /// We sent a REPLCONF GETACK request to this slave, and we are waiting
        /// for a reply back
        waiting_for_ack,
        /// The peer on the other side of this connection is waiting for a
        /// certain number of slaves to get up to date with the master
        blocked: *PendingWait,
        /// The peer on the other side of this connection is waiting for data
        /// to be added to one of the given streams
        waiting_for_new_data_on_stream: *BlockedStreamRead,
        /// The peer on the other side of this connection is waiting for data
        /// to be added to a list
        waiting_for_new_data_on_list: *BlockedBlpop,
        /// A new transaction has been started, and we are in the process of
        /// collecting all the new commands received into a list to be executed
        /// later
        executing_transaction,
        /// The client has subscribed to a list of channels
        subscribed_to: std.StringArrayHashMap(void),
    } = .waiting_for_commands,
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

    pub fn hasCommands(self: *Self) bool {
        return self.commands_to_execute.len > 0;
    }

    pub fn subscribeTo(self: *Self, chan: []const u8) !usize {
        if (self.state != .subscribed_to) {
            self.state = .{ .subscribed_to = std.StringArrayHashMap(void).init(self.allocator) };
        }
        if (!self.state.subscribed_to.contains(chan)) {
            try self.state.subscribed_to.put(try self.allocator.dupe(u8, chan), {});
        }

        return self.state.subscribed_to.count();
    }

    pub fn unsubscribe(self: *Self, chan: []const u8) !usize {
        std.debug.assert(self.state == .subscribed_to);

        var n = self.state.subscribed_to.count();

        if (self.state.subscribed_to.contains(chan)) {
            const chan_kv = self.state.subscribed_to.fetchSwapRemove(chan).?;
            n -= 1;
            self.allocator.free(chan_kv.key);
            if (n == 0) {
                self.state.subscribed_to.deinit();
                // Changing the state here is necessary to prevent trying to
                // clean up a de-initialized .subscribed_to HashMap in
                // Connection.deinit
                self.state = .waiting_for_commands;
            }
        }

        return n;
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        posix.close(self.fd);
        self.flushCommandsQueue();
        if (self.state == .subscribed_to) {
            while (self.state.subscribed_to.pop()) |entry| {
                self.allocator.free(entry.key);
            }
            self.state.subscribed_to.deinit();
        }
        util.resizeBuffer(&self.buffer, CLIENT_BUFFER_SIZE);
        allocator.free(self.buffer);
    }
};

pub const FSM = struct {
    type: union(enum) {
        /// A TCP socket in listen mode
        server: *Server,
        /// An established TCP connection
        connection: *Connection,
    },
    /// Global data that can be shared among multiple FSMs. In the absence of
    /// multiple threads, this is safe
    global_data: *GlobalData,
    allocator: std.mem.Allocator,
    /// A list of IDs of asynchronous events that are pending for this FSM
    /// Can be used later on for cancellation
    pending_event_identifiers: std.AutoHashMap(usize, void),
    /// A flag indicating whether this FSM is still valid or not. It can happen
    /// that an async event for this FSM has already completed before this FSM
    /// needs to be shutdown due to, e.g., connection reset, in which case we
    /// an event with pointer to this FSM that needs to be ignored
    /// This flags allows us to detect this situation and ignore the event
    alive: bool,

    const Self = @This();

    pub fn newServer(allocator: std.mem.Allocator, address: []const u8, port: u16) !Self {
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

        const server = try allocator.create(Server);
        server.* = .{ .state = .waiting, .listener = listener, .socket = listener.stream.handle, .signalfd = sfd };

        const global_data = try allocator.create(GlobalData);
        global_data.* = GlobalData.init(allocator);

        return .{ .global_data = global_data, .type = .{
            .server = server,
        }, .allocator = allocator, .pending_event_identifiers = std.AutoHashMap(usize, void).init(allocator), .alive = true };
    }

    pub fn newConnection(allocator: std.mem.Allocator, peer_socket: posix.socket_t, peer_type: @FieldType(Connection, "peer_type"), global_data: *GlobalData) !Self {
        const connection = try allocator.create(Connection);
        connection.* = try Connection.init(allocator, peer_socket, peer_type);

        return .{ .global_data = global_data, .type = .{ .connection = connection }, .allocator = allocator, .pending_event_identifiers = std.AutoHashMap(usize, void).init(allocator), .alive = true };
    }

    /// New commands to execute are available, and this FSM should now go and
    /// execute them
    pub fn notify(fsm: RcFSM, event_queue: *EventQueue, n_new_commands: usize) !void {
        std.debug.assert(fsm.get().type == .connection);
        if (n_new_commands > 0) {
            try fsm.get().pending_event_identifiers.put(try event_queue.addAsyncEvent(Event{
                .type = .{ .notify = .{ .fd = fsm.get().type.connection.new_commands_notification_fd, .n = n_new_commands } },
                .user_data = fsm.clone(),
            }), undefined);
        }
    }

    pub fn requestOffset(fsm: RcFSM, event_queue: *EventQueue) !void {
        std.debug.assert(fsm.get().type == .connection);
        std.debug.assert(fsm.get().type.connection.peer_type == .slave);
        var request_buffer = [_]u8{0} ** 256;
        var temp_allocator = std.heap.FixedBufferAllocator.init(&request_buffer);
        const slave_fsm = fsm.get().type.connection;
        const ack_request = resp.Array(&[_]resp.Value{ resp.BulkString("REPLCONF"), resp.BulkString("GETACK"), resp.BulkString("*") });
        util.resizeBuffer(&slave_fsm.buffer, CLIENT_BUFFER_SIZE);
        slave_fsm.buffer = try std.fmt.bufPrint(slave_fsm.buffer, "{s}", .{try ack_request.encode(temp_allocator.allocator())});
        const getack_event = Event{ .type = .{ .send = .{ .buffer = slave_fsm.buffer, .fd = slave_fsm.fd } }, .user_data = fsm.clone() };
        slave_fsm.state = .sending_getack;
        try fsm.get().pending_event_identifiers.put(try event_queue.addAsyncEvent(getack_event), undefined);
    }

    pub fn propagateCommand(fsm: RcFSM, event_queue: *EventQueue, command: cmd.Command) !void {
        std.debug.assert(fsm.get().type == .connection);
        std.debug.assert(fsm.get().type.connection.peer_type == .slave);
        const slave_connection = fsm.get().type.connection;
        slave_connection.state = .propagating_command;
        util.resizeBuffer(&slave_connection.buffer, CLIENT_BUFFER_SIZE);
        slave_connection.buffer = try std.fmt.bufPrint(slave_connection.buffer, "{s}", .{command.bytes});
        const propagation_event = Event{
            .type = .{ .send = .{ .fd = slave_connection.fd, .buffer = slave_connection.buffer } },
            .user_data = fsm.clone(),
        };
        try fsm.get().pending_event_identifiers.put(try event_queue.addAsyncEvent(propagation_event), undefined);
    }

    /// Update the replica offset of this slave and possibly unblock clients if
    /// the new slave offset update increases the number of up-to-date slaves
    /// they are waiting on and reaches the desired threshold
    pub fn updateSlaveOffset(self: *FSM, new_offset: usize, event_queue: *EventQueue) !void {
        std.debug.assert(self.type == .connection);
        const slave_connection = self.type.connection;
        const old_offset = slave_connection.peer_type.slave.repl_offset;
        // Only update the offset if the message is still relevant
        // (i.e. if the received offset is larger than the last
        // previously known offset)
        if (old_offset < new_offset) {
            slave_connection.peer_type.slave.repl_offset = new_offset;
        }

        var new_pending_waits = std.PriorityQueue(*PendingWait, void, PendingWait.order).init(self.allocator, undefined);
        while (self.global_data.pending_waits.removeOrNull()) |pending_wait| {
            // Only pending waits who have a threshold offset that
            // wasn't met previously by the slave and that is now
            // met after the offset update should be counted
            if (pending_wait.threshold_offset > old_offset and pending_wait.threshold_offset <= new_offset) {
                pending_wait.actual_n_replicas += 1;
                if (pending_wait.actual_n_replicas >= pending_wait.expected_n_replicas) {
                    const reply = resp.Integer(@bitCast(pending_wait.actual_n_replicas));
                    var response_buffer = [_]u8{0} ** 256;
                    var temp_allocator = std.heap.FixedBufferAllocator.init(&response_buffer);
                    try StateTransitions.respondWith(pending_wait.client_connection_fsm, try reply.encode(temp_allocator.allocator()), event_queue, .{});
                    pending_wait.deinit();
                    self.allocator.destroy(pending_wait);
                } else {
                    try new_pending_waits.add(pending_wait);
                }
            } else {
                try new_pending_waits.add(pending_wait);
            }
        }

        self.global_data.pending_waits.deinit();
        self.global_data.pending_waits = new_pending_waits;
    }

    /// Send the new data to all peers that were blocked waiting on new data on this stream
    pub fn unblockAllClientsWaitingOnStream(self: *Self, xadd_command: *const cmd.StreamAddCommand, new_entry_id: resp.Value, event_queue: *EventQueue) !void {
        if (self.global_data.blocked_xreads.get(xadd_command.stream_key)) |blocked_stream_reads| {
            for (blocked_stream_reads.keys()) |blocked_stream_read| {
                defer self.allocator.destroy(blocked_stream_read);
                defer blocked_stream_read.deinit();

                var temp_allocator = std.heap.ArenaAllocator.init(self.allocator);
                defer temp_allocator.deinit();

                var response = std.ArrayList(resp.Value).init(temp_allocator.allocator());
                try response.append(resp.BulkString(xadd_command.stream_key));
                var entry_elements = std.ArrayList(resp.Value).init(temp_allocator.allocator());
                for (xadd_command.key_value_pairs) |pair| {
                    try entry_elements.append(resp.BulkString(pair.key));
                    try entry_elements.append(resp.BulkString(pair.value));
                }

                var tmp_array = try temp_allocator.allocator().alloc(resp.Value, 2);
                tmp_array[0] = new_entry_id;
                tmp_array[1] = resp.Array(try entry_elements.toOwnedSlice());
                try response.append(resp.Array(&[_]resp.Value{resp.Array(tmp_array)}));

                try StateTransitions.respondWith(blocked_stream_read.client_connection_fsm, try resp.Array(&[_]resp.Value{resp.Array(try response.toOwnedSlice())}).encode(temp_allocator.allocator()), event_queue, .{});
            }

            // Cleanup of the full HashMap entries associated with this stream
            if (self.global_data.blocked_xreads.getKey(xadd_command.stream_key)) |stream_key| {
                const blocked_xreads = self.global_data.blocked_xreads.get(xadd_command.stream_key).?;
                blocked_xreads.deinit();
                self.allocator.destroy(blocked_xreads);
                _ = self.global_data.blocked_xreads.remove(stream_key);
                self.allocator.free(stream_key);
            }
        }
    }

    /// Unblock the first peer that is waiting for data on this list
    pub fn unblockOneClientWaitingOnList(self: *Self, list_key: []const u8, db_instance: *db.Instance, event_queue: *EventQueue) !void {
        if (self.global_data.blocked_blpops.getKey(list_key)) |blocked_blpops_key| {
            const blocked_blpops = self.global_data.blocked_blpops.getEntry(blocked_blpops_key).?;

            var temp_allocator = std.heap.ArenaAllocator.init(self.allocator);
            defer temp_allocator.deinit();

            var response = std.ArrayList(resp.Value).init(temp_allocator.allocator());
            try response.append(resp.BulkString(list_key));
            try response.append(resp.BulkString((try db_instance.pop(temp_allocator.allocator(), list_key)).?));

            const maybe_blocked_blpop = blocked_blpops.value_ptr.*.popFirst();

            if (maybe_blocked_blpop) |blocked_blpop| {
                defer self.allocator.destroy(blocked_blpop);
                defer self.allocator.destroy(blocked_blpop.data);
                defer blocked_blpop.data.deinit();

                try StateTransitions.respondWith(blocked_blpop.data.client_connection_fsm, try resp.Array(try response.toOwnedSlice()).encode(temp_allocator.allocator()), event_queue, .{});
            }

            // Cleanup of the HashMap entry for this list if it has gone empty
            if (blocked_blpops.value_ptr.*.len == 0) {
                _ = self.global_data.blocked_blpops.remove(blocked_blpops_key);
                self.allocator.free(blocked_blpops_key);
            }
        }
    }

    pub fn classifyTimeout(self: *Self, timerfd: posix.fd_t) ?Timeout {
        // Is it a pending wait?
        var pending_wait_it = self.global_data.pending_waits.iterator();

        const connection_fsm = self.type.connection;

        while (pending_wait_it.next()) |pw| {
            // TODO: not the best: what if there is both a valid and an invalid pw that match the FSM?
            // To revisit: probably best to add some ancilliary data to the .pollin event
            if (timerfd == pw.timerfd and connection_fsm.state == .blocked and connection_fsm.state.blocked == pw and pw.timeout <= std.time.milliTimestamp()) {
                return .{ .kind = .PENDING_WAIT, .valid = true };
            }
            if (pw.client_connection_fsm.get() == self and (connection_fsm.state != .blocked or connection_fsm.state.blocked != pw)) { // late alarm
                return .{ .kind = .PENDING_WAIT, .valid = false };
            }
        }

        if (connection_fsm.state == .waiting_for_new_data_on_stream) {
            return .{
                .kind = .BLOCKED_XREAD,
                // The client might have done a blocking read, got the data, done another blocking read, and this timeout could be the timeout of either
                // Admittedly, this check is not bulletproof either for fd's can be recycled. Probably best to also save an identifier in the pollin event
                .valid = if (connection_fsm.state.waiting_for_new_data_on_stream.timerfd == timerfd) true else false,
            };
        }

        if (connection_fsm.state == .waiting_for_new_data_on_list) {
            return .{ .kind = .BLOCKED_BLPOP, .valid = if (connection_fsm.state.waiting_for_new_data_on_list.timerfd == timerfd) true else false };
        }
        var blocked_xreads_it = self.global_data.blocked_xreads.iterator();
        while (blocked_xreads_it.next()) |blocked_xread| {
            const stream_blocked_reads = blocked_xread.value_ptr.*;
            for (stream_blocked_reads.keys()) |read| {
                if (read.client_connection_fsm.get() == self and read.timerfd != null and read.timerfd == timerfd) {
                    return .{ .kind = .BLOCKED_XREAD, .valid = false };
                }
            }
        }

        var blocked_blpops_it = self.global_data.blocked_blpops.iterator();
        while (blocked_blpops_it.next()) |blocked_blpop| {
            var blocked_blpops_current_node = blocked_blpop.value_ptr.*.first;
            while (blocked_blpops_current_node) |current_node| {
                if (current_node.data.client_connection_fsm.get() == self and current_node.data.timerfd != null and current_node.data.timerfd == timerfd)
                    return .{ .kind = .BLOCKED_BLPOP, .valid = false };
                blocked_blpops_current_node = current_node.next;
            }
        }

        return null;
    }

    pub fn cleanupLateStreamReadTimeout(self: *Self, timerfd: posix.fd_t) !void {
        var temp_allocator = std.heap.ArenaAllocator.init(self.allocator);
        defer temp_allocator.deinit();

        var blocked_xreads_it = self.global_data.blocked_xreads.iterator();
        var streams_to_remove = std.ArrayList([]const u8).init(temp_allocator.allocator());
        while (blocked_xreads_it.next()) |blocked_xread| {
            const stream = blocked_xread.key_ptr.*;
            const stream_blocked_reads = blocked_xread.value_ptr.*;
            var blockex_xreads_to_remove = std.ArrayList(*BlockedStreamRead).init(temp_allocator.allocator());
            for (stream_blocked_reads.keys()) |read| {
                if (read.client_connection_fsm.get() == self and read.timerfd != null and read.timerfd == timerfd) {
                    try blockex_xreads_to_remove.append(read);
                }
            }
            for (blockex_xreads_to_remove.items) |xread| {
                xread.deinit();
                _ = stream_blocked_reads.swapRemove(xread);
            }
            if (stream_blocked_reads.count() == 0) {
                stream_blocked_reads.deinit();
                self.allocator.destroy(stream_blocked_reads);
                try streams_to_remove.append(stream);
            }
        }
        for (streams_to_remove.items) |stream| {
            const stream_key = self.global_data.blocked_xreads.getKey(stream).?;
            _ = self.global_data.blocked_xreads.remove(stream_key);
            self.allocator.free(stream_key);
        }
    }

    pub fn cleanupLateBlpopTimeout(self: *Self, timerfd: posix.fd_t) !void {
        var temp_allocator = std.heap.ArenaAllocator.init(self.allocator);
        defer temp_allocator.deinit();

        // Is it a late timeout that needs to be cleaned up?
        var blocked_blpops_it = self.global_data.blocked_blpops.iterator();
        while (blocked_blpops_it.next()) |blocked_blpop| {
            const list = blocked_blpop.key_ptr.*;
            var blocked_blpops = blocked_blpop.value_ptr;

            var current_node = blocked_blpops.first;
            var nodes_to_remove = std.ArrayList(*std.DoublyLinkedList(*BlockedBlpop).Node).init(temp_allocator.allocator());
            while (current_node) |node| {
                if (node.data.client_connection_fsm.get() == self and node.data.timerfd != null and node.data.timerfd == timerfd) {
                    try nodes_to_remove.append(node);
                    break;
                }
                current_node = node.next;
            }

            for (try nodes_to_remove.toOwnedSlice()) |node| {
                node.data.deinit();
                _ = blocked_blpops.remove(node);
                self.allocator.destroy(node);
            }
            if (blocked_blpops.len == 0) {
                self.allocator.destroy(blocked_blpops);
                _ = self.global_data.blocked_blpops.remove(list);
                self.allocator.free(list);
            }
        }
    }

    pub fn unblockStreamReadClient(self: *FSM, event_queue: *EventQueue) !void {
        std.debug.assert(self.type == .connection);
        const connection_fsm = self.type.connection;
        std.debug.assert(connection_fsm.state == .waiting_for_new_data_on_stream);

        var temp_allocator = std.heap.ArenaAllocator.init(self.allocator);
        defer temp_allocator.deinit();

        const blocked_xread = connection_fsm.state.waiting_for_new_data_on_stream;
        defer self.allocator.destroy(blocked_xread);
        defer blocked_xread.deinit();
        for (blocked_xread.streams.items) |stream| {
            if (self.global_data.blocked_xreads.getKey(stream)) |stream_key| {
                var blocked_reads = self.global_data.blocked_xreads.get(stream_key).?;
                _ = blocked_reads.swapRemove(blocked_xread);
                if (blocked_reads.count() == 0) {
                    blocked_reads.deinit();
                    self.allocator.destroy(blocked_reads);
                    self.allocator.free(stream_key);
                }
            }
        }
        try StateTransitions.respondWith(blocked_xread.client_connection_fsm, try resp.NullArray.encode(temp_allocator.allocator()), event_queue, .{});
    }

    pub fn unblockWaitingClient(self: *Self, timerfd: posix.fd_t, event_queue: *EventQueue) !void {
        std.debug.assert(self.type == .connection);
        const connection_fsm = self.type.connection;

        // Is it a pending wait?
        var pending_wait_it = self.global_data.pending_waits.iterator();

        var maybe_index: ?usize = null;
        var i: usize = 0;

        var temp_allocator = std.heap.ArenaAllocator.init(self.allocator);
        defer temp_allocator.deinit();

        var indicesToRemove = std.ArrayList(usize).init(temp_allocator.allocator());
        while (pending_wait_it.next()) |pw| : (i += 1) {
            if (timerfd == pw.timerfd and connection_fsm.state == .blocked and connection_fsm.state.blocked == pw) {
                maybe_index = i;
            }

            if (pw.client_connection_fsm.get() == self and (connection_fsm.state != .blocked or connection_fsm.state.blocked != pw)) { // late alarm
                try indicesToRemove.append(i);
            }
        }

        // Cleanup of stale events, while we're at it
        for (indicesToRemove.items) |index| {
            const pending_wait = self.global_data.pending_waits.removeIndex(index);
            pending_wait.deinit();
            self.allocator.destroy(pending_wait);
        }

        if (maybe_index) |index| {
            const pending_wait = self.global_data.pending_waits.removeIndex(index);

            const reply = resp.Integer(@bitCast(pending_wait.actual_n_replicas));
            try StateTransitions.respondWith(pending_wait.client_connection_fsm, try reply.encode(temp_allocator.allocator()), event_queue, .{});
            pending_wait.deinit();
            self.allocator.destroy(pending_wait);
        }
    }

    pub fn cleanupLateWaitTimeout(self: *Self, timerfd: posix.fd_t) !void {
        std.debug.assert(self.type == .connection);

        // Is it a pending wait?
        var pending_wait_it = self.global_data.pending_waits.iterator();

        var maybe_index: ?usize = null;

        var temp_allocator = std.heap.ArenaAllocator.init(self.allocator);
        defer temp_allocator.deinit();

        var i: usize = 0;
        while (pending_wait_it.next()) |pw| : (i += 1) {
            if (pw.client_connection_fsm.get() == self and pw.timerfd == timerfd and pw.timeout <= std.time.milliTimestamp()) {
                maybe_index = i;
                break;
            }
        }

        if (maybe_index) |index| {
            const pending_wait = self.global_data.pending_waits.removeIndex(index);
            pending_wait.deinit();
            self.allocator.destroy(pending_wait);
        }
    }

    pub fn unblockBlpopClient(self: *Self, timerfd: posix.fd_t, event_queue: *EventQueue) !void {
        std.debug.assert(self.type == .connection);
        const connection_fsm = self.type.connection;
        std.debug.assert(connection_fsm.state == .waiting_for_new_data_on_list);

        const blocked_blpop = connection_fsm.state.waiting_for_new_data_on_list;
        defer self.allocator.destroy(blocked_blpop);

        var temp_allocator = std.heap.ArenaAllocator.init(self.allocator);
        defer temp_allocator.deinit();

        try StateTransitions.respondWith(blocked_blpop.client_connection_fsm, try resp.NullArray.encode(temp_allocator.allocator()), event_queue, .{});

        if (self.global_data.blocked_blpops.getKey(blocked_blpop.key)) |list_key| {
            var blocked_blpops = self.global_data.blocked_blpops.get(list_key).?;
            var current_node = blocked_blpops.first;
            var nodes_to_remove = std.ArrayList(*std.DoublyLinkedList(*BlockedBlpop).Node).init(temp_allocator.allocator());
            while (current_node) |node| {
                if (node.data.client_connection_fsm.get() == self and node.data.timerfd != null and node.data.timerfd == timerfd) {
                    try nodes_to_remove.append(node);
                    break;
                }
                current_node = node.next;
            }

            for (try nodes_to_remove.toOwnedSlice()) |node| {
                blocked_blpops.remove(node);
                node.data.deinit();
                self.allocator.destroy(node);
            }

            if (blocked_blpops.len == 0) {
                _ = self.global_data.blocked_blpops.remove(list_key);
                self.allocator.free(list_key);
            }
        }
    }

    pub fn unsubscribe(self: *FSM, chan: []const u8) !usize {
        std.debug.assert(self.type == .connection);

        var subscriptions = &self.global_data.subscriptions;
        const subscription = subscriptions.getEntry(chan).?;
        const rcfsm_kv = subscription.value_ptr.fetchRemove(self).?;
        rcfsm_kv.value.release();
        if (subscription.value_ptr.count() == 0) {
            var subscription_kv = subscriptions.fetchRemove(chan).?;
            subscription_kv.value.deinit();
            self.allocator.free(subscription_kv.key);
        }

        const n = try self.type.connection.unsubscribe(chan);

        return n;
    }

    pub fn deinit(self: *Self) void {
        switch (self.type) {
            .server => |fsm| {
                self.global_data.deinit();
                fsm.listener.deinit();
                posix.close(fsm.signalfd);
                self.allocator.destroy(self.global_data);
                self.allocator.destroy(fsm);
            },
            .connection => |fsm| {
                fsm.deinit(self.allocator);
                self.allocator.destroy(fsm);
            },
        }
        (@constCast(&self.pending_event_identifiers)).deinit();
    }
};

pub const GlobalData = struct {
    slaves: std.AutoHashMap(*FSM, RcFSM),
    pending_waits: std.PriorityQueue(*PendingWait, void, PendingWait.order),
    blocked_xreads: std.StringHashMap(*std.AutoArrayHashMap(*BlockedStreamRead, void)),
    blocked_blpops: std.StringHashMap(std.DoublyLinkedList(*BlockedBlpop)),
    subscriptions: std.StringHashMap(std.AutoHashMap(*FSM, RcFSM)),
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return Self{ .pending_waits = std.PriorityQueue(*PendingWait, void, PendingWait.order).init(allocator, undefined), .blocked_xreads = std.StringHashMap(*std.AutoArrayHashMap(*BlockedStreamRead, void)).init(allocator), .blocked_blpops = std.StringHashMap(std.DoublyLinkedList(*BlockedBlpop)).init(allocator), .slaves = std.AutoHashMap(*FSM, RcFSM).init(allocator), .subscriptions = std.StringHashMap(std.AutoHashMap(*FSM, RcFSM)).init(allocator), .allocator = allocator };
    }

    pub fn deinit(self: *Self) void {
        self.slaves.deinit();
        self.pending_waits.deinit();
        self.blocked_xreads.deinit();
        self.blocked_blpops.deinit();
        self.subscriptions.deinit();
    }
};

pub const PendingWait = struct {
    timeout: i64,
    client_connection_fsm: RcFSM,
    threshold_offset: usize,
    expected_n_replicas: usize,
    actual_n_replicas: usize,
    timerfd: posix.fd_t,

    pub fn order(context: void, a: *PendingWait, b: *PendingWait) std.math.Order {
        _ = context;
        return std.math.order(a.timeout, b.timeout);
    }

    pub fn deinit(self: *PendingWait) void {
        posix.close(self.timerfd);
        self.client_connection_fsm.release();
    }
};

pub const BlockedStreamRead = struct {
    client_connection_fsm: RcFSM,
    streams: std.ArrayList([]const u8),
    timerfd: ?posix.fd_t,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, client_connection_fsm: RcFSM, timerfd: ?posix.fd_t) Self {
        std.debug.assert(client_connection_fsm.get().type == .connection);
        return Self{
            .client_connection_fsm = client_connection_fsm.clone(),
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
        self.client_connection_fsm.release();
    }
};

pub const BlockedBlpop = struct {
    client_connection_fsm: RcFSM,
    key: []const u8,
    timerfd: ?posix.fd_t,
    allocator: Allocator,

    const Self = @This();

    /// The key argument will be duped in this function
    pub fn init(allocator: Allocator, client_connection_fsm: RcFSM, key: []const u8, timerfd: ?posix.fd_t) !Self {
        std.debug.assert(client_connection_fsm.get().type == .connection);
        return Self{ .client_connection_fsm = client_connection_fsm.clone(), .timerfd = timerfd, .allocator = allocator, .key = try allocator.dupe(u8, key) };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.key);
        self.client_connection_fsm.release();
        self.* = undefined;
    }
};

pub fn compareRcFSMs(fsm1: RcFSM, fsm2: RcFSM) bool {
    return fsm1.get() == fsm2.get();
}

pub const StateTransitions = struct {
    pub fn waitForCommand(fsm: RcFSM, event_queue: *EventQueue, new_state: struct { new_state: @FieldType(Connection, "state") = .waiting_for_commands }) !void {
        const pointee = fsm.get();

        if (pointee.type != .connection)
            return error.InvalidFSM;
        const connection_fsm = pointee.type.connection;
        util.resizeBuffer(&connection_fsm.buffer, CLIENT_BUFFER_SIZE);
        @memset(connection_fsm.buffer, 0);
        connection_fsm.state = new_state.new_state;
        const new_command_event = Event{
            .type = .{ .recv = .{ .fd = connection_fsm.fd, .buffer = connection_fsm.buffer } },
            .user_data = fsm.clone(),
        };

        try fsm.get().pending_event_identifiers.put(try event_queue.addAsyncEvent(new_command_event), undefined);
    }

    /// The connection has been broken, do the necessary tombstoning and event
    /// cancellation
    pub fn shutdown(fsm: RcFSM, event_queue: *EventQueue) !void {
        var pending_events_it = fsm.get().pending_event_identifiers.keyIterator();

        while (pending_events_it.next()) |event_id| {
            try fsm.get().pending_event_identifiers.put(try event_queue.addAsyncEvent(Event{ .type = .{ .cancel = event_id.* }, .user_data = fsm.clone() }), undefined);
        }

        if (fsm.get().type == .connection and fsm.get().type.connection.state == .subscribed_to) {
            var temp_allocator = std.heap.ArenaAllocator.init(fsm.allocator);
            defer temp_allocator.deinit();

            // We copy the keys here because the process of unsubscribe will
            // modify the subscibed_to HashMap
            // Iterating over it while removing elements leads to UB
            var to_unsub = std.ArrayList([]const u8).init(temp_allocator.allocator());
            for (fsm.get().type.connection.state.subscribed_to.keys()) |key| {
                try to_unsub.append(try temp_allocator.allocator().dupe(u8, key));
            }

            for (to_unsub.items) |chan| {
                _ = try fsm.get().unsubscribe(chan);
            }
        }

        fsm.get().alive = false;
    }

    /// Send a message to the other end of this connection, and transition to
    /// the new given state
    pub fn respondWith(fsm: RcFSM, response: []const u8, event_queue: *EventQueue, new_state: struct { new_state: @FieldType(Connection, "state") = .sending_response }) !void {
        if (fsm.get().type != .connection)
            return error.InvalidFSM;
        const connection_fsm = fsm.get().type.connection;
        util.resizeBuffer(&connection_fsm.buffer, CLIENT_BUFFER_SIZE);
        connection_fsm.buffer = try std.fmt.bufPrint(connection_fsm.buffer, "{s}", .{response});
        connection_fsm.state = new_state.new_state;
        const response_sent_event = Event{
            .type = .{ .send = .{ .fd = connection_fsm.fd, .buffer = connection_fsm.buffer } },
            .user_data = fsm.clone(),
        };
        try fsm.get().pending_event_identifiers.put(try event_queue.addAsyncEvent(response_sent_event), undefined);
    }

    /// Add a new pending wait to the global list of pending waits and
    /// transition this FSM to being blocked, as well as adding a timeout event
    /// to the event queue
    pub fn setWaiting(fsm: RcFSM, timeout_timestamp: i64, threshold_offset: usize, expected_n_replicas: usize, actual_n_replicas: usize, timeout_fd: posix.fd_t, event_queue: *EventQueue) !void {
        std.debug.assert(fsm.get().type == .connection);

        const pending_wait = try fsm.allocator.create(PendingWait);
        pending_wait.timeout = timeout_timestamp;
        pending_wait.client_connection_fsm = fsm.clone();
        pending_wait.threshold_offset = threshold_offset;
        pending_wait.expected_n_replicas = expected_n_replicas;
        pending_wait.actual_n_replicas = actual_n_replicas;
        pending_wait.timerfd = timeout_fd;

        fsm.get().type.connection.state = .{ .blocked = pending_wait };

        try fsm.get().global_data.pending_waits.add(pending_wait);

        const timeout_event = Event{ .type = .{ .pollin = timeout_fd }, .user_data = fsm.clone() };

        try fsm.get().pending_event_identifiers.put(try event_queue.addAsyncEvent(timeout_event), undefined);
    }

    /// Set the current FSM to being blocked waiting on new data being
    /// available for the given list
    pub fn setWaitingOnListPush(fsm: RcFSM, list_key: []const u8, timeout_fd: ?posix.fd_t, event_queue: *EventQueue) !void {
        std.debug.assert(fsm.get().type == .connection);
        const blocked_blpop = try fsm.get().allocator.create(BlockedBlpop);
        blocked_blpop.* = try BlockedBlpop.init(fsm.get().allocator, fsm, list_key, timeout_fd);

        const blocked_blpops_queue = try fsm.get().global_data.blocked_blpops.getOrPut(list_key);

        if (!blocked_blpops_queue.found_existing) {
            blocked_blpops_queue.key_ptr.* = try fsm.get().allocator.dupe(u8, list_key);
            blocked_blpops_queue.value_ptr.* = std.DoublyLinkedList(*BlockedBlpop){};
        }

        const new_node = try fsm.get().allocator.create(std.DoublyLinkedList(*BlockedBlpop).Node);
        new_node.data = blocked_blpop;

        blocked_blpops_queue.value_ptr.*.append(new_node);

        if (timeout_fd) |fd| {
            const timeout_event = Event{ .type = .{ .pollin = fd }, .user_data = fsm.clone() };
            try fsm.get().pending_event_identifiers.put(try event_queue.addAsyncEvent(timeout_event), undefined);
        }

        fsm.get().type.connection.state = .{ .waiting_for_new_data_on_list = blocked_blpop };
    }

    /// Set the current FSM to being blocked waiting on new data being
    /// available for the given stream
    pub fn setWaitingOnStreamUpdate(fsm: RcFSM, stream_read_requests: []const cmd.StreamReadRequest, timeout_fd: ?posix.fd_t, event_queue: *EventQueue) !void {
        var blocked_stream_read = try fsm.get().allocator.create(BlockedStreamRead);
        blocked_stream_read.* = BlockedStreamRead.init(fsm.get().allocator, fsm, timeout_fd);
        for (stream_read_requests) |request| {
            try blocked_stream_read.addStream(request.stream_key);
        }

        for (stream_read_requests) |request| {
            const blocked_stream_read_queue = try fsm.get().global_data.blocked_xreads.getOrPut(request.stream_key);
            if (!blocked_stream_read_queue.found_existing) {
                blocked_stream_read_queue.key_ptr.* = try fsm.get().allocator.dupe(u8, request.stream_key);
                blocked_stream_read_queue.value_ptr.* = try fsm.get().allocator.create(std.AutoArrayHashMap(*BlockedStreamRead, void));
                blocked_stream_read_queue.value_ptr.*.* = std.AutoArrayHashMap(*BlockedStreamRead, void).init(fsm.get().allocator);
            }

            try blocked_stream_read_queue.value_ptr.*.put(blocked_stream_read, {});
        }

        fsm.get().type.connection.state = .{ .waiting_for_new_data_on_stream = blocked_stream_read };

        if (timeout_fd) |fd| {
            const timeout_event = Event{ .type = .{ .pollin = fd }, .user_data = fsm.clone() };
            try fsm.get().pending_event_identifiers.put(try event_queue.addAsyncEvent(timeout_event), undefined);
        }
    }

    pub fn executeCommands(fsm: RcFSM, event_queue: *EventQueue) !void {
        std.debug.assert(fsm.get().type == .connection);
        const connection_fsm = fsm.get().type.connection;
        const wakeup_event = Event{
            .type = .{ .read = .{ .fd = connection_fsm.new_commands_notification_fd, .buffer = std.mem.asBytes(&connection_fsm.notification_val) } },
            .user_data = fsm.clone(),
        };
        connection_fsm.state = .executing_commands;
        try fsm.get().pending_event_identifiers.put(try event_queue.addAsyncEvent(wakeup_event), undefined);
    }

    pub fn addSubscription(fsm: RcFSM, chan: []const u8, event_queue: *EventQueue) !void {
        std.debug.assert(fsm.get().type == .connection);
        const n = try fsm.get().type.connection.subscribeTo(chan);

        var temp_allocator = std.heap.ArenaAllocator.init(fsm.allocator);
        defer temp_allocator.deinit();

        var subscriptions = &fsm.get().global_data.subscriptions;

        if (!subscriptions.contains(chan)) {
            try subscriptions.put(try fsm.get().allocator.dupe(u8, chan), std.AutoHashMap(*FSM, RcFSM).init(fsm.get().allocator));
        }

        var subscriptions_for_chan = subscriptions.getEntry(chan).?;

        if (!subscriptions_for_chan.value_ptr.contains(fsm.get())) {
            try subscriptions_for_chan.value_ptr.put(fsm.get(), fsm.clone());
        }

        try StateTransitions.respondWith(fsm, try resp.Array(&[_]resp.Value{
            resp.BulkString("subscribe"),
            resp.BulkString(chan),
            resp.Integer(@intCast(n)),
        }).encode(temp_allocator.allocator()), event_queue, .{ .new_state = fsm.get().type.connection.state });
    }

    pub fn unsubscribe(fsm: RcFSM, chan: []const u8, event_queue: *EventQueue) !void {
        std.debug.assert(fsm.get().type == .connection);
        const n = try fsm.get().unsubscribe(chan);

        var temp_allocator = std.heap.ArenaAllocator.init(fsm.allocator);
        defer temp_allocator.deinit();

        try StateTransitions.respondWith(fsm, try resp.Array(&[_]resp.Value{
            resp.BulkString("unsubscribe"),
            resp.BulkString(chan),
            resp.Integer(@intCast(n)),
        }).encode(temp_allocator.allocator()), event_queue, if (n > 0)
            .{ .new_state = fsm.get().type.connection.state }
        else
            .{});
    }
};

pub const Timeout = struct { kind: enum {
    PENDING_WAIT,
    BLOCKED_XREAD,
    BLOCKED_BLPOP,
}, valid: bool };
