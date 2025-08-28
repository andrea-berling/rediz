const std = @import("std");

// Let's give PWP a shot, why not? :)
// https://www.hytradboi.com/2025/05c72e39-c07e-41bc-ac40-85e8308f2917-programming-without-pointers
// https://ziggit.dev/t/video-programming-without-pointers/8859
pub fn PointersTable(comptime IndexType: type) type {
    return struct {
        const Pointer = struct {
            next: IndexType,
            distance_from_next: IndexType,
        };

        allocator: std.mem.Allocator,
        backing_array: std.ArrayList(Pointer),
        free_list: std.DoublyLinkedList(struct { offset: IndexType, count: usize }),

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .backing_array = @call(.auto, @field(@FieldType(Self, "backing_array"), "init"), .{allocator}),
                .free_list = .{},
            };
        }

        pub fn deinit(self: *Self) void {
            self.backing_array.deinit();
            var free_list_ptr = self.free_list.first;
            while (free_list_ptr) |node| {
                free_list_ptr = node.next;
                self.allocator.destroy(node);
            }
        }

        // Allocate n new pointers, and return an index to it in the backing
        // array
        // Going outside the boundary (i.e. over the returned index + n) will
        // lead to data corruption
        pub fn newPointers(self: *Self, n: u8) !IndexType {
            var free_list_ptr = self.free_list.first;
            while (free_list_ptr) |node| : (free_list_ptr = node.next) {
                if (node.data.count >= n) {
                    const ret = node.data.offset;

                    node.data.count -= n;
                    if (node.data.count == 0) {
                        self.free_list.remove(node);
                        self.allocator.destroy(node);
                    } else {
                        node.data.offset += n;
                    }

                    return ret;
                }
            }

            // Couldn't find a free slot in the free list, time to allocate
            // some more
            const offset_before_append = self.backing_array.items.len;
            _ = try self.backing_array.addManyAt(self.backing_array.items.len, n);

            return @truncate(offset_before_append);
        }

        pub fn get(self: *const Self, index: IndexType, count: u8) ?[]Pointer {
            if (index >= self.backing_array.items.len or index + count > self.backing_array.items.len) {
                return null;
            }

            return self.backing_array.items[index .. index + count];
        }

        pub fn free(self: *Self, index: IndexType, count: u8) !void {
            if (index >= self.backing_array.items.len or index + count > self.backing_array.items.len) {
                return;
            }

            @memset(self.backing_array.items[index .. index + count], Pointer{ .next = 0, .distance_from_next = 0 });
            if (index + count == self.backing_array.items.len) {
                for (0..count) |_| {
                    _ = self.backing_array.pop();
                }
            } else {
                try self.addToFreeList(index, count);
            }
        }

        fn addToFreeList(self: *Self, index: IndexType, count: usize) !void {
            var free_list_ptr = self.free_list.first;
            const Node = @field(@TypeOf(self.free_list), "Node");
            var first_larger_node: ?*Node = null;
            var first_adjacent_node: ?*Node = null;
            var first_adjacent_node_comes_after = false;
            while (free_list_ptr) |node| : (free_list_ptr = node.next) {
                if (count < node.data.count and first_larger_node == null) {
                    first_larger_node = node;
                }
                if (index + count == node.data.offset) {
                    first_adjacent_node_comes_after = true;
                    first_adjacent_node = node;
                }
                if (node.data.offset + node.data.count == index) {
                    first_adjacent_node = node;
                }
                if (first_larger_node != null and first_adjacent_node != null) {
                    break;
                }
            }

            if (first_adjacent_node) |adjacent_node| {
                adjacent_node.data.count += count;
                if (first_adjacent_node_comes_after) {
                    adjacent_node.data.offset = index;
                }

                return;
            }

            const new_node = try self.allocator.create(Node);
            new_node.data = .{ .offset = index, .count = count };

            if (first_larger_node) |larger_node| {
                self.free_list.insertBefore(larger_node, new_node);
            } else {
                self.free_list.append(new_node);
            }
        }
    };
}

/// A sorted set based on a skip list
pub const SortedSet = struct {
    allocator: std.mem.Allocator,
    free_list: std.SinglyLinkedList(IndexType),
    max_level: u8,
    /// Number of live nodes
    n_items: u32,
    // Memory pool for nodes. Just because an index is less than the len of
    // this array list, doesn't mean the node is valid (might be in the free
    // list)
    nodes: std.ArrayList(Node),
    // Mapping from node name to node index in the nodes memory pool
    nodes_index: std.StringArrayHashMap(IndexType),
    // Compact array of pointers to the next nodes, for each node
    pointers: PointersTable(IndexType),
    rng: std.Random.DefaultPrng,
    starting_pointers: [MAX_HEIGHT_PER_NODE]Pointer,

    const IndexType = u32;
    const MAX_HEIGHT_PER_NODE: usize = 16;
    const NIL: IndexType = std.math.maxInt(IndexType);

    const Node = struct {
        height: u8,
        name: []const u8,
        pointers_off: IndexType,
        score: f64,
    };

    const Self = @This();
    const Pointer = PointersTable(IndexType).Pointer;

    pub fn init(allocator: std.mem.Allocator) !Self {
        return .{
            .allocator = allocator,
            .free_list = .{},
            .max_level = 0,
            .n_items = 0,
            .nodes = std.ArrayList(Node).init(allocator),
            .nodes_index = std.StringArrayHashMap(IndexType).init(allocator),
            .pointers = @call(.auto, @field(@FieldType(Self, "pointers"), "init"), .{allocator}),
            .rng = std.Random.DefaultPrng.init(@bitCast(std.time.microTimestamp())),
            .starting_pointers = [_]Pointer{.{ .next = NIL, .distance_from_next = 1 }} ** MAX_HEIGHT_PER_NODE,
        };
    }

    pub fn deinit(self: *Self) void {
        var free_list_it = self.free_list.first;
        while (free_list_it) |free_list_item| {
            free_list_it = free_list_item.next;
            self.allocator.destroy(free_list_item);
        }
        while (self.nodes_index.pop()) |entry| {
            self.allocator.free(entry.key);
        }
        self.pointers.deinit();
        self.nodes.deinit();
        self.nodes_index.deinit();
    }

    // name is considered owned by the data structure after calling this function
    fn newNode(self: *Self, name: []const u8, score: f64, height: u8) !IndexType {
        std.debug.assert(height <= MAX_HEIGHT_PER_NODE);
        const new_node_idx: IndexType = if (self.free_list.popFirst()) |node| blk: {
            defer self.allocator.destroy(node);
            break :blk node.data;
        } else blk: {
            // Important for keeping the internal len of the ArrayList consistent for reallocs
            _ = try self.nodes.addOne();
            break :blk @truncate(self.nodes.items.len - 1);
        };
        try self.nodes_index.put(name, new_node_idx);
        var node = &self.nodes.items[new_node_idx];
        node.name = name;
        node.score = score;
        node.pointers_off = try self.pointers.newPointers(height);
        node.height = height;

        return new_node_idx;
    }

    fn freeNode(self: *Self, idx: IndexType) !void {
        if (idx == self.nodes.items.len) {
            _ = self.nodes.pop();
        } else {
            var new_free_list_node = try self.allocator.create(@field(@TypeOf(self.free_list), "Node"));
            new_free_list_node.data = idx;
            self.free_list.prepend(new_free_list_node);
        }
    }

    fn randomLevel(self: *Self) u8 {
        var level: u8 = 0;
        var i: u8 = 0;
        while (i < self.max_level + 1) : (i += 1) {
            if (self.rng.random().boolean()) {
                level += 1;
            }
        }
        return level;
    }

    // name is duped in the function body
    pub fn put(self: *Self, name: []const u8, score: f64) !void {
        if (self.nodes_index.contains(name)) {
            try self.remove(name);
        }

        var temp_allocator = std.heap.ArenaAllocator.init(self.allocator);
        defer temp_allocator.deinit();
        const find_result = try self.find(name, score, .insert, temp_allocator.allocator());

        const random_level = self.randomLevel();
        if (random_level > self.max_level) {
            self.max_level += 1;
            self.starting_pointers[self.max_level].next = NIL;
            self.starting_pointers[self.max_level].distance_from_next = self.n_items + 1;
        }

        const node_idx = try self.newNode(try self.allocator.dupe(u8, name), score, random_level + 1);

        // No errors allowed forward
        errdefer comptime unreachable;

        const new_node = &self.nodes.items[node_idx];
        var new_node_pointers = self.pointers.get(new_node.pointers_off, new_node.height).?;

        std.debug.assert(find_result == .trace);

        // The rank of the new node is 1 + the rank of the node before it.
        const new_node_rank = find_result.trace[0].rank + 1;
        for (0..self.max_level + 1) |i| {
            // Get the pointer that needs to be updated at this level.
            // This is the last node we visited at this level before finding
            // the insertion point, or, if the new node's level increased the
            // max level, is the new corresponding head pointer
            var pointer_to_update = if (i < find_result.trace.len and find_result.trace[i].node_idx != null) blk: {
                const node_to_update_idx = find_result.trace[i].node_idx.?;
                const node = &self.nodes.items[node_to_update_idx];
                var node_ptrs = self.pointers.get(node.pointers_off, node.height).?;
                break :blk &node_ptrs[i];
            } else blk: {
                break :blk &self.starting_pointers[i];
            };

            // If the current level `i` is within the height of our new node,
            // we need to wire it in.
            if (i < new_node.height) {
                const previous_node_rank = if (i < find_result.trace.len) find_result.trace[i].rank else 0;
                const distance_from_previous_to_new_node = new_node_rank - previous_node_rank;
                // Everything to the right has been shifted forward by 1 in the
                // ranking, hence the +1
                const distance_from_new_node_to_its_next = pointer_to_update.distance_from_next + 1 - distance_from_previous_to_new_node;

                new_node_pointers[i].next = pointer_to_update.next;
                new_node_pointers[i].distance_from_next = distance_from_new_node_to_its_next;
                pointer_to_update.next = node_idx;
                pointer_to_update.distance_from_next = distance_from_previous_to_new_node;
            } else {
                // If the new node is shorter than the current level `i`, it
                // won't be linked in at this level.
                // However, the `pointer_to_update` at this level now has one
                // more node between it and its `next` (our new node), so we
                // increment its distance.
                pointer_to_update.distance_from_next += 1;
            }
        }

        self.n_items += 1;
    }

    // node_idx and the index i int the array identify a pointer as
    // pointers[nodes[node_idx]][i]
    // node_idx is null if the pointer refers to one of the starting pointers
    // This representation is resistant against any sort of re-allocations
    const TraceItem = struct { node_idx: ?IndexType, rank: IndexType };
    const Trace = []TraceItem;

    // Returns a find trace: a slice of the rightmost Pointers that were
    // navigated and would be affected by the given operation + the rank of the
    // element owning that pointer, or the rank of the found node
    // It's an error for insert find an element with the given name in the
    // ordered set
    fn find(self: *const Self, name: []const u8, score: f64, operation: enum { get, insert, remove }, allocator: std.mem.Allocator) !union(enum) { trace: Trace, rank: ?IndexType } {
        var trace = try allocator.alloc(TraceItem, self.max_level + 1);

        var current: struct {
            node_idx: ?IndexType,
            node_pointers: []const Pointer,
            level: usize,
            rank: IndexType,
        } = .{
            .node_idx = null,
            .node_pointers = self.starting_pointers[0..MAX_HEIGHT_PER_NODE],
            .level = self.max_level,
            .rank = 0,
        };

        while (true) {
            trace[current.level].rank = current.rank;
            trace[current.level].node_idx = current.node_idx;

            const next_node_idx = current.node_pointers[current.level].next;
            const distance_from_next = current.node_pointers[current.level].distance_from_next;

            if (next_node_idx == NIL) {
                if (current.level == 0) {
                    if (operation == .get) {
                        return .{ .rank = null };
                    } else {
                        return .{ .trace = trace };
                    }
                }
                current.level -= 1;
                continue;
            }

            const next_node = &self.nodes.items[next_node_idx];

            if (std.mem.eql(u8, next_node.name, name)) {
                if (operation == .insert) {
                    return error.NodePresentAlready;
                } else if (operation == .get) {
                    return .{ .rank = current.rank + distance_from_next };
                } else {
                    trace[current.level].rank = current.rank;
                    trace[current.level].node_idx = current.node_idx;

                    if (current.level == 0) {
                        return .{ .trace = trace };
                    }
                    current.level -= 1;
                    continue;
                }
            }

            // Lower score first, lower lexicographic order second
            if (next_node.score < score or (next_node.score == score and std.mem.lessThan(u8, next_node.name, name))) {
                current.node_pointers = self.pointers.get(next_node.pointers_off, next_node.height).?;
                current.rank += distance_from_next;
                current.node_idx = next_node_idx;
            } else {
                if (current.level == 0) {
                    return .{ .trace = trace };
                }
                current.level -= 1;
            }
        }
    }

    pub fn remove(self: *Self, name: []const u8) !void {
        const node_to_remove_idx = self.nodes_index.get(name) orelse return;

        const node_to_remove = &self.nodes.items[node_to_remove_idx];
        const node_to_remove_ptrs = self.pointers.get(node_to_remove.pointers_off, node_to_remove.height).?;

        var temp_allocator = std.heap.ArenaAllocator.init(self.allocator);
        defer temp_allocator.deinit();

        const find_result = try self.find(
            node_to_remove.name,
            node_to_remove.score,
            .remove,
            temp_allocator.allocator(),
        );
        std.debug.assert(find_result == .trace);

        for (0..self.max_level + 1) |i| {
            // Get the node pointer to update at this level
            var node_pointer_to_update = if (find_result.trace[i].node_idx) |node_to_update_idx| blk: {
                const node_to_update = &self.nodes.items[node_to_update_idx];
                break :blk &(self.pointers.get(node_to_update.pointers_off, node_to_update.height).?)[i];
            } else blk: {
                break :blk &self.starting_pointers[i];
            };

            // If we link to the node to be removed at this level, we need to
            // update the previous pointer
            if (i < node_to_remove.height) {
                node_pointer_to_update.next = node_to_remove_ptrs[i].next;
            }

            // This is the gap in the distance that is left at this level by
            // removing the node
            // If we don't link with the node to be removed at this level, we
            // don't create any gap in the distance chain
            const distance_gap = if (i < node_to_remove.height)
                node_to_remove_ptrs[i].distance_from_next
            else
                0;
            node_pointer_to_update.distance_from_next += distance_gap;
            // Everything to the right has been shifted backward by 1 in the
            // ranking, hence the -1
            node_pointer_to_update.distance_from_next -= 1;
        }

        // No more nodes at the highest level, we can go one down
        if (node_to_remove.height == self.max_level and self.starting_pointers[self.max_level].next == NIL) {
            self.max_level -= 1;
        }

        if (self.nodes_index.fetchSwapRemove(name)) |entry| {
            self.allocator.free(entry.key);
        }

        try self.freeNode(node_to_remove_idx);
        self.n_items -= 1;
    }

    pub fn contains(self: *Self, name: []const u8) bool {
        return self.nodes_index.contains(name);
    }

    pub fn getScoreByName(self: *const Self, name: []const u8) ?f64 {
        const node_idx = self.nodes_index.get(name) orelse return null;
        return self.nodes.items[node_idx].score;
    }

    pub fn getRankByName(self: *const Self, name: []const u8) ?IndexType {
        const node_idx = self.nodes_index.get(name) orelse return null;
        const score = self.nodes.items[node_idx].score;

        var temp_allocator = std.heap.ArenaAllocator.init(self.allocator);
        defer temp_allocator.deinit();
        // find's body has no errors when the operation is get
        const find_result = self.find(name, score, .get, temp_allocator.allocator()) catch unreachable;
        std.debug.assert(find_result == .rank);
        return find_result.rank;
    }

    // n is in [1..=n_items]
    fn findNth(self: *const Self, n: IndexType) ?IndexType {
        if (n > self.n_items) return null;

        var current: struct {
            node_idx: ?IndexType,
            node_pointers: []const Pointer,
            level: usize,
            rank: IndexType,
        } = .{
            .node_idx = null,
            .node_pointers = self.starting_pointers[0..MAX_HEIGHT_PER_NODE],
            .level = self.max_level,
            .rank = 0,
        };

        // A rank value is either above the number of elements, in which case
        // no node has that rank, else there must be a node with that rank
        // (i.e. no gaps)
        // The .eq branch will always eventually be evaluated
        while (true) {
            const next_node_idx = current.node_pointers[current.level].next;
            const distance_from_next = current.node_pointers[current.level].distance_from_next;

            switch (std.math.order(current.rank + distance_from_next, n)) {
                .eq => {
                    return next_node_idx;
                },
                .lt => {
                    current.node_idx = next_node_idx;
                    current.rank += distance_from_next;
                    current.node_pointers = self.pointers.get(self.nodes.items[next_node_idx].pointers_off, self.nodes.items[next_node_idx].height).?;
                },
                .gt => {
                    current.level -= 1;
                },
            }
        }
    }

    fn rangeBoundToRank(range_bound: i32, max_rank: u32) ?u32 {
        if (range_bound >= max_rank) return null;
        return 1 + (if (range_bound >= 0)
            @min(@as(u32, @intCast(range_bound)), max_rank - 1)
        else
            max_rank -| @as(u32, @intCast(-range_bound)));
    }

    // Range bounds start at 0
    pub fn getRange(self: *const Self, range_start: i32, range_end: i32, allocator: std.mem.Allocator) !?[]Node {
        const start_rank = rangeBoundToRank(range_start, self.n_items) orelse return &[0]Node{};
        const end_rank = rangeBoundToRank(range_end, self.n_items) orelse return &[0]Node{};

        if (end_rank < start_rank) return null;

        var ret = try allocator.alloc(Node, end_rank - start_rank + 1);
        ret[0] = self.nodes.items[self.findNth(start_rank).?];
        for (1..ret.len) |i| {
            const prev_node_pointers = self.pointers.get(ret[i - 1].pointers_off, ret[i - 1].height).?;
            ret[i] = self.nodes.items[prev_node_pointers[0].next];
        }

        return ret;
    }

    // getByRange

    // ------------------ Visualization ------------------

    /// Emit a Graphviz DOT representation of the skip list.
    /// Example:
    ///   var file = try std.fs.cwd().createFile("skiplist.dot", .{ .truncate = true });
    ///   defer file.close();
    ///   var bw = std.io.bufferedWriter(file.writer());
    ///   try os.emitDot(bw.writer());
    ///   try bw.flush();
    pub fn emitDot(self: *SortedSet, w: anytype) !void {
        const max_h: usize = @intCast(self.max_level + 1);

        try w.print(
            \\digraph Skiplist {{
            \\  rankdir=LR;
            \\  splines=true;
            \\  nodesep=0.4; ranksep=0.7;
            \\  fontname="Helvetica";
            \\  node [shape=plaintext, fontname="Helvetica"];
            \\
        , .{});

        // ---- HEAD ----
        try w.print("  HEAD [label=<\n", .{});
        try emitTowerLabel(w, "HEAD", null, max_h);
        try w.print(">];\n", .{});

        // ---- each node as an HTML-like table with one vertical column of level slots ----
        var node_idx: u32 = self.starting_pointers[0].next;
        while (node_idx != Self.NIL) {
            const n = self.nodes.items[node_idx];
            const node_pointers = self.pointers.get(n.pointers_off, n.height).?;
            try w.print("  n{d} [label=<\n", .{node_idx});
            try emitTowerLabel(w, null, &n, max_h);
            try w.print(">];\n", .{});
            node_idx = node_pointers[0].next;
        }

        // ---- NIL box ----
        try w.print(
            \\  NIL [shape=record, label="{{NIL}}", style="rounded,dashed", color="#888888"];
            \\
        , .{});

        // ---- edges from HEAD ----
        for (0..max_h) |lvl| {
            const nxt = self.starting_pointers[lvl];
            if (nxt.next != Self.NIL) {
                try w.print(
                    \\  HEAD:p{[lvl]d}:e -> n{[next]d}:p{[lvl]d}:w [arrowsize=0.7, penwidth=1.1, label="{[distance]d}"];
                    \\
                , .{ .lvl = lvl, .next = nxt.next, .distance = nxt.distance_from_next });
            } else {
                try w.print(
                    \\  HEAD:p{[lvl]d}:e -> NIL [color="#888888", arrowsize=0.7, label="{[distance]d}"];
                    \\
                , .{ .lvl = lvl, .distance = nxt.distance_from_next });
            }
        }

        // ---- edges for each node per level ----
        node_idx = self.starting_pointers[0].next;
        var i: usize = 0;
        while (node_idx != Self.NIL) : (i += 1) {
            const n = self.nodes.items[node_idx];
            const nexts = self.pointers.get(n.pointers_off, n.height).?;
            var l: usize = 0;
            while (l < n.height) : (l += 1) {
                const nxt = nexts[l];
                if (nxt.next != Self.NIL) {
                    try w.print(
                        \\  n{d}:p{d}:e -> n{d}:p{d}:w [arrowsize=0.7, penwidth=1.1, label="{d}"];
                        \\
                    , .{ node_idx, l, nxt.next, l, nxt.distance_from_next });
                } else {
                    try w.print(
                        \\  n{d}:p{d}:e -> NIL [color="#888888", arrowsize=0.7, label="{d}"];
                        \\
                    , .{ node_idx, l, nxt.distance_from_next });
                }
            }
            // if a node doesn't reach higher levels, nothing to draw (blank cells already rendered)
            node_idx = nexts[0].next;
        }

        try w.print("}}\n", .{});
    }

    /// Helper: render an HTML-like label for a tower.
    /// If `title_text` is non-null we show it (HEAD); else we show the node's (score,name).
    fn emitTowerLabel(w: anytype, title_text: ?[]const u8, node: ?*const Node, max_h: usize) !void {
        try w.print(
            \\  <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">
            \\    <TR>
            \\      <TD>
            \\        <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">
            \\
        , .{});

        // highest level at the top for a consistent “tower” height
        var lvl: i32 = @intCast(max_h);
        while (lvl > 0) : (lvl -= 1) {
            const L: usize = @intCast(lvl - 1);
            const have: bool = blk: {
                if (node) |n| break :blk (L < n.height);
                // HEAD has all levels up to max_h
                break :blk true;
            };
            if (have) {
                try w.print(
                    \\          <TR><TD PORT="p{d}" FIXEDSIZE="TRUE" WIDTH="10" HEIGHT="18">•</TD></TR>
                    \\
                , .{L});
            } else {
                // keep column height: draw an empty slot (white bullet)
                try w.print(
                    \\          <TR><TD FIXEDSIZE="TRUE" WIDTH="10" HEIGHT="18"><FONT COLOR="white">•</FONT></TD></TR>
                    \\
                , .{});
            }
        }

        try w.print(
            \\        </TABLE>
            \\      </TD>
            \\      <TD>
            \\        <TABLE BORDER="0" CELLBORDER="0" CELLSPACING="2">
            \\
        , .{});

        if (title_text) |t| {
            try w.print("          <TR><TD><B>{s}</B></TD></TR>\n", .{t});
        } else if (node) |n| {
            try w.print("          <TR><TD><B>{s}</B></TD></TR>\n", .{n.name});
            try w.print("          <TR><TD>{d:.6}</TD></TR>\n", .{n.score});
        }

        try w.print(
            \\        </TABLE>
            \\      </TD>
            \\    </TR>
            \\  </TABLE>
            \\
        , .{});
    }

    pub fn printDot(self: *SortedSet) !void {
        try self.emitDot(std.io.getStdOut().writer());
    }

    // convenience wrapper to write directly to a file
    pub fn writeDotFile(self: *SortedSet, path: []const u8) !void {
        var file = try std.fs.cwd().createFile(path, .{ .truncate = true });
        defer file.close();
        var bw = std.io.bufferedWriter(file.writer());
        try self.emitDot(bw.writer());
        try bw.flush();
    }
};

const testing = std.testing;

fn expectRank(set: *SortedSet, name: []const u8, expected: SortedSet.IndexType) !void {
    const got = set.getRankByName(name) orelse return error.Missing;
    try testing.expectEqual(expected, got);
}

fn expectScore(set: *SortedSet, name: []const u8, expected: f64) !void {
    const got = set.getScoreByName(name) orelse return error.Missing;
    try testing.expectEqual(@as(f64, expected), got);
}

fn expectMissing(set: *SortedSet, name: []const u8) !void {
    try testing.expect(set.getScoreByName(name) == null);
}

test "create empty and basic lookups" {
    var set = try SortedSet.init(testing.allocator);
    defer set.deinit();

    try expectMissing(&set, "alice");
    // Removing a non-existent element should be a no-op (and not error)
    try set.remove("alice");
    try expectMissing(&set, "alice");
}

test "single insert and remove" {
    var set = try SortedSet.init(testing.allocator);
    defer set.deinit();

    try set.put("alice", 1.0);
    try expectScore(&set, "alice", 1.0);

    try set.remove("alice");
    try expectMissing(&set, "alice");

    // Re-insert after removal still works
    try set.put("alice", 2.5);

    try expectScore(&set, "alice", 2.5);
}

test "multiple inserts, hits and misses" {
    var set = try SortedSet.init(testing.allocator);
    defer set.deinit();

    try set.put("alice", 1.0);
    try set.put("bob", 0.5);
    try set.put("carol", 3.25);
    try set.put("dave", 2.0);

    try expectScore(&set, "alice", 1.0);
    try expectScore(&set, "bob", 0.5);
    try expectScore(&set, "carol", 3.25);
    try expectScore(&set, "dave", 2.0);

    try expectMissing(&set, "eve");
}

test "removing present and absent nodes" {
    var set = try SortedSet.init(testing.allocator);
    defer set.deinit();

    try set.put("alice", 1.0);
    try set.put("bob", 2.0);

    // Remove existing
    try set.remove("alice");
    try expectMissing(&set, "alice");
    try expectScore(&set, "bob", 2.0);

    // Remove absent (no-op)
    try set.remove("carol");
    try expectScore(&set, "bob", 2.0);
}

test "stress: many inserts, then partial removals" {
    var set = try SortedSet.init(testing.allocator);
    defer set.deinit();

    // Insert 100 names with predictable scores
    var buf: [32]u8 = undefined;
    var f: f64 = 0.0;
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        const name = try std.fmt.bufPrint(&buf, "user_{d}", .{i});
        f = @floatFromInt(i); // score = i
        try set.put(name, f);
    }

    // Check a few lookups hit
    try expectScore(&set, "user_0", 0.0);
    try expectScore(&set, "user_42", 42.0);
    try expectScore(&set, "user_99", 99.0);

    // Check ranks
    i = 0;
    @memset(&buf, 0);
    while (i < 100) : (i += 1) {
        const name = try std.fmt.bufPrint(&buf, "user_{d}", .{i});
        try expectRank(&set, name, @truncate(i + 1));
    }

    // Remove half of them (even indices)
    i = 0;
    while (i < 100) : (i += 2) {
        const name = try std.fmt.bufPrint(&buf, "user_{d}", .{i});
        try set.remove(name);
    }

    // Verify evens are gone, odds remain
    i = 0;
    @memset(&buf, 0);
    while (i < 100) : (i += 1) {
        const name = try std.fmt.bufPrint(&buf, "user_{d}", .{i});
        if (i % 2 == 0) {
            try expectMissing(&set, name);
        } else {
            try expectScore(&set, name, @floatFromInt(i));
            try expectRank(&set, name, @truncate((i + 1) / 2));
        }
    }
}

test "insert with duplicate scores" {
    var set = try SortedSet.init(testing.allocator);
    defer set.deinit();

    try set.put("c", 1.0);
    try set.put("a", 1.0);
    try set.put("b", 1.0);

    try expectRank(&set, "a", 1);
    try expectRank(&set, "b", 2);
    try expectRank(&set, "c", 3);
}

test "remove head and tail" {
    var set = try SortedSet.init(testing.allocator);
    defer set.deinit();

    try set.put("a", 1.0);
    try set.put("b", 2.0);
    try set.put("c", 3.0);

    // Remove tail
    try set.remove("c");
    try expectMissing(&set, "c");
    try expectRank(&set, "a", 1);
    try expectRank(&set, "b", 2);
    try testing.expectEqual(@as(u32, 2), set.n_items);

    // Remove head
    try set.remove("a");
    try expectMissing(&set, "a");
    try expectRank(&set, "b", 1);
    try testing.expectEqual(@as(u32, 1), set.n_items);
}

test "max_level changes" {
    var set = try SortedSet.init(testing.allocator);
    defer set.deinit();

    // This is probabilistic, so we can't guarantee max_level changes.
    // But with enough insertions, it should.
    // We'll check that the data structure remains consistent.
    var i: usize = 0;
    while (i < 50) : (i += 1) {
        var buf: [16]u8 = undefined;
        const name = try std.fmt.bufPrint(&buf, "item_{d}", .{i});
        try set.put(name, @floatFromInt(i));
    }

    try testing.expectEqual(@as(u32, 50), set.n_items);
    try expectRank(&set, "item_0", 1);
    try expectRank(&set, "item_49", 50);

    // Remove all but one to see if max_level decreases
    i = 0;
    while (i < 49) : (i += 1) {
        var buf: [16]u8 = undefined;
        const name = try std.fmt.bufPrint(&buf, "item_{d}", .{i});
        try set.remove(name);
    }

    try testing.expectEqual(@as(u32, 1), set.n_items);
    try expectRank(&set, "item_49", 1);
}

test "getRange edge cases" {
    var set = try SortedSet.init(testing.allocator);
    defer set.deinit();

    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        var buf: [16]u8 = undefined;
        const name = try std.fmt.bufPrint(&buf, "n{d}", .{i});
        try set.put(name, @floatFromInt(i));
    }

    var temp_allocator = std.heap.ArenaAllocator.init(testing.allocator);
    defer temp_allocator.deinit();

    // Full range
    var range = (try set.getRange(0, -1, temp_allocator.allocator())).?;
    try testing.expectEqual(@as(usize, 10), range.len);
    try testing.expectEqualStrings("n0", range[0].name);
    try testing.expectEqualStrings("n9", range[9].name);

    // Partial range
    range = (try set.getRange(2, 4, temp_allocator.allocator())).?;
    try testing.expectEqual(@as(usize, 3), range.len);
    try testing.expectEqualStrings("n2", range[0].name);
    try testing.expectEqualStrings("n4", range[2].name);

    // Negative indices
    range = (try set.getRange(-3, -1, temp_allocator.allocator())).?;
    try testing.expectEqual(@as(usize, 3), range.len);
    try testing.expectEqualStrings("n7", range[0].name);
    try testing.expectEqualStrings("n9", range[2].name);

    // Out of bounds
    range = (try set.getRange(20, 30, temp_allocator.allocator())).?;
    try testing.expectEqual(@as(usize, 0), range.len);

    // Inverted range
    const inverted_range = try set.getRange(5, 1, temp_allocator.allocator());
    try testing.expect(inverted_range == null);
}

test "PointersTable allocation and reuse" {
    var pt = PointersTable(u32).init(testing.allocator);
    defer pt.deinit();

    const p1 = try pt.newPointers(4);
    try testing.expectEqual(@as(u32, 0), p1);
    try testing.expectEqual(@as(usize, 4), pt.backing_array.items.len);

    const p2 = try pt.newPointers(8);
    try testing.expectEqual(@as(u32, 4), p2);
    try testing.expectEqual(@as(usize, 12), pt.backing_array.items.len);

    // Free the first block
    try pt.free(p1, 4);
    try testing.expectEqual(@as(usize, 1), pt.free_list.len);
    try testing.expectEqual(@as(u32, 0), pt.free_list.first.?.data.offset);
    try testing.expectEqual(@as(u8, 4), pt.free_list.first.?.data.count);

    // Allocate a smaller block, should reuse the free slot
    const p3 = try pt.newPointers(2);
    try testing.expectEqual(@as(u32, 0), p3);
    try testing.expectEqual(@as(usize, 12), pt.backing_array.items.len); // No change in size
    try testing.expectEqual(@as(usize, 1), pt.free_list.len);
    try testing.expectEqual(@as(u32, 2), pt.free_list.first.?.data.offset);
    try testing.expectEqual(@as(u8, 2), pt.free_list.first.?.data.count);
}

test "PointersTable free at end shrinks backing array" {
    var pt = PointersTable(u32).init(testing.allocator);
    defer pt.deinit();

    _ = try pt.newPointers(4);
    const p2 = try pt.newPointers(8);
    try testing.expectEqual(@as(usize, 12), pt.backing_array.items.len);

    // Free the last block
    try pt.free(p2, 8);
    try testing.expectEqual(@as(usize, 4), pt.backing_array.items.len);
    try testing.expectEqual(@as(usize, 0), pt.free_list.len);
}

test "PointersTable free list merging" {
    var pt = PointersTable(u32).init(testing.allocator);
    defer pt.deinit();

    _ = try pt.newPointers(2);
    const p2 = try pt.newPointers(3);
    const p3 = try pt.newPointers(4);
    _ = try pt.newPointers(5);
    try testing.expectEqual(@as(usize, 14), pt.backing_array.items.len);

    // Free p2 and p3, which are adjacent
    try pt.free(p2, 3);
    try pt.free(p3, 4);

    // Should merge into a single free block of size 7
    try testing.expectEqual(@as(usize, 1), pt.free_list.len);
    try testing.expectEqual(@as(u32, 2), pt.free_list.first.?.data.offset);
    try testing.expectEqual(@as(u8, 7), pt.free_list.first.?.data.count);
}

test "SortedSet node allocator reuse" {
    var set = try SortedSet.init(testing.allocator);
    defer set.deinit();

    try set.put("a", 1.0);
    try set.put("b", 2.0);
    try set.put("c", 3.0);
    try testing.expectEqual(@as(usize, 3), set.nodes.items.len);
    try testing.expectEqual(@as(usize, 0), set.free_list.len());

    // Remove a node from the middle
    try set.remove("b");
    try testing.expectEqual(@as(usize, 3), set.nodes.items.len); // Length doesn't change
    try testing.expectEqual(@as(usize, 1), set.free_list.len());
    const free_idx = set.free_list.first.?.data;
    try testing.expectEqual(@as(u32, 1), free_idx); // "b" was at index 1

    // Add a new node
    try set.put("d", 4.0);
    // Should reuse the free slot
    try testing.expectEqual(@as(usize, 3), set.nodes.items.len);
    try testing.expectEqual(@as(usize, 0), set.free_list.len());

    // Check that the reused node has the correct data
    const node_idx = set.nodes_index.get("d").?;
    try testing.expectEqual(free_idx, node_idx);
    const node = set.nodes.items[node_idx];
    try testing.expectEqualStrings("d", node.name);
}
