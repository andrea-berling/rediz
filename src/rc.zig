const std = @import("std");
const Allocator = std.mem.Allocator;

const logger = std.log.scoped(.rc);
const trace_ref_counts = false;

/// This is the internal, heap-allocated object.
/// It's not meant to be used directly by consumers of Rc.
fn RcBox(comptime T: type) type {
    return struct {
        ref_count: usize,
        data: T,
    };
}

/// A reference-counted smart pointer.
pub fn Rc(comptime T: type) type {
    return struct {
        ptr: *RcBox(T),
        allocator: std.mem.Allocator,

        const Self = @This();

        /// Creates a new Rc, allocating the data on the heap with a reference count of 1.
        pub fn init(allocator: Allocator, data: T) !Self {
            const box = try allocator.create(RcBox(T));
            box.* = .{
                .ref_count = 1,
                .data = data,
            };
            if (trace_ref_counts) {
                logger.debug("Reference count for {*} set to 1", .{box});
                std.debug.dumpCurrentStackTrace(null);
            }
            return Self{ .ptr = box, .allocator = allocator };
        }

        /// Creates a new Rc that points to the same allocation.
        /// This increases the reference count by 1.
        pub fn clone(self: Self) Self {
            self.ptr.ref_count += 1;
            if (trace_ref_counts) {
                logger.debug("Reference count for {*} increased by 1 (current value: {})", .{ self.ptr, self.ptr.ref_count });
                std.debug.dumpCurrentStackTrace(null);
            }
            return self;
        }

        /// Decrements the reference count. If the count reaches 0,
        /// it deinitializes the inner data and frees the memory.
        pub fn release(self: Self) void {
            self.ptr.ref_count -= 1;
            if (trace_ref_counts) {
                logger.debug("Reference count for {*} decreased by 1 (current value: {})", .{ self.ptr, self.ptr.ref_count });
                std.debug.dumpCurrentStackTrace(null);
            }
            if (self.ptr.ref_count == 0) {
                // If the inner type T has a deinit function, call it.
                if (@hasDecl(T, "deinit")) {
                    self.ptr.data.deinit();
                }
                self.allocator.destroy(self.ptr);
            }
        }

        pub fn deinit(self: Self) void {
            self.release();
        }

        /// Returns a pointer to the underlying data.
        pub fn get(self: Self) *T {
            return &self.ptr.data;
        }
    };
}
