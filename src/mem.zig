const std = @import("std");
const Allocator = std.mem.Allocator;
const Atomic = @import("atomic.zig").Atomic;

pub fn Ref(comptime atomic: bool) type {
    return struct {
        const Self = @This();

        refs: if (atomic) Atomic(usize) else usize,

        pub fn init(ref: usize) Self {
            return .{
                .refs = if (atomic) Atomic(usize).init(ref) else ref,
            };
        }

        pub fn incr(self: *Self) void {
            if (atomic) {
                self.refs.add(1);
            } else {
                self.refs += 1;
            }
        }

        pub fn decr(self: *Self) void {
            if (atomic) {
                self.refs.sub(1);
            } else {
                self.refs -= 1;
            }
        }

        pub fn get(self: *Self) usize {
            if (atomic) {
                return self.refs.get();
            }
            return self.refs;
        }
    };
}

pub const Bytes = Slice(u8);

pub const BytesHashContext = struct {
    pub fn hash(_: @This(), s: Bytes) u64 {
        return std.hash.Wyhash.hash(0, s.val());
    }
    pub fn eql(_: @This(), a: Bytes, b: Bytes) bool {
        return std.mem.eql(u8, a.val(), b.val());
    }
};

pub fn Slice(comptime T: type) type {
    return union(enum) {
        const Box = struct {
            allocator: Allocator,
            slice: []T,
            refs: Ref(true),
        };

        const Self = @This();

        Owned: *Box,
        Const: []const T,

        pub fn move(s: []T, allocator: Allocator) Allocator.Error!Self {
            const box = allocator.create(Box) catch |err| {
                allocator.free(s);
                return err;
            };
            box.* = .{
                .slice = s,
                .allocator = allocator,
                .refs = .init(1),
            };
            return .{ .Owned = box };
        }

        pub fn copy(s: []const T, allocator: Allocator) Allocator.Error!Self {
            const box = try allocator.create(Box);
            box.* = .{
                .slice = try allocator.dupe(T, s),
                .allocator = allocator,
                .refs = .init(1),
            };
            return .{ .Owned = box };
        }

        pub fn static(comptime s: []const T) Self {
            return .{ .Const = s };
        }

        pub fn managed(s: []const T) Self {
            return .{ .Const = s };
        }

        pub fn clone(self: Self) Self {
            switch (self) {
                .Owned => |box| {
                    box.refs.incr();
                    return .{ .Owned = box };
                },
                .Const => return self,
            }
        }

        pub fn val(self: Self) []T {
            switch (self) {
                .Owned => |box| return box.slice,
                .Const => |c| return @constCast(c),
            }
        }

        pub fn eql(self: Self, other: Self) bool {
            return std.mem.eql(T, self.val(), other.val());
        }

        pub fn len(self: Self) usize {
            return self.val().len;
        }

        pub fn deinit(self: Self) void {
            switch (self) {
                .Owned => |box| {
                    box.refs.decr();
                    if (box.refs.get() == 0) {
                        box.allocator.free(box.slice);
                        box.allocator.destroy(box);
                    }
                },
                .Const => {},
            }
        }
    };
}
