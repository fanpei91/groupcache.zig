const std = @import("std");
const Allocator = std.mem.Allocator;

/// A reference-counted smart pointer that provides shared ownership of a value.
/// The resource is automatically freed when the last `Rc` is deinitialized.
/// The Idea is from https://github.com/Arwalk/zig-protobuf.
pub fn Slice(comptime T: type) type {
    return union(enum) {
        pub const Allocated = struct {
            allocator: Allocator,
            slice: []T,
        };

        const Self = @This();

        Owned: Allocated,
        Const: []const T,

        /// Take ownership of the heap-allocated slice `s`
        /// (allocated with `allocator`).
        pub fn move(s: []T, allocator: Allocator) Self {
            return .{
                .Owned = Allocated{
                    .slice = s,
                    .allocator = allocator,
                },
            };
        }

        /// Copy the provided slice using the allocator.
        /// The `s` parameter should be freed by the caller.
        pub fn copy(s: []const T, allocator: Allocator) Allocator.Error!Self {
            return .{
                .Owned = Allocated{
                    .slice = try allocator.dupe(T, s),
                    .allocator = allocator,
                },
            };
        }

        /// Create a deep copy of the managed slice
        /// using the provided allocator.
        pub fn dupe(self: Self, allocator: Allocator) Allocator.Error!Self {
            switch (self) {
                .Owned => |alloc| return copy(alloc.slice, allocator),
                .Const => return self,
            }
        }

        /// Create a static slice from a compile time const.
        pub fn static(comptime s: []const T) Self {
            return .{ .Const = s };
        }

        /// Create a static slice that will not be released
        /// by calling .deinit().
        pub fn managed(s: []const T) Self {
            return .{ .Const = s };
        }

        /// Return the underlying slice.
        pub fn slice(self: Self) []T {
            switch (self) {
                .Owned => |alloc| return alloc.slice,
                .Const => |c| return @constCast(c),
            }
        }

        /// Return the underlying slice's len.
        pub fn len(self: Self) usize {
            return self.slice().len;
        }

        /// Free any allocated memory associated with the managed slice.
        pub fn deinit(self: Self) void {
            switch (self) {
                .Owned => |alloc| {
                    alloc.allocator.free(alloc.slice);
                },
                .Const => {},
            }
        }
    };
}

test "runtime string" {
    const allocator = std.testing.allocator;

    const message = try std.fmt.allocPrint(allocator, "hello world", .{});
    const greeting = Bytes.move(message, allocator);
    defer greeting.deinit();

    try std.testing.expectEqualStrings(
        "hello world",
        greeting.slice(),
    );
}

test "copy runtime string" {
    const allocator = std.testing.allocator;

    const message = try std.fmt.allocPrint(allocator, "hello world", .{});
    const greeting = Bytes.copy(message, allocator) catch {
        allocator.free(message);
        return;
    };
    allocator.free(message);
    defer greeting.deinit();

    try std.testing.expectEqualStrings(
        "hello world",
        greeting.slice(),
    );
}

test "static string" {
    const greeting = Bytes.static("hello world");
    greeting.deinit();

    try std.testing.expectEqualStrings(
        "hello world",
        greeting.slice(),
    );
}

test "managed string" {
    const allocator = std.testing.allocator;

    const message = try std.fmt.allocPrint(allocator, "hello world", .{});
    defer allocator.free(message);

    const greeting = Bytes.managed(message);
    greeting.deinit();

    try std.testing.expectEqualStrings(
        "hello world",
        greeting.slice(),
    );
}

const Bytes = Slice(u8);
