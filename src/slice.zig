const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Bytes = Slice(u8);

pub const BytesHashMapContext = struct {
    pub fn hash(_: @This(), s: Bytes) u64 {
        return std.hash.Wyhash.hash(0, s.val());
    }
    pub fn eql(_: @This(), a: Bytes, b: Bytes) bool {
        return std.mem.eql(u8, a.val(), b.val());
    }
};

/// A reference-counted slice type that can hold either owned or borrowed data.
/// Provides memory-safe sharing through reference counting for owned slices,
/// while allowing zero-cost usage of compile-time constants and borrowed data.
pub fn Slice(comptime T: type) type {
    return union(enum) {
        const Rc = struct {
            allocator: Allocator,
            slice: []T,
            refs: usize,
        };

        const Self = @This();

        Owned: *Rc,
        Const: []const T,

        /// Take ownership of the heap-allocated slice `s`
        /// (allocated with `allocator`).
        /// Automatically free `s` if internal allocation fails.
        pub fn move(s: []T, allocator: Allocator) Allocator.Error!Self {
            const rc = allocator.create(Rc) catch |err| {
                allocator.free(s);
                return err;
            };
            rc.* = .{
                .slice = s,
                .allocator = allocator,
                .refs = 1,
            };
            return .{ .Owned = rc };
        }

        /// Copy the provided slice using the allocator.
        /// The `s` parameter should be freed by the caller.
        pub fn copy(s: []const T, allocator: Allocator) Allocator.Error!Self {
            const rc = try allocator.create(Rc);
            rc.* = .{
                .slice = try allocator.dupe(T, s),
                .allocator = allocator,
                .refs = 1,
            };
            return .{ .Owned = rc };
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

        /// Create a new reference to the same slice data and increment
        /// the reference count.
        pub fn clone(self: Self) Self {
            switch (self) {
                .Owned => |rc| {
                    rc.refs += 1;
                    return .{ .Owned = rc };
                },
                .Const => return self,
            }
        }

        /// Return the underlying slice.
        pub fn val(self: Self) []T {
            switch (self) {
                .Owned => |rc| return rc.slice,
                .Const => |c| return @constCast(c),
            }
        }

        /// Return the underlying slice's len.
        pub fn len(self: Self) usize {
            return self.val().len;
        }

        /// Free any allocated memory associated with the managed slice.
        pub fn deinit(self: Self) void {
            switch (self) {
                .Owned => |rc| {
                    rc.refs -= 1;
                    if (rc.refs == 0) {
                        rc.allocator.free(rc.slice);
                        rc.allocator.destroy(rc);
                    }
                },
                .Const => {},
            }
        }
    };
}

test "runtime string" {
    const allocator = std.testing.allocator;

    const message = try std.fmt.allocPrint(allocator, "hello world", .{});
    const greeting = try Bytes.move(message, allocator);
    defer greeting.deinit();

    try std.testing.expectEqualStrings(
        "hello world",
        greeting.val(),
    );
}

test "copy runtime string" {
    const allocator = std.testing.allocator;

    const message = try std.fmt.allocPrint(allocator, "hello world", .{});
    const greeting = try Bytes.copy(message, allocator);
    allocator.free(message);
    defer greeting.deinit();

    try std.testing.expectEqualStrings(
        "hello world",
        greeting.val(),
    );
}

test "static string" {
    const greeting = Bytes.static("hello world");
    greeting.deinit();

    try std.testing.expectEqualStrings(
        "hello world",
        greeting.val(),
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
        greeting.val(),
    );
}

test "clone runtime slice" {
    const allocator = std.testing.allocator;
    const message = try std.fmt.allocPrint(allocator, "hello world", .{});

    const greeting = try Bytes.move(message, allocator);
    const clone = greeting.clone();
    defer clone.deinit();

    greeting.deinit();
    try std.testing.expectEqualStrings(
        "hello world",
        clone.val(),
    );
}

test "clone static slice" {
    const greeting = Bytes.static("hello world");
    const clone = greeting.clone();
    defer clone.deinit();

    greeting.deinit();
    try std.testing.expectEqualStrings(
        "hello world",
        clone.val(),
    );
}

test "slice of string" {
    const strings: Slice(Bytes) = .static(&.{
        Bytes.static("hello"),
        Bytes.static("world"),
    });

    for (strings.val(), 0..) |string, i| {
        try std.testing.expectEqualStrings(
            strings.val()[i].val(),
            string.val(),
        );
    }
}
