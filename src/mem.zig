const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn Rc(comptime T: type) type {
    return struct {
        const Self = @This();

        const Box = struct {
            value: T,
            refs: usize = 1,
        };

        box: *Box,
        allocator: Allocator,

        pub fn init(
            allocator: Allocator,
            value: T,
        ) Allocator.Error!Self {
            const box = try allocator.create(Box);
            box.* = .{ .value = value };
            return .{
                .allocator = allocator,
                .box = box,
            };
        }

        pub fn val(self: *const Self) T {
            return self.box.value;
        }

        /// Increment the reference count and returns a new `Rc`
        /// pointing to the same value.
        pub fn clone(self: *const Self) Self {
            self.box.refs += 1;
            return .{
                .allocator = self.allocator,
                .box = self.box,
            };
        }

        /// Decrement the reference count and frees the value
        /// when it reaches zero.
        pub fn deinit(self: Self) void {
            self.box.refs -= 1;
            if (self.box.refs == 0) {
                self.box.value.deinit();
                self.allocator.destroy(self.box);
            }
        }
    };
}

pub const String = Slice(u8);

/// A reference-counted smart pointer that provides shared ownership of a value.
/// The resource is automatically freed when the last `Rc` is deinitialized.
pub fn Slice(comptime T: type) type {
    return union(enum) {
        pub const Allocated = struct {
            allocator: Allocator,
            slice: []T,
        };

        const Self = @This();

        Owned: Allocated,
        Const: []const T,

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

test "Rc runtime string" {
    const allocator = std.testing.allocator;
    const StringRc = Rc(String);

    const message = try std.fmt.allocPrint(allocator, "hello world", .{});
    const greesting = String.move(message, allocator);

    var greeting_rc = try StringRc.init(
        allocator,
        greesting,
    );
    defer greeting_rc.deinit();

    try std.testing.expectEqualStrings(
        "hello world",
        greeting_rc.val().slice(),
    );
}

test "Rc copy runtime string" {
    const allocator = std.testing.allocator;
    const StringRc = Rc(String);

    const message = try std.fmt.allocPrint(allocator, "hello world", .{});
    defer allocator.free(message);

    const greesting = try String.copy(message, allocator);
    var greeting_rc = try StringRc.init(
        allocator,
        greesting,
    );
    defer greeting_rc.deinit();

    try std.testing.expectEqualStrings(
        "hello world",
        greeting_rc.val().slice(),
    );
}

test "Rc static string" {
    const allocator = std.testing.allocator;
    const StringRc = Rc(String);

    const greesting = String.static("hello world");
    var greeting_rc = try StringRc.init(
        allocator,
        greesting,
    );
    defer greeting_rc.deinit();

    try std.testing.expectEqualStrings(
        "hello world",
        greeting_rc.val().slice(),
    );
}

test "Rc managed string" {
    const allocator = std.testing.allocator;
    const StringRc = Rc(String);

    const message = try std.fmt.allocPrint(allocator, "hello world", .{});
    defer allocator.free(message);
    const greesting = String.managed(message);

    var greeting_rc = try StringRc.init(
        allocator,
        greesting,
    );
    defer greeting_rc.deinit();

    try std.testing.expectEqualStrings(
        "hello world",
        greeting_rc.val().slice(),
    );
}

test "Rc single value deinit" {
    const Account = struct {
        const Self = @This();
        allocator: Allocator,
        blance: usize,

        fn init(allocator: Allocator) Self {
            return .{
                .allocator = allocator,
                .blance = 0,
            };
        }

        fn deinit(self: *Self) void {
            self.* = undefined;
        }
    };

    const allocator = std.testing.allocator;
    const AccountRc = Rc(*Account);

    var account = Account.init(allocator);
    var account_rc = try AccountRc.init(
        allocator,
        &account,
    );
    defer account_rc.deinit();
    account.blance = 1;

    var clone_account_rc = account_rc.clone();
    defer clone_account_rc.deinit();
    try std.testing.expectEqual(1, clone_account_rc.val().blance);

    var optional: ?AccountRc = account_rc.clone();
    if (optional) |*o| {
        o.deinit();
    }
}
