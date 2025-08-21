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

test Rc {
    const Account = struct {
        const Self = @This();
        allocator: Allocator,
        blance: usize,
        data: []u8,

        fn init(allocator: Allocator) !Self {
            const data = try allocator.alloc(u8, 10);
            return .{
                .allocator = allocator,
                .blance = 0,
                .data = data,
            };
        }

        fn deinit(self: *Self) void {
            self.allocator.free(self.data);
            self.* = undefined;
        }
    };

    const allocator = std.testing.allocator;

    var account = try Account.init(allocator);
    var account_rc: Rc(*Account) = try .init(
        allocator,
        &account,
    );
    account.blance = 1;

    var clone_account_rc = account_rc.clone();
    defer clone_account_rc.deinit();
    account_rc.deinit();
    try std.testing.expectEqual(1, clone_account_rc.val().blance);
}
