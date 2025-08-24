const std = @import("std");

pub fn Atomic(comptime T: type) type {
    return struct {
        const Self = @This();

        value: std.atomic.Value(T),

        pub fn init(value: T) Self {
            return .{
                .value = .init(value),
            };
        }

        pub fn add(self: *Self, delta: T) void {
            _ = self.value.fetchAdd(delta, .monotonic);
        }

        pub fn sub(self: *Self, delta: T) void {
            _ = self.value.fetchSub(delta, .monotonic);
        }

        pub fn get(self: *const Self) T {
            return self.value.load(.monotonic);
        }
    };
}
