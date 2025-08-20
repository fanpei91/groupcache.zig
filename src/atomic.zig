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

        pub fn get(self: *const Self) T {
            return self.value.load(.monotonic);
        }
    };
}

test "Atomic" {
    const Atomic64 = Atomic(i64);
    var a = Atomic64.init(0);
    const threads = 12;
    var wg = std.Thread.WaitGroup{};
    wg.startMany(threads);
    for (0..threads) |_| {
        _ = std.Thread.spawn(.{}, struct {
            fn add(at: *Atomic64, w: *std.Thread.WaitGroup) void {
                at.add(1);
                w.finish();
            }
        }.add, .{ &a, &wg }) catch unreachable;
    }
    wg.wait();

    try std.testing.expectEqual(threads, a.get());
}
