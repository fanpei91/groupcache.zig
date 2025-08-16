const std = @import("std");
const Thread = std.Thread;
const Allocator = std.mem.Allocator;
const expectEqual = std.testing.expectEqual;

pub const Key = []const u8;

pub fn Group(comptime ResultType: type) type {
    return struct {
        const Self = @This();

        mutex: Thread.Mutex = .{},
        allocator: Allocator,
        inflight: std.StringHashMap(*Caller),

        const Caller = struct {
            cond: Thread.Condition = .{},
            value: ?ResultType = null,
            waits: usize = 0,
            allocator: Allocator,

            fn enter(self: *Caller) void {
                self.waits += 1;
            }

            fn leave(self: *Caller) void {
                self.waits -= 1;
            }

            fn destroy(self: *Caller) void {
                if (self.waits == 0) {
                    self.allocator.destroy(self);
                }
            }
        };

        pub fn init(allocator: Allocator) Self {
            return .{
                .allocator = allocator,
                .inflight = .init(allocator),
            };
        }

        /// Only one execution is in-flight for a given key at a
        /// time. If a duplicate comes in, the duplicate caller waits for the
        /// original to complete and receives the same results.
        /// The `key` is not stored after `do` called.
        pub fn do(
            self: *Self,
            key: Key,
            ctx: anytype,
            task: *const fn (@TypeOf(ctx)) ResultType,
        ) ResultType {
            self.mutex.lock();
            const calling = self.inflight.get(key);
            if (calling) |caller| {
                defer self.mutex.unlock();
                caller.enter();
                while (caller.value == null) {
                    caller.cond.wait(&self.mutex);
                }
                caller.leave();
                const value = caller.value;
                caller.destroy();
                return value.?;
            }

            const caller = self.allocator.create(Caller) catch |err| {
                @branchHint(.unlikely);
                self.mutex.unlock();
                return err;
            };
            caller.* = .{ .allocator = self.allocator };
            self.inflight.put(key, caller) catch |err| {
                @branchHint(.unlikely);
                defer self.mutex.unlock();
                caller.destroy();
                return err;
            };
            self.mutex.unlock();

            caller.value = task(ctx);
            caller.cond.broadcast();

            self.mutex.lock();
            defer self.mutex.unlock();
            _ = self.inflight.remove(key);
            const value = caller.value;
            caller.destroy();
            return value.?;
        }

        pub fn deinit(self: *Self) void {
            self.inflight.deinit();
        }
    };
}

test "Group.do in single thread" {
    const I32Group = Group(anyerror!i32);
    const allocator = std.testing.allocator;
    var group = I32Group.init(allocator);
    defer group.deinit();

    const value = try group.do("k1", {}, struct {
        fn do(_: void) !i32 {
            return 1;
        }
    }.do);
    try expectEqual(1, value);

    _ = group.do("k2", {}, struct {
        fn do(_: void) !i32 {
            return error.Error;
        }
    }.do) catch |err| {
        try expectEqual(error.Error, err);
        return;
    };
    unreachable;
}

test "Group.do in multiple threads" {
    const I32Group = Group(anyerror!i32);
    const allocator = std.testing.allocator;
    var group = I32Group.init(allocator);
    defer group.deinit();

    const Task = struct {
        group: *I32Group,
        cnt: i32 = 0,

        fn incr(self: *@This()) !i32 {
            std.Thread.sleep(100 * std.time.ns_per_ms);
            self.cnt += 1;
            return self.cnt;
        }

        fn do(self: *@This(), key: Key, wg: *std.Thread.WaitGroup) void {
            const v = self.group.do(key, self, incr) catch unreachable;
            expectEqual(1, v) catch unreachable;
            wg.finish();
        }
    };

    var wg = Thread.WaitGroup{};
    var task = Task{ .group = &group };
    const threads = 128;
    wg.startMany(threads);
    for (0..threads) |_| {
        _ = try std.Thread.spawn(.{}, Task.do, .{ &task, "key1", &wg });
    }
    wg.wait();

    try expectEqual(1, task.cnt);
}
