const std = @import("std");
const Thread = std.Thread;
const Allocator = std.mem.Allocator;
const expectEqual = std.testing.expectEqual;

const Rc = @import("rc.zig").Rc;
const Slice = @import("slice.zig").Slice;

pub const Key = Slice(u8);

pub fn Group(comptime Result: type) type {
    return struct {
        const Self = @This();

        mutex: Thread.Mutex = .{},
        allocator: Allocator,
        inflight: std.StringHashMap(*Inflight),

        const Inflight = struct {
            cond: Thread.Condition = .{},
            result: ?Result = null,
            waits: usize = 1,
            allocator: Allocator,

            fn wait(self: *Inflight, mutex: *Thread.Mutex) void {
                self.waits += 1;
                while (self.result == null) {
                    self.cond.wait(mutex);
                }
            }

            fn deinit(self: *Inflight) void {
                self.waits -= 1;
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
            task: *const fn (@TypeOf(ctx), Key) Result,
        ) Result {
            const rawkey = key.slice();
            self.mutex.lock();
            if (self.inflight.get(rawkey)) |inflight| {
                defer self.mutex.unlock();
                inflight.wait(&self.mutex);
                defer inflight.deinit();
                return inflight.result.?;
            }

            const inflight = self.allocator.create(Inflight) catch |err| {
                @branchHint(.unlikely);
                self.mutex.unlock();
                return err;
            };
            inflight.* = .{ .allocator = self.allocator };
            self.inflight.put(rawkey, inflight) catch |err| {
                @branchHint(.unlikely);
                defer self.mutex.unlock();
                inflight.deinit();
                return err;
            };
            self.mutex.unlock();

            inflight.result = task(ctx, key);
            inflight.cond.broadcast();

            self.mutex.lock();
            defer self.mutex.unlock();
            _ = self.inflight.remove(rawkey);
            defer inflight.deinit();
            return inflight.result.?;
        }

        pub fn deinit(self: *Self) void {
            self.inflight.deinit();
            self.* = undefined;
        }
    };
}

test "Group.do in single thread" {
    const I32Group = Group(anyerror!i32);
    const allocator = std.testing.allocator;
    var group = I32Group.init(allocator);
    defer group.deinit();

    const k1 = Key.static("k1");
    defer k1.deinit();

    const value = try group.do(k1, {}, struct {
        fn do(_: void, _: Key) !i32 {
            return 1;
        }
    }.do);
    try expectEqual(1, value);

    const k2 = Key.static("k2");
    defer k2.deinit();

    _ = group.do(k2, {}, struct {
        fn do(_: void, _: Key) !i32 {
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

        fn incr(self: *@This(), _: Key) !i32 {
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

    var key1 = Key.static("key1");
    defer key1.deinit();

    var wg = Thread.WaitGroup{};
    var task = Task{ .group = &group };
    const threads = 128;
    wg.startMany(threads);
    for (0..threads) |_| {
        _ = try std.Thread.spawn(.{}, Task.do, .{ &task, key1, &wg });
    }
    wg.wait();

    try expectEqual(1, task.cnt);
}
