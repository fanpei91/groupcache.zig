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
            value: ?anyerror!ResultType = null,
        };

        pub fn init(allocator: Allocator) Self {
            return .{
                .allocator = allocator,
                .inflight = .init(allocator),
            };
        }

        pub fn do(
            self: *Self,
            key: Key,
            ctx: anytype,
            task: *const fn (@TypeOf(ctx)) anyerror!ResultType,
        ) anyerror!ResultType {
            self.mutex.lock();
            const calling = self.inflight.get(key);
            if (calling) |caller| {
                defer self.mutex.unlock();
                while (caller.value == null) {
                    caller.cond.wait(&self.mutex);
                }
                return caller.value.?;
            }

            const caller = try self.allocator.create(Caller);
            defer self.allocator.destroy(caller);

            caller.* = .{};
            self.inflight.put(key, caller) catch {
                @branchHint(.unlikely);
            };
            self.mutex.unlock();

            caller.value = task(ctx);
            caller.cond.broadcast();

            self.mutex.lock();
            _ = self.inflight.remove(key);
            self.mutex.unlock();

            return caller.value.?;
        }

        pub fn deinit(self: *Self) void {
            self.inflight.deinit();
        }
    };
}

test "Group.do in single thread" {
    const I32Group = Group(i32);
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
    const I32Group = Group(i32);
    const allocator = std.testing.allocator;
    var group = I32Group.init(allocator);
    defer group.deinit();

    var mutex = Thread.Mutex{};
    var cond = Thread.Condition{};

    const Task = struct {
        group: *I32Group,
        cnt: i32 = 0,
        mutex: *Thread.Mutex,
        cond: *Thread.Condition,

        fn incr(self: *@This()) !i32 {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.cond.wait(self.mutex);
            self.cnt += 1;
            return self.cnt;
        }

        fn do(self: *@This(), key: Key, wg: *Thread.WaitGroup) !void {
            defer wg.finish();
            _ = try self.group.do(key, self, incr);
        }
    };

    var task = Task{
        .group = &group,
        .mutex = &mutex,
        .cond = &cond,
    };

    var wg = Thread.WaitGroup{};
    const num = 3;
    wg.startMany(num);
    for (0..num) |_| {
        _ = try Thread.spawn(
            .{ .allocator = allocator },
            Task.do,
            .{ &task, "key1", &wg },
        );
    }
    std.time.sleep(1 * std.time.ns_per_s);
    cond.broadcast();
    wg.wait();
    try expectEqual(1, task.cnt);
}
