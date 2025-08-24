const std = @import("std");
const Thread = std.Thread;
const Allocator = std.mem.Allocator;

const mem = @import("mem.zig");
pub const Key = mem.Bytes;
pub const Value = mem.Bytes;

const Self = @This();

mutex: Thread.Mutex = .{},
allocator: Allocator,
inflight: std.StringHashMap(*Inflight),

const Inflight = struct {
    cond: Thread.Condition = .{},
    result: ?anyerror!Value = null,
    waits: usize = 1,
    allocator: Allocator,

    fn wait(self: *Inflight, mutex: *Thread.Mutex) void {
        self.waits += 1;
        while (self.result == null) {
            self.cond.wait(mutex);
        }
    }

    fn value(self: *Inflight) anyerror!Value {
        const v = try self.result.?;
        return v.clone();
    }

    fn deinit(self: *Inflight) void {
        self.waits -= 1;
        if (self.waits == 0) {
            defer self.allocator.destroy(self);
            const result = self.result.?;
            const v = result catch {
                return;
            };
            v.deinit();
        }
    }
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
    task: *const fn (@TypeOf(ctx), Key) anyerror!Value,
) anyerror!Value {
    const rawkey = key.val();
    self.mutex.lock();
    if (self.inflight.get(rawkey)) |inflight| {
        defer self.mutex.unlock();
        inflight.wait(&self.mutex);
        defer inflight.deinit();
        return inflight.value();
    }

    const inflight = self.allocator.create(Inflight) catch |err| {
        self.mutex.unlock();
        return err;
    };
    inflight.* = .{ .allocator = self.allocator };
    self.inflight.put(rawkey, inflight) catch |err| {
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
    return inflight.value();
}

pub fn deinit(self: *Self) void {
    self.inflight.deinit();
    self.* = undefined;
}
