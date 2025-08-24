const std = @import("std");
const Allocator = std.mem.Allocator;
const Crc32 = std.hash.Crc32;
const sort = @import("sort.zig");

const Self = @This();

pub const Key = @import("mem.zig").Bytes;
pub const Hash = fn (data: []const u8) u32;
const HashMap = std.HashMap(
    u32,
    Key,
    std.hash_map.AutoContext(u32),
    std.hash_map.default_max_load_percentage,
);

replicas: usize,
hash: *const Hash,
keys: std.ArrayList(u32),
hash_map: HashMap,
allocator: Allocator,

pub fn init(allocator: Allocator, replicas: usize, hash: ?*const Hash) Self {
    return .{
        .replicas = replicas,
        .hash = hash orelse Crc32.hash,
        .keys = .empty,
        .hash_map = .init(allocator),
        .allocator = allocator,
    };
}

pub fn add(self: *Self, key: Key) !bool {
    if (try self.hasKey(key)) {
        return true;
    }
    for (0..self.replicas) |i| {
        const replica_key = try buildReplicaKey(self.allocator, key, i);
        defer replica_key.deinit();
        const hash = self.hash(replica_key.val());
        try self.keys.append(self.allocator, hash);
        self.hash_map.put(hash, key.clone()) catch |err| {
            _ = self.keys.pop();
            return err;
        };
    }
    std.mem.sort(u32, self.keys.items, {}, std.sort.asc(u32));
    return false;
}

pub fn hasKey(self: *Self, key: Key) !bool {
    const first_replica = try buildReplicaKey(
        self.allocator,
        key,
        0,
    );
    defer first_replica.deinit();

    const first_replica_hash = self.hash(first_replica.val());
    return self.hash_map.contains(first_replica_hash);
}

fn buildReplicaKey(allocator: Allocator, key: Key, replica: usize) !Key {
    const data = try std.fmt.allocPrint(
        allocator,
        "{}{s}",
        .{ replica, key.val() },
    );
    return Key.move(data, allocator);
}

const BinarySearchContext = struct {
    hash: u32,
    items: []const u32,

    fn predict(ctx: BinarySearchContext, i: usize) bool {
        return ctx.items[i] >= ctx.hash;
    }
};

pub fn get(self: *const Self, key: Key) ?Key {
    if (self.isEmpty()) return null;

    var idx = sort.binarySearch(
        self.keys.items.len,
        BinarySearchContext{
            .hash = self.hash(key.val()),
            .items = self.keys.items,
        },
        BinarySearchContext.predict,
    );

    if (idx == self.keys.items.len) {
        idx = 0;
    }

    const found = self.hash_map.get(self.keys.items[idx]);
    return if (found) |f| f.clone() else null;
}

pub fn isEmpty(self: *const Self) bool {
    return self.keys.items.len == 0;
}

pub fn reset(self: *Self) void {
    var it = self.hash_map.valueIterator();
    while (it.next()) |k| {
        k.deinit();
    }
    self.keys.clearAndFree(self.allocator);
    self.hash_map.clearAndFree();
}

pub fn deinit(self: *Self) void {
    self.reset();
    self.keys.deinit(self.allocator);
    self.hash_map.deinit();
    self.* = undefined;
}
