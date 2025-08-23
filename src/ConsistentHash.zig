const std = @import("std");
const Allocator = std.mem.Allocator;
const Crc32 = std.hash.Crc32;
const expectEqual = std.testing.expectEqual;
const sort = @import("sort.zig");

const Self = @This();

const replica_start = 0;

pub const Key = @import("slice.zig").Bytes;
pub const Hash = fn (data: []const u8) u32;
const HashMap = std.HashMap(
    u32,
    Key,
    std.hash_map.AutoContext(u32),
    std.hash_map.default_max_load_percentage,
);

replicas: usize,
hash: *const Hash,
keys: std.ArrayList(u32), // items is sorted.
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

/// Returns true if the key already exists.
pub fn add(self: *Self, key: Key) !bool {
    if (try self.hasKey(key)) {
        return true;
    }
    for (replica_start..replica_start + self.replicas) |i| {
        const replica_key = try allocBuildReplicaKey(self.allocator, key, i);
        defer replica_key.deinit();
        const hash = self.hash(replica_key.val());
        try self.keys.append(self.allocator, hash);
        try self.hash_map.put(hash, key.clone());
    }
    std.mem.sort(u32, self.keys.items, {}, std.sort.asc(u32));
    return false;
}

pub fn hasKey(self: *Self, key: Key) !bool {
    const first_replica = try allocBuildReplicaKey(
        self.allocator,
        key,
        replica_start,
    );
    defer first_replica.deinit();

    const first_replica_hash = self.hash(first_replica.val());
    if (self.hash_map.contains(first_replica_hash)) {
        return true;
    }
    return false;
}

fn allocBuildReplicaKey(allocator: Allocator, key: Key, replica: usize) !Key {
    const data = try std.fmt.allocPrint(
        allocator,
        "{}{s}",
        .{ replica, key.val() },
    );
    return Key.move(data, allocator);
}

const BinarySearchContext = struct {
    hash: u32,
    items: []u32,

    fn predict(ctx: BinarySearchContext, i: usize) bool {
        return ctx.items[i] >= ctx.hash;
    }
};

// Gets the closest item in the hash to the provided key.
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

    // Means we have cycled back to the first replica.
    if (idx == self.keys.items.len) {
        idx = 0;
    }

    const found = self.hash_map.get(self.keys.items[idx]);
    return if (found) |f| f.clone() else null;
}

pub fn isEmpty(self: *const Self) bool {
    return self.keys.items.len == 0;
}

pub fn deinit(self: *Self) void {
    self.keys.deinit(self.allocator);
    var it = self.hash_map.valueIterator();
    while (it.next()) |k| {
        k.deinit();
    }
    self.hash_map.deinit();
    self.* = undefined;
}

test "consistency" {
    const allocator = std.testing.allocator;
    var ch1: Self = .init(allocator, 3, null);
    defer ch1.deinit();

    var ch2: Self = .init(allocator, 3, null);
    defer ch2.deinit();

    _ = try ch1.add(Key.static("key1"));
    _ = try ch1.add(Key.static("key2"));

    _ = try ch2.add(Key.static("key1"));
    _ = try ch2.add(Key.static("key2"));

    try expectEqual(
        ch1.get(Key.static("key11")),
        ch2.get(Key.static("key11")),
    );
    try expectEqual(
        ch1.get(Key.static("key22")),
        ch2.get(Key.static("key22")),
    );
}

test "hashing" {
    const allocator = std.testing.allocator;
    var ch1: Self = .init(allocator, @as(u8, 3), struct {
        fn hash(data: []const u8) u32 {
            return std.fmt.parseInt(u32, data, 10) catch unreachable;
        }
    }.hash);
    defer ch1.deinit();

    _ = try ch1.add(Key.static("2"));
    _ = try ch1.add(Key.static("4"));
    _ = try ch1.add(Key.static("6"));

    const exists = try ch1.add(Key.static("6"));
    try expectEqual(true, exists);

    try expectEqual(ch1.get(Key.static("2")).?.val(), "2");
    try expectEqual(ch1.get(Key.static("11")).?.val(), "2");
    try expectEqual(ch1.get(Key.static("27")).?.val(), "2");
    try expectEqual(ch1.get(Key.static("23")).?.val(), "4");
}
