const std = @import("std");
const Allocator = std.mem.Allocator;
const Crc32 = std.hash.Crc32;
const expectEqual = std.testing.expectEqual;
const sort = @import("sort.zig");

const Self = @This();

pub const Key = []const u8;
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
        .keys = .init(allocator),
        .hash_map = .init(allocator),
        .allocator = allocator,
    };
}

pub fn add(self: *Self, key: Key) !void {
    for (0..self.replicas) |i| {
        const data = try std.fmt.allocPrint(
            self.allocator,
            "{}{s}",
            .{ i, key },
        );
        defer self.allocator.free(data);
        const hash = self.hash(data);
        try self.keys.append(hash);
        try self.hash_map.put(hash, key);
    }
    std.mem.sort(u32, self.keys.items, {}, std.sort.asc(u32));
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
            .hash = self.hash(key),
            .items = self.keys.items,
        },
        BinarySearchContext.predict,
    );

    // Means we have cycled back to the first replica.
    if (idx == self.keys.items.len) {
        idx = 0;
    }

    return self.hash_map.get(self.keys.items[idx]);
}

pub const KeyIterator = struct {
    value_iter: HashMap.ValueIterator,

    pub fn next(self: *KeyIterator) ?Key {
        const value = self.value_iter.next() orelse return null;
        return value.*;
    }
};

pub fn keyIterator(self: *const Self) KeyIterator {
    return .{
        .value_iter = self.hash_map.valueIterator(),
    };
}

pub fn isEmpty(self: *const Self) bool {
    return self.keys.items.len == 0;
}

pub fn deinit(self: *Self) void {
    self.keys.deinit();
    self.hash_map.deinit();
    self.* = undefined;
}

test "consistency" {
    const allocator = std.testing.allocator;
    var ch1: Self = .init(allocator, 3, null);
    defer ch1.deinit();

    var ch2: Self = .init(allocator, 3, null);
    defer ch2.deinit();

    try ch1.add("key1");
    try ch1.add("key2");

    try ch2.add("key1");
    try ch2.add("key2");

    try expectEqual(ch1.get("key11"), ch2.get("key11"));
    try expectEqual(ch1.get("key22"), ch2.get("key22"));
}

test "hashing" {
    const allocator = std.testing.allocator;
    var ch1: Self = .init(allocator, @as(u8, 3), struct {
        fn hash(data: []const u8) u32 {
            return std.fmt.parseInt(u32, data, 10) catch unreachable;
        }
    }.hash);
    defer ch1.deinit();

    try ch1.add("2");
    try ch1.add("4");
    try ch1.add("6");

    try expectEqual(ch1.get("2"), "2");
    try expectEqual(ch1.get("11"), "2");
    try expectEqual(ch1.get("27"), "2");

    try expectEqual(ch1.get("23"), "4");
}

test keyIterator {
    const allocator = std.testing.allocator;
    var ch: Self = .init(allocator, 3, null);
    defer ch.deinit();

    var keys = std.HashMap(
        Key,
        bool,
        std.hash_map.StringContext,
        80,
    ).init(allocator);
    defer keys.deinit();

    try keys.put("k1", true);
    try keys.put("k2", true);
    try keys.put("k3", true);

    try ch.add("k1");
    try ch.add("k2");
    try ch.add("k3");

    var it = ch.keyIterator();
    while (it.next()) |k| {
        _ = keys.remove(k);
    }

    try expectEqual(0, keys.count());
}
