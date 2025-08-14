const std = @import("std");
const Allocator = std.mem.Allocator;
const expectEqual = std.testing.expectEqual;
const AutoContext = std.hash_map.AutoContext;

/// Context must be a struct type with two member functions:
///   hash(self, K) u64
///   eql(self, K, K) bool
pub fn Cache(
    comptime K: type,
    comptime V: type,
    comptime Context: type,
) type {
    return struct {
        const Self = @This();
        pub const Entry = struct {
            key: K,
            value: V,
        };
        pub const EntryEvictor = struct {
            ptr: *anyopaque,
            evict: *const fn (ptr: *anyopaque, entry: Entry) void,
        };
        const DoublyLinkedList = std.DoublyLinkedList(Entry);
        const Node = DoublyLinkedList.Node;

        pub const EntryIterator = struct {
            current: ?*Node,

            pub fn next(self: *EntryIterator) ?Entry {
                const current = self.current orelse return null;
                self.current = current.next;
                return current.data;
            }
        };

        allocator: Allocator,
        cache: std.HashMap(K, *Node, Context, 80),
        ll: DoublyLinkedList = .{},
        max_entries: usize,
        entry_evictor: ?EntryEvictor,

        pub fn init(
            allocator: std.mem.Allocator,
            max_entries: usize,
            entry_evictor: ?EntryEvictor,
        ) Self {
            return .{
                .allocator = allocator,
                .cache = .init(allocator),
                .max_entries = max_entries,
                .entry_evictor = entry_evictor,
            };
        }

        pub fn iterator(self: *const Self) EntryIterator {
            return .{
                .current = self.ll.first,
            };
        }

        /// Returns the old value if key already exists.
        pub fn add(self: *Self, key: K, value: V) !?V {
            const existing_node = self.cache.get(key);
            if (existing_node) |node| {
                self.markAsRecentlyUsed(node);
                const old_value = node.data.value;
                node.data.value = value;
                return old_value;
            }
            const new_node = try self.allocator.create(Node);
            new_node.data = .{
                .key = key,
                .value = value,
            };
            if (self.ll.len > 0) {
                self.ll.prepend(new_node);
            } else {
                self.ll.append(new_node);
            }
            try self.cache.put(key, new_node);
            if (self.max_entries != 0 and self.ll.len > self.max_entries) {
                self.removeOldest();
            }
            return null;
        }

        fn markAsRecentlyUsed(self: *Self, node: *Node) void {
            self.ll.remove(node);
            self.ll.prepend(node);
        }

        pub fn get(self: *Self, key: K) ?V {
            const node = self.cache.get(key) orelse return null;
            self.markAsRecentlyUsed(node);
            return node.data.value;
        }

        pub fn remove(self: *Self, key: K) void {
            const node = self.cache.get(key) orelse return;
            self.removeNode(node);
        }

        pub fn removeOldest(self: *Self) void {
            const node = self.ll.last orelse return;
            self.removeNode(node);
        }

        fn removeNode(self: *Self, node: *Node) void {
            const entry = node.data;
            _ = self.cache.remove(entry.key);
            self.ll.remove(node);
            if (self.entry_evictor) |evictor| {
                evictor.evict(evictor.ptr, entry);
            }
            self.allocator.destroy(node);
        }

        pub fn len(self: *Self) usize {
            return self.ll.len;
        }

        pub fn deinit(self: *Self) void {
            var node = self.ll.first;
            while (node) |n| {
                node = n.next;
                self.allocator.destroy(n);
            }
            self.cache.deinit();
            self.* = undefined;
        }
    };
}

test "Cache.add" {
    const allocator = std.testing.allocator;
    var cache = Cache(i32, i32, AutoContext(i32)).init(
        allocator,
        3,
        null,
    );
    defer cache.deinit();

    var old_value = try cache.add(1, 11);
    try expectEqual(null, old_value);

    old_value = try cache.add(1, 22);
    try expectEqual(11, old_value);
}

test "Cache.get" {
    const allocator = std.testing.allocator;
    var cache = Cache(i32, i32, AutoContext(i32)).init(
        allocator,
        3,
        null,
    );
    defer cache.deinit();

    try expectEqual(null, cache.get(1));

    _ = try cache.add(1, 1);
    try expectEqual(1, cache.get(1));

    _ = try cache.add(2, 2);
    _ = try cache.add(3, 3);
    _ = try cache.add(4, 4);

    try expectEqual(null, cache.get(1));
}

test "Cache.len" {
    const allocator = std.testing.allocator;
    var cache = Cache(i32, i32, AutoContext(i32)).init(
        allocator,
        3,
        null,
    );
    defer cache.deinit();

    _ = try cache.add(1, 1);
    _ = try cache.add(2, 2);
    _ = try cache.add(3, 3);
    _ = try cache.add(4, 4);

    try expectEqual(3, cache.len());
}

test "Cache.iterator" {
    const allocator = std.testing.allocator;
    const I32LRUCache = Cache(i32, i32, AutoContext(i32));
    const Entry = I32LRUCache.Entry;
    var cache = I32LRUCache.init(
        allocator,
        3,
        null,
    );
    defer cache.deinit();

    _ = try cache.add(1, 1);
    _ = try cache.add(2, 2);
    _ = try cache.add(3, 3);

    var iterator = cache.iterator();

    var entry = iterator.next();
    try expectEqual(Entry{ .key = 3, .value = 3 }, entry);

    entry = iterator.next();
    try expectEqual(Entry{ .key = 2, .value = 2 }, entry);

    entry = iterator.next();
    try expectEqual(Entry{ .key = 1, .value = 1 }, entry);

    entry = iterator.next();
    try expectEqual(null, entry);
}

test "Cache.evict" {
    const allocator = std.testing.allocator;
    const I32LRUCache = Cache(i32, i32, AutoContext(i32));
    const Entry = I32LRUCache.Entry;

    const I32EntryEvictor = struct {
        const Self = @This();

        entry: ?Entry = null,

        fn evictor(self: *Self) I32LRUCache.EntryEvictor {
            return .{
                .ptr = self,
                .evict = evict,
            };
        }

        fn evict(ptr: *anyopaque, entry: Entry) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.entry = entry;
        }
    };

    var evictor: I32EntryEvictor = .{};
    var cache = I32LRUCache.init(
        allocator,
        3,
        evictor.evictor(),
    );
    defer cache.deinit();

    _ = try cache.add(1, 1);
    _ = try cache.add(2, 2);
    _ = try cache.add(3, 3);
    _ = try cache.add(4, 4);

    try expectEqual(Entry{ .key = 1, .value = 1 }, evictor.entry);
}

test "Cache.remove" {
    const allocator = std.testing.allocator;
    const I32LRUCache = Cache(i32, i32, AutoContext(i32));

    var cache = I32LRUCache.init(
        allocator,
        3,
        null,
    );
    defer cache.deinit();

    _ = try cache.add(1, 1);
    _ = try cache.add(2, 2);
    _ = try cache.add(3, 3);

    cache.remove(1);
    try expectEqual(null, cache.get(1));

    cache.removeOldest();
    try expectEqual(null, cache.get(2));

    try expectEqual(1, cache.len());
}
