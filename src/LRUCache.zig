const std = @import("std");
const Allocator = std.mem.Allocator;
const AutoContext = std.hash_map.AutoContext;
const DoublyLinkedList = std.DoublyLinkedList;
const Node = DoublyLinkedList.Node;

const mem = @import("mem.zig");
const Key = mem.Bytes;
const Value = mem.Bytes;

allocator: Allocator,
cache: std.HashMap(Key, *Entry, mem.BytesHashContext, 80),
ll: DoublyLinkedList = .{},
max_entries: usize,
evictor: ?Evictor,

const Self = @This();

const Entry = struct {
    key: Key,
    value: Value,

    node: Node,
    fn fromNode(node: *Node) *Entry {
        return @fieldParentPtr("node", node);
    }
};

pub const Evictor = struct {
    ptr: *anyopaque,
    evict: *const fn (ptr: *anyopaque, key: Key, value: Value) void,
};

pub fn init(
    allocator: Allocator,
    max_entries: usize,
    evictor: ?Evictor,
) Self {
    return .{
        .allocator = allocator,
        .cache = .init(allocator),
        .max_entries = max_entries,
        .evictor = evictor,
    };
}

pub fn add(self: *Self, key: Key, value: Value) !?Value {
    const previous = self.cache.get(key);
    if (previous) |entry| {
        self.markAsRecentlyUsed(&entry.node);
        const old_value = entry.value;
        entry.value = value.clone();
        return old_value;
    }
    const new_entry = try self.allocator.create(Entry);
    new_entry.* = .{
        .key = key.clone(),
        .value = value.clone(),
        .node = .{},
    };
    self.ll.prepend(&new_entry.node);
    self.cache.put(key, new_entry) catch |err| {
        self.ll.remove(&new_entry.node);
        new_entry.key.deinit();
        new_entry.value.deinit();
        self.allocator.destroy(new_entry);
        return err;
    };
    if (self.max_entries != 0 and self.len() > self.max_entries) {
        self.removeOldest();
    }
    return null;
}

fn markAsRecentlyUsed(self: *Self, node: *Node) void {
    self.ll.remove(node);
    self.ll.prepend(node);
}

pub fn get(self: *Self, key: Key) ?Value {
    const entry = self.cache.get(key) orelse return null;
    self.markAsRecentlyUsed(&entry.node);
    return entry.value.clone();
}

pub fn remove(self: *Self, key: Key) void {
    const entry = self.cache.get(key) orelse return;
    self.removeNode(&entry.node);
}

pub fn removeOldest(self: *Self) void {
    const node = self.ll.last orelse return;
    self.removeNode(node);
}

fn removeNode(self: *Self, node: *Node) void {
    const entry = Entry.fromNode(node);
    _ = self.cache.remove(entry.key);
    self.ll.remove(node);

    const key = entry.key;
    defer key.deinit();

    const value = entry.value;
    defer value.deinit();

    if (self.evictor) |evictor| {
        evictor.evict(evictor.ptr, key, value);
    }

    self.allocator.destroy(entry);
}

pub fn len(self: *Self) usize {
    return self.cache.count();
}

pub fn deinit(self: *Self) void {
    var node = self.ll.first;
    while (node) |n| {
        node = n.next;
        const entry = Entry.fromNode(n);
        entry.key.deinit();
        entry.value.deinit();
        self.allocator.destroy(entry);
    }
    self.cache.deinit();
    self.* = undefined;
}
