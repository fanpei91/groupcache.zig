const std = @import("std");
const Allocator = std.mem.Allocator;
const protocol = @import("protocol/groupcachepb.pb.zig");

const FlightGroup = @import("singleflight.zig").Group(anyerror!Value);
const LRUCache = @import("lru.zig").Cache(
    Key,
    Value,
    KeyContext,
);
const KeyContext = struct {
    pub fn hash(_: @This(), s: Key) u64 {
        return std.hash_map.hashString(s.slice());
    }
    pub fn eql(_: @This(), a: Key, b: Key) bool {
        return std.hash_map.eqlString(a.slice(), b.slice());
    }
};
pub const AtomicUsize = @import("atomic.zig").Atomic(usize);
pub const GetRequest = protocol.GetRequest;
pub const GetResponse = protocol.GetResponse;

const Slice = @import("slice.zig").Slice;
const Rc = @import("rc.zig").Rc;

pub const Bytes = Slice(u8);
pub const Key = Bytes;
pub const Value = Rc(Bytes);

pub const ProtoGetter = struct {
    pub const Vtable = struct {
        get: *const fn (*anyopaque, Allocator, GetRequest) anyerror!GetResponse,
        name: *const fn (*anyopaque) Bytes,
    };
    ptr: *anyopaque,
    vtable: *const Vtable,

    const Self = @This();

    fn get(
        self: *Self,
        allocator: Allocator,
        req: GetRequest,
    ) !GetResponse {
        return try self.vtable.get(self.ptr, allocator, req);
    }

    fn name(self: *Self) Bytes {
        return self.vtable.name(self.ptr);
    }
};

pub const Getter = struct {
    pub const Vtable = struct {
        get: *const fn (*anyopaque, Key) anyerror!Value,
    };

    ptr: *anyopaque,
    vtable: *const Vtable,

    fn get(
        self: *Getter,
        key: Key,
    ) anyerror!Value {
        return try self.vtable.get(self.ptr, key);
    }
};

pub const PeerPicker = struct {
    pub const Vtable = struct {
        pickPeer: *const fn (ptr: *anyopaque, key: Key) ?ProtoGetter,
    };
    ptr: *anyopaque,
    vtable: *const Vtable,

    fn pickPeer(self: *PeerPicker, key: Key) ?ProtoGetter {
        return self.vtable.pickPeer(self.ptr, key);
    }
};

pub const GroupCache = struct {
    const Self = @This();

    pub const Stats = struct {
        /// any Get request, including from peers
        gets: AtomicUsize = .init(0),

        /// either cache was good
        cache_hits: AtomicUsize = .init(0),

        /// either remote load or remote cache hit (not an error)
        peer_loads: AtomicUsize = .init(0),

        peer_errors: AtomicUsize = .init(0),

        /// (gets - cacheHits)
        loads: AtomicUsize = .init(0),

        /// after single flight
        loads_deduped: AtomicUsize = .init(0),

        /// total good local loads
        local_loads: AtomicUsize = .init(0),

        /// total bad local loads
        local_load_errs: AtomicUsize = .init(0),

        /// gets that came over the network from peers
        server_requests: AtomicUsize = .init(0),
    };

    pub const Options = struct {
        getter: Getter,
        peers: PeerPicker,
        /// limit for sum of main_cache and hot_cache size.
        cached_bytes: usize,
        rand: std.Random = std.crypto.random,
    };

    allocator: Allocator,
    name: Bytes,
    stats: Stats = .{},
    main_cache: *Cache,
    hot_cache: *Cache,
    load_group: FlightGroup,
    options: Options,

    pub fn init(
        allocator: Allocator,
        name: Bytes,
        options: Options,
    ) Allocator.Error!Self {
        return .{
            .allocator = allocator,
            .name = try name.dupe(allocator),
            .main_cache = try .init(allocator),
            .hot_cache = try .init(allocator),
            .load_group = .init(allocator),
            .options = options,
        };
    }

    pub fn get(self: *Self, key: Key) !Value {
        self.stats.gets.add(1);

        const cached_value = self.lookupCache(key);
        if (cached_value) |v| {
            self.stats.cache_hits.add(1);
            return v;
        }

        return try self.load(key);
    }

    fn lookupCache(self: *Self, key: Key) ?Value {
        if (self.options.cached_bytes == 0) {
            return null;
        }

        const value = self.main_cache.get(key);
        if (value) |v| {
            return v;
        }

        return self.hot_cache.get(key);
    }

    fn load(self: *Self, key: Key) !Value {
        self.stats.loads.add(1);
        return try self.load_group.do(key, self, loadTask);
    }

    fn loadTask(self: *Self, key: Key) !Value {
        const cached_value = self.lookupCache(key);
        if (cached_value) |cached| {
            self.stats.cache_hits.add(1);
            return cached;
        }

        self.stats.loads_deduped.add(1);

        var peer = self.options.peers.pickPeer(key);
        if (peer) |*p| {
            if (self.getFromPeer(p, key)) |peer_value| {
                self.stats.peer_loads.add(1);
                return peer_value;
            } else |err| {
                const name = p.name();
                defer name.deinit();
                std.log.err("get value from peer({s}) by key({s}): {}", .{
                    name.slice(),
                    key.slice(),
                    err,
                });
                self.stats.peer_errors.add(1);
            }
        }

        const local_value = self.getFromLocal(key) catch |err| {
            self.stats.local_load_errs.add(1);
            return err;
        };
        self.stats.local_loads.add(1);
        return local_value;
    }

    fn getFromPeer(self: *Self, peer: *ProtoGetter, key: Key) !Value {
        const req: GetRequest = .{
            .group = self.name.slice(),
            .key = key.slice(),
        };
        const res = try peer.get(self.allocator, req);

        const value = res.value orelse return error.MissingPeerResponseValue;
        const val_slice = Bytes.move(@constCast(value), self.allocator);
        const val_rc = Value.init(self.allocator, val_slice) catch |err| {
            val_slice.deinit();
            return err;
        };

        const pop = self.options.rand.intRangeAtMost(u8, 0, 9) == 0;
        if (pop) {
            self.populateCache(key, val_rc, self.hot_cache) catch |err| {
                val_rc.deinit();
                return err;
            };
        }
        return val_rc;
    }

    fn getFromLocal(self: *Self, key: Key) !Value {
        const value = try self.options.getter.get(key);
        self.populateCache(key, value, self.main_cache) catch |err| {
            value.deinit();
            return err;
        };
        return value;
    }

    fn populateCache(
        self: *Self,
        key: Key,
        value: Value,
        cache: *Cache,
    ) !void {
        if (self.options.cached_bytes == 0) {
            return;
        }
        try cache.add(key, value);
        while (true) {
            const main_bytes = self.main_cache.bytes();
            const hot_bytes = self.hot_cache.bytes();
            if (main_bytes + hot_bytes < self.options.cached_bytes) {
                break;
            }

            var victim = self.main_cache;
            if (hot_bytes > main_bytes / 8) {
                victim = self.hot_cache;
            }
            victim.removeOldest();
        }
    }

    pub fn deinit(self: *Self) void {
        self.name.deinit();
        self.load_group.deinit();
        self.main_cache.deinit();
        self.hot_cache.deinit();
        self.* = undefined;
    }
};

/// Cache is a wrapper around an *lru.Cache that adds synchronization,
/// counts the size of all keys and values.
const Cache = struct {
    const Self = @This();

    const Stats = struct {
        bytes: usize,
        items: usize,
        gets: usize,
        hits: usize,
        evictions: usize,
    };

    allocator: Allocator,
    lru: LRUCache,
    mu: std.Thread.Mutex = .{},

    /// of all keys and values
    nbytes: usize = 0,

    nhit: usize = 0,
    nget: usize = 0,
    nevict: usize = 0,

    fn init(allocator: Allocator) Allocator.Error!*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            .lru = .init(allocator, 0, .{
                .ptr = self,
                .evict = evict,
            }),
        };
        return self;
    }

    fn add(self: *Self, key: Key, value: Value) !void {
        self.mu.lock();
        defer self.mu.unlock();

        const keydup = try key.dupe(self.allocator);
        const valclone = value.clone();
        const old_value = self.lru.add(keydup, valclone) catch |err| {
            keydup.deinit();
            valclone.deinit();
            return err;
        };
        self.nbytes += keydup.len();
        self.nbytes += valclone.val().len();

        if (old_value) |old| {
            self.nbytes -= old.val().len();
            old.deinit();
            self.nbytes -= keydup.len();
            keydup.deinit();
        }
    }

    fn get(self: *Self, key: Key) ?Value {
        self.mu.lock();
        defer self.mu.unlock();

        self.nget += 1;

        const value = self.lru.get(key);
        if (value) |v| {
            self.nhit += 1;
            return v.clone();
        }
        return null;
    }

    fn removeOldest(self: *Self) void {
        self.mu.lock();
        defer self.mu.unlock();
        self.lru.removeOldest();
    }

    pub fn stats(self: *Self) Stats {
        self.mu.lock();
        defer self.mu.unlock();
        return .{
            .bytes = self.nbytes,
            .items = self.items_locked(),
            .gets = self.nget,
            .hits = self.nhit,
            .evictions = self.nevict,
        };
    }

    fn items(self: *Self) usize {
        self.mu.lock();
        defer self.mu.unlock();
        return self.items_locked();
    }

    fn items_locked(self: *Self) usize {
        return self.lru.len();
    }

    fn bytes(self: *Self) usize {
        self.mu.lock();
        defer self.mu.unlock();
        return self.nbytes;
    }

    fn evict(ptr: *anyopaque, entry: LRUCache.Entry) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        {
            self.mu.lock();
            defer self.mu.unlock();
            self.nbytes -= entry.key.len();
            self.nbytes -= entry.value.val().len();
            self.nevict += 1;
        }
        entry.key.deinit();
        entry.value.deinit();
    }

    fn deinit(self: *Self) void {
        var entry_iter = self.lru.iterator();
        while (entry_iter.next()) |entry| {
            entry.key.deinit();
            entry.value.deinit();
        }
        self.lru.deinit();
        self.allocator.destroy(self);
    }
};

test GroupCache {
    const MockProtoGetter = struct {
        const Self = @This();

        identifier: Bytes,
        ngets: usize = 0,

        fn protoGetter(self: *Self) ProtoGetter {
            return .{
                .ptr = self,
                .vtable = &.{
                    .get = get,
                    .name = name,
                },
            };
        }

        fn get(ptr: *anyopaque, allocator: Allocator, req: GetRequest) !GetResponse {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.ngets += 1;
            const res_value = try std.fmt.allocPrint(
                allocator,
                "{s}->[group: {s}, key: {s}]",
                .{
                    self.identifier.slice(),
                    req.group,
                    req.key,
                },
            );
            return .{
                .value = res_value,
                .minute_qps = 0,
            };
        }

        fn name(ptr: *anyopaque) Bytes {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return self.identifier;
        }
    };
    const MockPeerPicker = struct {
        const Self = @This();

        mock_proto_getter: *MockProtoGetter,

        fn peerPicker(self: *Self) PeerPicker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .pickPeer = pickPeer,
                },
            };
        }

        fn pickPeer(ptr: *anyopaque, key: Key) ?ProtoGetter {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (std.mem.startsWith(u8, key.slice(), "peer")) {
                return self.mock_proto_getter.protoGetter();
            }
            return null;
        }
    };
    const MockGetter = struct {
        allocator: Allocator,
        ngets: usize = 0,

        const Self = @This();

        fn getter(self: *Self) Getter {
            return .{
                .ptr = self,
                .vtable = &.{
                    .get = get,
                },
            };
        }

        fn get(ptr: *anyopaque, key: Key) !Value {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.ngets += 1;
            const rawvalue = try std.fmt.allocPrint(
                self.allocator,
                "local->[key: {s}]",
                .{key.slice()},
            );
            const value = Bytes.move(rawvalue, self.allocator);
            return try Value.init(
                self.allocator,
                value,
            );
        }
    };

    const allocator = std.testing.allocator;
    const peer_identifier = Bytes.static("peer://127.0.0.1:8080");

    var mock_proto_getter = MockProtoGetter{
        .identifier = peer_identifier,
    };
    var mock_peer_picker = MockPeerPicker{
        .mock_proto_getter = &mock_proto_getter,
    };
    var mock_getter = MockGetter{
        .allocator = allocator,
    };

    var gc = try GroupCache.init(
        allocator,
        Bytes.static("g1"),
        .{
            .cached_bytes = 128 * 1024 * 1024,
            .peers = mock_peer_picker.peerPicker(),
            .getter = mock_getter.getter(),
        },
    );
    defer gc.deinit();

    const k1 = Key.static("peer:key1");
    defer k1.deinit();
    const v1 = try gc.get(k1);
    defer v1.deinit();
    const expected_v1 = "peer://127.0.0.1:8080->[group: g1, key: peer:key1]";
    try std.testing.expectEqualStrings(
        expected_v1,
        v1.val().slice(),
    );
    const v1_1 = try gc.get(k1);
    defer v1_1.deinit();
    try std.testing.expectEqualStrings(
        expected_v1,
        v1_1.val().slice(),
    );
    try std.testing.expect(mock_proto_getter.ngets >= 1);

    const k2 = Key.static("local:key1");
    defer k2.deinit();
    const v2 = try gc.get(k2);
    defer v2.deinit();
    const expected_v2 = "local->[key: local:key1]";
    try std.testing.expectEqualStrings(
        expected_v2,
        v2.val().slice(),
    );
    const v2_1 = try gc.get(k2);
    defer v2_1.deinit();
    try std.testing.expectEqualStrings(
        expected_v2,
        v2_1.val().slice(),
    );
    try std.testing.expectEqual(1, mock_getter.ngets);
}

test "tests:modules" {
    _ = @import("atomic.zig");
    _ = @import("ConsistentHash.zig");
    _ = @import("lru.zig");
    _ = @import("singleflight.zig");
    _ = @import("sort.zig");
    _ = @import("slice.zig");
    _ = @import("rc.zig");
    std.testing.refAllDecls(@This());
}
