const std = @import("std");
const Allocator = std.mem.Allocator;
const protocol = @import("protocol/groupcachepb.pb.zig");
const mem = @import("mem.zig");

const FlightGroup = @import("FlightGroup.zig");
const LRUCache = @import("LRUCache.zig");

pub const cluster = @import("cluster.zig");

pub const GetRequest = protocol.GetRequest;
pub const GetResponse = protocol.GetResponse;

pub const Bytes = mem.Bytes;
pub const Key = Bytes;
pub const Value = Bytes;

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
    ) anyerror!GetResponse {
        return self.vtable.get(self.ptr, allocator, req);
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
        return self.vtable.get(self.ptr, key);
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

    const AtomicUsize = std.atomic.Value(usize);

    pub const Stats = struct {
        gets: AtomicUsize = .init(0),
        cache_hits: AtomicUsize = .init(0),

        peer_loads: AtomicUsize = .init(0),
        peer_errors: AtomicUsize = .init(0),

        loads: AtomicUsize = .init(0),
        loads_deduped: AtomicUsize = .init(0),
        local_loads: AtomicUsize = .init(0),
        local_load_errs: AtomicUsize = .init(0),

        server_requests: AtomicUsize = .init(0),
    };

    pub const Options = struct {
        getter: Getter,
        peers: PeerPicker,
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
            .name = name.clone(),
            .main_cache = try .init(allocator),
            .hot_cache = try .init(allocator),
            .load_group = .init(allocator),
            .options = options,
        };
    }

    pub fn get(self: *Self, key: Key) !Value {
        _ = self.stats.gets.fetchAdd(1, .monotonic);

        const cached = self.lookupCache(key);
        if (cached) |value| {
            _ = self.stats.cache_hits.fetchAdd(1, .monotonic);
            return value;
        }

        return self.load(key);
    }

    fn lookupCache(self: *Self, key: Key) ?Value {
        if (self.options.cached_bytes == 0) {
            return null;
        }
        return self.main_cache.get(key) orelse self.hot_cache.get(key);
    }

    fn load(self: *Self, key: Key) !Value {
        _ = self.stats.loads.fetchAdd(1, .monotonic);
        return try self.load_group.do(key, self, doLoad);
    }

    fn doLoad(self: *Self, key: Key) !Value {
        const cached = self.lookupCache(key);
        if (cached) |value| {
            _ = self.stats.cache_hits.fetchAdd(1, .monotonic);
            return value;
        }

        _ = self.stats.loads_deduped.fetchAdd(1, .monotonic);

        var peer = self.options.peers.pickPeer(key);
        if (peer) |*p| {
            if (self.getFromPeer(p, key)) |peer_value| {
                _ = self.stats.peer_loads.fetchAdd(1, .monotonic);
                return peer_value;
            } else |err| {
                const name = p.name();
                defer name.deinit();
                std.log.err("get value from peer({s}) by key({s}): {}", .{
                    name.val(),
                    key.val(),
                    err,
                });
                _ = self.stats.peer_errors.fetchAdd(1, .monotonic);
            }
        }

        const local_value = self.getFromLocal(key) catch |err| {
            _ = self.stats.local_load_errs.fetchAdd(1, .monotonic);
            return err;
        };
        _ = self.stats.local_loads.fetchAdd(1, .monotonic);
        return local_value;
    }

    fn getFromPeer(self: *Self, peer: *ProtoGetter, key: Key) !Value {
        const req: GetRequest = .{
            .group = self.name.val(),
            .key = key.val(),
        };
        const res = try peer.get(self.allocator, req);
        const value = res.value orelse return error.MissingPeerResponseValue;
        const val_slice = try Bytes.move(
            @constCast(value),
            self.allocator,
        );

        const pop = self.options.rand.intRangeAtMost(u8, 0, 9) == 0;
        if (pop) {
            self.populateCache(key, val_slice, self.hot_cache) catch |err| {
                val_slice.deinit();
                return err;
            };
        }
        return val_slice;
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

        self.nbytes += key.len();
        self.nbytes += value.len();

        const old_value = try self.lru.add(key, value);
        if (old_value) |old| {
            defer old.deinit();
            self.nbytes -= old.len();
            self.nbytes -= key.len();
        }
    }

    fn get(self: *Self, key: Key) ?Value {
        self.mu.lock();
        defer self.mu.unlock();

        self.nget += 1;

        const value = self.lru.get(key);
        if (value) |v| {
            self.nhit += 1;
            return v;
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
            .items = self.lru.len(),
            .gets = self.nget,
            .hits = self.nhit,
            .evictions = self.nevict,
        };
    }

    fn items(self: *Self) usize {
        self.mu.lock();
        defer self.mu.unlock();
        return self.lru.len();
    }

    fn bytes(self: *Self) usize {
        self.mu.lock();
        defer self.mu.unlock();
        return self.nbytes;
    }

    fn evict(ptr: *anyopaque, key: Key, value: Value) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.mu.lock();
        defer self.mu.unlock();
        self.nbytes -= key.len();
        self.nbytes -= value.len();
        self.nevict += 1;
    }

    fn deinit(self: *Self) void {
        self.lru.deinit();
        self.allocator.destroy(self);
    }
};
