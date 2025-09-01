const std = @import("std");
const http = std.http;
const ConsistentHash = @import("ConsistentHash.zig");
const mem = @import("mem.zig");
const Bytes = mem.Bytes;
const Allocator = std.mem.Allocator;
const groupcache = @import("groupcache.zig");
const ProtoGetter = groupcache.ProtoGetter;
const PeerPicker = groupcache.PeerPicker;
const GetRequest = groupcache.GetRequest;
const GetResponse = groupcache.GetResponse;
const Key = groupcache.Key;
const Value = groupcache.Value;
const GroupCache = groupcache.GroupCache;

pub const Peer = Bytes;

pub const HTTPCluster = struct {
    const Self = @This();

    const default_replicas = 50;
    const default_base_path = Bytes.static("/_groupcache/");

    pub const Options = struct {
        /// https://example.net:8000
        self: Peer,

        /// "/_groupcache/"
        base_path: ?Bytes = null,
        replicas: ?usize = null,
        hash: ?*const fn (data: []const u8) u32 = null,
    };

    self: Peer,
    allocator: Allocator,
    peers: ConsistentHash,
    client: *http.Client,
    base_path: Bytes,
    replicas: usize,
    hash: ?*const fn (data: []const u8) u32,
    http_getters: std.HashMap(
        Peer,
        HTTPGetter,
        mem.BytesHashContext,
        std.hash_map.default_max_load_percentage,
    ),
    groups: std.StringHashMap(*GroupCache),
    mu: std.Thread.Mutex = .{},

    pub fn init(
        allocator: Allocator,
        client: *http.Client,
        options: Options,
    ) Allocator.Error!Self {
        return .{
            .self = options.self.clone(),
            .allocator = allocator,
            .peers = ConsistentHash.init(
                allocator,
                options.replicas orelse default_replicas,
                options.hash,
            ),
            .client = client,
            .base_path = blk: {
                if (options.base_path) |url| {
                    break :blk url.clone();
                }
                break :blk default_base_path;
            },
            .replicas = options.replicas orelse default_replicas,
            .hash = options.hash,
            .http_getters = .init(allocator),
            .groups = .init(allocator),
        };
    }

    // Peer: http://127.0.0.1:8080
    pub fn setPeers(self: *Self, peers: []const Peer) !void {
        self.mu.lock();
        defer self.mu.unlock();

        self.resetPeersLocked();

        for (peers) |peer| {
            const existing = try self.peers.add(peer);
            if (existing) {
                continue;
            }
            const peer_base_url = try Bytes.move(
                try std.fmt.allocPrint(
                    self.allocator,
                    "{s}{s}",
                    .{ peer.val(), self.base_path.val() },
                ),
                self.allocator,
            );
            defer peer_base_url.deinit();

            const peer_key = peer.clone();
            errdefer peer_key.deinit();
            var prev = try self.http_getters.fetchPut(
                peer_key,
                HTTPGetter.init(self.client, peer_base_url),
            ) orelse continue;
            prev.value.deinit();
        }
    }

    fn resetPeersLocked(self: *Self) void {
        var it = self.http_getters.iterator();
        while (it.next()) |entry| {
            entry.key_ptr.deinit();
            entry.value_ptr.deinit();
        }
        self.http_getters.clearAndFree();
        self.peers.reset();
    }

    pub fn addGroup(self: *Self, group: *GroupCache) !void {
        return self.groups.put(group.name.val(), group);
    }

    pub fn peerPicker(self: *Self) PeerPicker {
        return .{
            .ptr = self,
            .vtable = &.{
                .pickPeer = pickPeer,
            },
        };
    }

    fn pickPeer(ptr: *anyopaque, key: Key) ?ProtoGetter {
        const self: *Self = @ptrCast(@alignCast(ptr));

        self.mu.lock();
        defer self.mu.unlock();

        const peer = self.peers.get(key) orelse return null;
        defer peer.deinit();
        if (peer.eql(&self.self)) {
            return null;
        }
        var getter = self.http_getters.getPtr(peer) orelse return null;
        return getter.protoGetter();
    }

    pub fn get(
        self: *Self,
        group: []const u8,
        key: Key,
    ) !Value {
        var gcache = self.groups.get(group) orelse {
            return error.GroupNotFound;
        };
        _ = gcache.stats.server_requests.fetchAdd(1, .monotonic);
        return try gcache.get(key);
    }

    pub fn deinit(self: *Self) void {
        self.self.deinit();
        self.peers.deinit();
        self.base_path.deinit();
        self.groups.deinit();
        self.* = undefined;
    }
};

const HTTPGetter = struct {
    client: *http.Client,
    base_url: Bytes, // http://127.0.0.1:8080/_groupcache/

    const Self = @This();

    fn init(client: *http.Client, base_url: Bytes) Self {
        return .{
            .client = client,
            .base_url = base_url.clone(),
        };
    }

    fn protoGetter(self: *Self) ProtoGetter {
        return .{
            .ptr = self,
            .vtable = &.{
                .get = get,
                .name = name,
            },
        };
    }

    fn get(
        ptr: *anyopaque,
        allocator: Allocator,
        req: GetRequest,
    ) !GetResponse {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const endpoint = try std.fmt.allocPrint(
            allocator,
            "{s}{s}/{s}",
            .{ self.base_url.val(), req.group, req.key },
        );
        defer allocator.free(endpoint);

        const uri = try std.Uri.parse(endpoint);
        var request = try self.client.request(.GET, uri, .{});
        defer request.deinit();

        try request.sendBodiless();

        var redirect_buffer: [1024]u8 = undefined;
        var response = try request.receiveHead(&redirect_buffer);

        var transfer_buffer: [4096]u8 = undefined;
        var decompress_buffer: [4096]u8 = undefined;
        var decompress: http.Decompress = undefined;
        const reader = response.readerDecompressing(
            &transfer_buffer,
            &decompress,
            &decompress_buffer,
        );
        return GetResponse.decode(reader, allocator);
    }

    fn name(ptr: *anyopaque) Bytes {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.base_url.clone();
    }

    fn deinit(self: *Self) void {
        self.base_url.deinit();
        self.* = undefined;
    }
};
