const std = @import("std");
const Allocator = std.mem.Allocator;
const groupcache = @import("groupcache");
const Peer = groupcache.http.Peer;
const HTTPPool = groupcache.http.HTTPPool;
const GroupCache = groupcache.GroupCache;
const Getter = groupcache.Getter;
const Key = groupcache.Key;
const Value = groupcache.Value;
const Bytes = groupcache.Bytes;
const GetResponse = groupcache.GetResponse;
const httpz = @import("httpz");
const http = std.http;
const fmt = std.fmt;

pub fn main() !void {
    var debug = std.heap.DebugAllocator(.{}).init;
    defer _ = debug.deinit();
    const allocator = debug.allocator();

    var args = std.process.args();
    _ = args.skip();

    const port_arg = args.next() orelse "8080";
    const port = try fmt.parseInt(u16, port_arg, 10);
    const self = try Peer.move(
        try fmt.allocPrint(allocator, "http://127.0.0.1:{}", .{port}),
        allocator,
    );
    defer self.deinit();

    var peers = std.ArrayList(Peer).empty;
    defer peers.deinit(allocator);
    while (args.next()) |arg| {
        const peer = try Peer.copy(arg, allocator);
        errdefer peer.deinit();
        try peers.append(allocator, peer);
    }
    var handler = try RouterHandler.init(
        allocator,
        self,
        peers.items,
    );
    for (peers.items) |peer| {
        peer.deinit();
    }
    defer handler.deinit();

    var server = try httpz.Server(*RouterHandler).init(
        allocator,
        .{ .port = port },
        &handler,
    );
    defer server.deinit();
    defer server.stop();

    var router = try server.router(.{});

    router.get("/_groupcache/:group/:key", RouterHandler.servePeer, .{});
    router.get("/:group/:key", RouterHandler.servePublic, .{});

    std.debug.print("listening {s}/\n", .{self.val()});

    try server.listen();
}

const RouterHandler = struct {
    allocator: Allocator,
    client: *std.http.Client,
    pool: *HTTPPool,
    getter: *FakeStaticFileGetter,
    gcache: *GroupCache,

    const Self = @This();

    fn init(allocator: Allocator, self: Peer, peers: []Peer) !Self {
        const client = try allocator.create(http.Client);
        errdefer allocator.destroy(client);
        client.* = .{ .allocator = allocator };
        errdefer client.deinit();

        const pool = try allocator.create(HTTPPool);
        errdefer allocator.destroy(pool);
        pool.* = try .init(allocator, client, .{
            .self = self,
        });
        errdefer pool.deinit();
        try pool.setPeers(peers);

        const getter = try allocator.create(FakeStaticFileGetter);
        errdefer allocator.destroy(getter);
        getter.* = .init(allocator);

        const gcache = try allocator.create(GroupCache);
        errdefer allocator.destroy(gcache);
        gcache.* = try .init(
            allocator,
            Bytes.static("files"),
            .{
                .cached_bytes = 512 * 1024 * 1024,
                .peers = pool.peerPicker(),
                .getter = getter.getter(),
            },
        );
        errdefer gcache.deinit();

        try pool.addGroup(gcache);

        return .{
            .allocator = allocator,
            .client = client,
            .pool = pool,
            .getter = getter,
            .gcache = gcache,
        };
    }

    fn servePeer(self: *Self, req: *httpz.Request, res: *httpz.Response) !void {
        const group = req.param("group").?;
        const key = try Key.copy(req.param("key").?, self.allocator);
        defer key.deinit();

        var value = try self.pool.serve(group, key);
        defer value.deinit();

        const peer_value = try std.mem.concat(
            res.arena,
            u8,
            &.{ "from peer: ", self.pool.self.val(), "\n", value.val() },
        );
        var protobuf_res = GetResponse{ .value = peer_value };
        res.header("Content-Type", "application/x-protobuf");
        var writer = res.writer();
        return protobuf_res.encode(&writer.interface, res.arena);
    }

    fn servePublic(self: *Self, req: *httpz.Request, res: *httpz.Response) !void {
        const group = req.param("group").?;
        const key = try Key.copy(req.param("key").?, self.allocator);
        defer key.deinit();

        var value = try self.pool.serve(group, key);
        defer value.deinit();

        const body = try res.arena.dupeZ(u8, value.val());
        res.body = body;
    }

    fn deinit(self: *Self) void {
        self.pool.deinit();
        self.allocator.destroy(self.pool);

        self.gcache.deinit();
        self.allocator.destroy(self.gcache);

        self.allocator.destroy(self.getter);

        self.client.deinit();
        self.allocator.destroy(self.client);

        self.* = undefined;
    }
};

const FakeStaticFileGetter = struct {
    const Self = @This();

    allocator: Allocator,

    fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
        };
    }

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
        return Value.move(
            try fmt.allocPrint(
                self.allocator,
                "file: {s}\ncontent: \n{s}",
                .{ key.val(), why_zig },
            ),
            self.allocator,
        );
    }
};

const why_zig =
    \\ Why Zig?
    \\
    \\ A Simple Language
    \\ Focus on debugging your application rather than debugging your programming language knowledge.
    \\
    \\ No hidden control flow.
    \\ No hidden memory allocations.
    \\ No preprocessor, no macros.
    \\
    \\ Comptime
    \\ A fresh approach to metaprogramming based on compile-time code execution and lazy evaluation.
    \\
    \\ Call any function at compile-time.
    \\ Manipulate types as values without runtime overhead.
    \\ Comptime emulates the target architecture.
    \\
    \\ Maintain it with Zig
    \\ Incrementally improve your C/C++/Zig codebase.
    \\
    \\ Use Zig as a zero-dependency, drop-in C/C++ compiler that supports cross-compilation out-of-the-box.
    \\ Leverage zig build to create a consistent development environment across all platforms.
    \\ Add a Zig compilation unit to C/C++ projects, exposing the rich standard library to your C/C++ code.
;
