const std = @import("std");
const protobuf = @import("protobuf");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const protobuf_dep = b.dependency("protobuf", .{
        .target = target,
        .optimize = optimize,
    });

    const protobuf_module = protobuf_dep.module("protobuf");

    const groupcache_mod = b.addModule("groupcache", .{
        .root_source_file = b.path("src/groupcache.zig"),
        .imports = &.{
            .{ .name = "protobuf", .module = protobuf_module },
        },
        .target = target,
        .optimize = optimize,
    });

    // gen-proto step
    {
        const protoc_step = protobuf.RunProtocStep.create(
            b,
            protobuf_dep.builder,
            target,
            .{
                .destination_directory = b.path("src/protocol"),
                .source_files = &.{
                    "src/protocol/groupcache.proto",
                },
                .include_directories = &.{},
            },
        );
        const gen_proto = b.step(
            "gen-proto",
            "Generate zig files from protocol buffer definitions",
        );
        gen_proto.dependOn(&protoc_step.step);
    }

    // bin
    {
        var httpz_dep = b.dependency("httpz", .{
            .target = target,
            .optimize = optimize,
        });

        const exe = b.addExecutable(.{
            .name = "static-file-cache",
            .root_module = b.createModule(.{
                .root_source_file = b.path("bin/static_file_cache.zig"),
                .target = target,
                .optimize = optimize,
                .imports = &.{
                    .{ .name = "groupcache", .module = groupcache_mod },
                    .{ .name = "httpz", .module = httpz_dep.module("httpz") },
                },
            }),
        });

        b.installArtifact(exe);

        const run_cmd = b.addRunArtifact(exe);
        run_cmd.step.dependOn(b.getInstallStep());
        if (b.args) |args| {
            run_cmd.addArgs(args);
        }

        const run_step = b.step(
            "static-file-cache",
            "Run static file cache server",
        );
        run_step.dependOn(&run_cmd.step);
    }
}
