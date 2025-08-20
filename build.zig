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

    const lib_mod = b.addModule("groupcache", .{
        .root_source_file = b.path("src/groupcache.zig"),
        .imports = &.{
            .{ .name = "protobuf", .module = protobuf_module },
        },
        .target = target,
        .optimize = optimize,
    });

    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "groupcache",
        .root_module = lib_mod,
    });

    b.installArtifact(lib);

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

    // unit test step
    {
        const enable_tsan = b.option(bool, "tsan", "Enable ThreadSanitizer");
        const test_filter = b.option(
            []const []const u8,
            "test-filter",
            "Filters for test: specify multiple times for multiple filters",
        );
        const tests = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = b.path("src/groupcache.zig"),
                .target = target,
                .optimize = optimize,
                .sanitize_thread = enable_tsan,
            }),
            .filters = test_filter orelse &.{},
        });

        tests.root_module.addImport("protobuf", protobuf_module);
        const run_test = b.addRunArtifact(tests);

        const test_step = b.step("test", "Run tests");
        test_step.dependOn(&run_test.step);
    }
}
