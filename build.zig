const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Main library module, exported so a dependent package can import it as
    // `uamqp`. `createModule` kept it private, so nothing downstream could.
    const lib_mod = b.addModule("uamqp", .{
        .root_source_file = b.path("src/zig/uamqp.zig"),
        .target = target,
        .optimize = optimize,
    });
    // So `uamqp.version` can be the version, rather than a second copy of it
    // that has to be remembered at release time.
    lib_mod.addAnonymousImport("build.zig.zon", .{
        .root_source_file = b.path("build.zig.zon"),
    });

    // Static library artifact
    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "uamqp",
        .root_module = lib_mod,
    });
    b.installArtifact(lib);

    // Unit tests
    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/zig/uamqp.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_mod.addAnonymousImport("build.zig.zon", .{
        .root_source_file = b.path("build.zig.zon"),
    });
    const lib_unit_tests = b.addTest(.{
        .root_module = test_mod,
    });
    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);

    // Examples
    inline for (.{
        .{ "sender", "examples/sender.zig" },
        .{ "receiver", "examples/receiver.zig" },
    }) |example| {
        const exe_mod = b.createModule(.{
            .root_source_file = b.path(example[1]),
            .target = target,
            .optimize = optimize,
        });
        exe_mod.addImport("uamqp", lib_mod);
        const exe = b.addExecutable(.{
            .name = example[0],
            .root_module = exe_mod,
        });
        b.installArtifact(exe);
    }

    // Interop check against a real broker. Not part of `test`: it needs a
    // broker to talk to, which is configured through the environment.
    const interop_mod = b.createModule(.{
        .root_source_file = b.path("interop/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    interop_mod.addImport("uamqp", lib_mod);
    const interop_exe = b.addExecutable(.{
        .name = "interop",
        .root_module = interop_mod,
    });
    b.installArtifact(interop_exe);
    const run_interop = b.addRunArtifact(interop_exe);
    run_interop.step.dependOn(&interop_exe.step);
    if (b.args) |args| run_interop.addArgs(args);
    const interop_step = b.step("interop", "Run the interop check against a broker");
    interop_step.dependOn(&run_interop.step);

    // Benchmarks. Always ReleaseFast: a debug build measures the allocator and
    // the safety checks rather than the codec, which is worse than no number
    // at all because it looks like one.
    const bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/main.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    bench_mod.addImport("uamqp", lib_mod);
    const bench_exe = b.addExecutable(.{
        .name = "bench",
        .root_module = bench_mod,
    });
    const run_bench = b.addRunArtifact(bench_exe);
    run_bench.step.dependOn(&bench_exe.step);
    const bench_step = b.step("bench", "Measure the codec and the send path");
    bench_step.dependOn(&run_bench.step);

    // API documentation, generated from the doc comments. Written to
    // zig-out/docs, which needs serving rather than opening: the viewer
    // fetches the sources over HTTP.
    const docs = b.addInstallDirectory(.{
        .source_dir = lib.getEmittedDocs(),
        .install_dir = .prefix,
        .install_subdir = "docs",
    });
    const docs_step = b.step("docs", "Generate API documentation");
    docs_step.dependOn(&docs.step);
}
