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
}
