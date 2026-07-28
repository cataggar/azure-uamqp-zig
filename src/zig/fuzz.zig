//! Hostile input against everything reachable from bytes on the wire.
//!
//! The rest of the suite checks that a well-formed encoding decodes to what it
//! should. This checks the other half: that a malformed one is refused rather
//! than crashing, hanging or leaking. Those are the bugs a remote peer gets to
//! choose, and the three fixed in 0.2.0 -- the `doff` underflow, the nesting
//! recursion and the 255-byte `@intCast` -- were all of that shape.
//!
//! The generators are seeded, so a failure here reproduces exactly. Every test
//! uses `testing.allocator`, so a leak on an error path fails too. Both matter
//! more than the iteration counts, which are sized to keep the run under a few
//! seconds.

const std = @import("std");
const testing = std.testing;

const decoder = @import("types/decoder.zig");
const encoder = @import("types/encoder.zig");
const frame = @import("protocol/frame.zig");
const frame_codec = @import("protocol/frame_codec.zig");
const described = @import("protocol/described.zig");
const defs = @import("protocol/definitions.zig");
const message = @import("message.zig");

/// The one-byte constructors, so a random buffer spends its time inside the
/// compound and variable-width paths instead of bouncing off `InvalidFormatCode`.
const constructors = [_]u8{
    0x00, 0x40, 0x41, 0x42, 0x43, 0x44, 0x50, 0x53, 0x54, 0x55, 0x56,
    0x60, 0x61, 0x70, 0x71, 0x72, 0x73, 0x80, 0x81, 0x82, 0x83, 0x84,
    0x94, 0x98, 0xa0, 0xa1, 0xa3, 0xb0, 0xb1, 0xb3, 0xc0, 0xc1, 0xd0,
    0xd1, 0xe0, 0xf0,
};

/// Random bytes, but weighted toward things that look like AMQP.
fn fill(rand: std.Random, buf: []u8) void {
    for (buf) |*c| {
        c.* = switch (rand.uintLessThan(u8, 4)) {
            0, 1 => constructors[rand.uintLessThan(usize, constructors.len)],
            2 => rand.uintLessThan(u8, 8), // small sizes and counts
            else => rand.int(u8),
        };
    }
}

test "decode refuses every single byte it does not know" {
    for (0..256) |b| {
        const buf = [_]u8{@intCast(b)};
        var r = decoder.decode(testing.allocator, &buf) catch continue;
        r.value.deinit(testing.allocator);
    }
}

test "decode survives uniformly random input" {
    var prng = std.Random.DefaultPrng.init(0xA57);
    var buf: [512]u8 = undefined;
    for (0..100_000) |_| {
        const n = prng.random().uintLessThan(usize, buf.len) + 1;
        prng.random().bytes(buf[0..n]);
        var r = decoder.decode(testing.allocator, buf[0..n]) catch continue;
        r.value.deinit(testing.allocator);
    }
}

test "decode survives input shaped like AMQP" {
    var prng = std.Random.DefaultPrng.init(0xBEEF);
    var buf: [256]u8 = undefined;
    for (0..100_000) |_| {
        const n = prng.random().uintLessThan(usize, buf.len) + 1;
        fill(prng.random(), buf[0..n]);
        var r = decoder.decode(testing.allocator, buf[0..n]) catch continue;
        r.value.deinit(testing.allocator);
    }
}

test "decode does not run away on a deeply nested constructor" {
    // The 0.2.0 stack overflow: one frame of recursion per descriptor byte.
    const nested = try testing.allocator.alloc(u8, 64 * 1024);
    defer testing.allocator.free(nested);
    @memset(nested, 0x00);
    try testing.expectError(error.NestingTooDeep, decoder.decode(testing.allocator, nested));
}

test "a performative decodes or is refused, never anything else" {
    var prng = std.Random.DefaultPrng.init(0xC0FFEE);
    var buf: [256]u8 = undefined;
    for (0..100_000) |_| {
        const n = prng.random().uintLessThan(usize, buf.len) + 1;
        fill(prng.random(), buf[0..n]);
        var r = described.decodePerformative(testing.allocator, buf[0..n]) catch continue;
        r.deinit();
    }
}

test "a message decodes or is refused, never anything else" {
    var prng = std.Random.DefaultPrng.init(0x77);
    var buf: [512]u8 = undefined;
    for (0..50_000) |_| {
        const n = prng.random().uintLessThan(usize, buf.len) + 1;
        fill(prng.random(), buf[0..n]);
        var m = message.Message.decode(testing.allocator, buf[0..n]) catch continue;
        m.deinit();
    }
}

test "a frame header is parsed or refused" {
    var prng = std.Random.DefaultPrng.init(0x5150);
    var buf: [8]u8 = undefined;
    for (0..100_000) |_| {
        prng.random().bytes(&buf);
        const h = frame.FrameHeader.parse(&buf) catch continue;
        std.mem.doNotOptimizeAway(&h);
    }
}

fn ignoreFrame(_: ?*anyopaque, _: frame.FrameHeader, _: []const u8) void {}

test "the frame codec survives a hostile stream split at hostile boundaries" {
    // The outermost surface: whatever the socket hands us, in whatever
    // chunks it happens to arrive in. Feeding it in random slices is the
    // point -- a codec that reassembles correctly only when a frame arrives
    // whole is a codec that fails against a real network.
    var prng = std.Random.DefaultPrng.init(0xDEAD);
    const rand = prng.random();
    var buf: [2048]u8 = undefined;

    for (0..2_000) |_| {
        var codec = frame_codec.FrameCodec.init(testing.allocator, 4096);
        defer codec.deinit();
        try codec.subscribe(.amqp, ignoreFrame, null);
        try codec.subscribe(.sasl, ignoreFrame, null);

        const n = rand.uintLessThan(usize, buf.len) + 1;
        fill(rand, buf[0..n]);
        // Make the size field plausible often enough to reach the body path.
        if (n >= 8 and rand.boolean()) {
            std.mem.writeInt(u32, buf[0..4], @intCast(rand.uintLessThan(usize, n) + 8), .big);
            buf[4] = 2;
            buf[5] = if (rand.boolean()) 0x00 else 0x01;
        }

        var off: usize = 0;
        while (off < n) {
            const chunk = @min(rand.uintLessThan(usize, 32) + 1, n - off);
            codec.receiveBytes(buf[off .. off + chunk]) catch break;
            off += chunk;
        }
    }
}

test "anything that decodes re-encodes without crashing" {
    // A value that survived decoding is one an attacker can hand to whatever
    // the application does next, and re-encoding is the most likely thing.
    var prng = std.Random.DefaultPrng.init(0x1234);
    var buf: [256]u8 = undefined;
    for (0..50_000) |_| {
        const n = prng.random().uintLessThan(usize, buf.len) + 1;
        fill(prng.random(), buf[0..n]);
        var r = decoder.decode(testing.allocator, buf[0..n]) catch continue;
        defer r.value.deinit(testing.allocator);

        var out = encoder.Buffer.initDynamic(testing.allocator);
        defer out.deinit();
        encoder.encode(r.value, &out) catch continue;

        // What it wrote must be readable again, and must say the same thing.
        // Encoding is not allowed to quietly lose or change a value that the
        // decoder was willing to accept.
        var back = decoder.decode(testing.allocator, out.written()) catch |e| {
            std.debug.print("re-encoded {any} bytes decode as {s}\n", .{ out.written().len, @errorName(e) });
            return error.ReEncodedValueDoesNotDecode;
        };
        defer back.value.deinit(testing.allocator);
        try testing.expect(r.value.eql(back.value));
    }
}
