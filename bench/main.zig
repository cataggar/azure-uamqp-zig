const std = @import("std");
const uamqp = @import("uamqp");

var io_undefined: std.Io = undefined;

fn nowNs(io: std.Io) i96 {
    return std.Io.Timestamp.now(io, .awake).nanoseconds;
}

const Counting = struct {
    parent: std.mem.Allocator,
    count: usize = 0,
    bytes: usize = 0,

    fn allocator(self: *Counting) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = alloc,
            .resize = resize,
            .remap = remap,
            .free = free,
        } };
    }
    fn alloc(ctx: *anyopaque, len: usize, a: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Counting = @ptrCast(@alignCast(ctx));
        self.count += 1;
        self.bytes += len;
        return self.parent.rawAlloc(len, a, ra);
    }
    fn resize(ctx: *anyopaque, buf: []u8, a: std.mem.Alignment, new_len: usize, ra: usize) bool {
        const self: *Counting = @ptrCast(@alignCast(ctx));
        return self.parent.rawResize(buf, a, new_len, ra);
    }
    fn remap(ctx: *anyopaque, buf: []u8, a: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
        const self: *Counting = @ptrCast(@alignCast(ctx));
        self.count += 1;
        self.bytes += new_len;
        return self.parent.rawRemap(buf, a, new_len, ra);
    }
    fn free(ctx: *anyopaque, buf: []u8, a: std.mem.Alignment, ra: usize) void {
        const self: *Counting = @ptrCast(@alignCast(ctx));
        self.parent.rawFree(buf, a, ra);
    }
};

fn sink(_: ?*anyopaque, _: []const u8) anyerror!void {}

/// The bytes a peer would send for one performative.
fn peerFrame(
    allocator: std.mem.Allocator,
    channel: u16,
    perf: uamqp.definitions.Performative,
    scratch: []u8,
) ![]const u8 {
    var body = uamqp.encoder.Buffer.initDynamic(allocator);
    defer body.deinit();
    try uamqp.described.encodePerformative(allocator, perf, &body);
    const total = uamqp.frame.frame_header_size + body.written().len;
    const hdr = uamqp.frame.FrameHeader{
        .size = @intCast(total),
        .doff = 2,
        .frame_type = .amqp,
        .channel = channel,
    };
    @memcpy(scratch[0..uamqp.frame.frame_header_size], &hdr.serialize());
    @memcpy(scratch[uamqp.frame.frame_header_size..total], body.written());
    return scratch[0..total];
}

pub fn main() !void {
    var threaded: std.Io.Threaded = .init(std.heap.page_allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const gpa = std.heap.smp_allocator;

    // ── 1. decoding an array of N uints ───────────────────────────────
    for ([_]usize{ 64, 256, 1024, 4096 }) |n| {
        var buf = uamqp.encoder.Buffer.initDynamic(gpa);
        defer buf.deinit();
        const items = try gpa.alloc(uamqp.AmqpValue, n);
        defer gpa.free(items);
        for (items, 0..) |*it, i| it.* = .{ .uint = @intCast(i) };
        try uamqp.encoder.encode(.{ .array = items }, &buf);
        const bytes = buf.written();

        const iters: usize = 200;
        var counting = Counting{ .parent = gpa };
        const ca = counting.allocator();
        const t0 = nowNs(io);
        for (0..iters) |_| {
            var r = try uamqp.decoder.decode(ca, bytes);
            r.value.deinit(ca);
        }
        const t1 = nowNs(io);
        std.debug.print("array decode n={d:<5} {d:>9} ns/op  {d:>6} allocs/op  wire={d}B\n", .{
            n, @divTrunc(t1 - t0, iters), counting.count / iters, bytes.len,
        });
    }

    // ── 2. one message round trip ─────────────────────────────────────
    {
        var message = uamqp.message.Message.init(gpa);
        defer message.deinit();
        try message.addBodyData("x" ** 512);
        try message.putApplicationProperty("operation", .{ .string = "READ" });
        var mbuf = uamqp.encoder.Buffer.initDynamic(gpa);
        defer mbuf.deinit();
        try message.encode(&mbuf);
        const bytes = mbuf.written();

        const iters: usize = 2000;
        var counting = Counting{ .parent = gpa };
        const ca = counting.allocator();
        const t0 = nowNs(io);
        for (0..iters) |_| {
            var m = try uamqp.message.Message.decode(ca, bytes);
            m.deinit();
        }
        const t1 = nowNs(io);
        std.debug.print("message decode        {d:>9} ns/op  {d:>6} allocs/op  wire={d}B\n", .{
            @divTrunc(t1 - t0, iters), counting.count / iters, bytes.len,
        });
    }

    // ── 3. sending a transfer through the whole stack ─────────────────
    {
        var counting = Counting{ .parent = gpa };
        const ca = counting.allocator();

        var conn = uamqp.connection.Connection.init(ca, "bench", null, .{});
        defer conn.deinit();
        conn.setIo(sink, null);
        try conn.open();
        try conn.onBytesReceived(&uamqp.frame.amqp_header);
        // Hand-feed the peer's Open/Begin/Attach using the library's own encoder.
        var scratch: [4096]u8 = undefined;
        try conn.onBytesReceived(try peerFrame(gpa, 0, .{ .open = .{ .container_id = "peer", .max_frame_size = 65536, .channel_max = 16 } }, &scratch));

        var session = uamqp.session.Session.init(ca, &conn, .{});
        defer session.deinit();
        try session.begin();
        try conn.onBytesReceived(try peerFrame(gpa, 1, .{ .begin = .{ .remote_channel = 0, .next_outgoing_id = 0, .incoming_window = 65535, .outgoing_window = 65535 } }, &scratch));

        var link = try uamqp.link.Link.init(ca, &session, "s", .sender, .{ .address = "q" }, .{ .address = "q" });
        defer link.deinit();
        try link.attach();
        try conn.onBytesReceived(try peerFrame(gpa, 1, .{ .attach = .{ .name = "s", .handle = 7, .role = .receiver } }, &scratch));

        const payload = "y" ** 1024;
        const iters: usize = 2000;
        var before = counting;
        _ = &before;
        const c0 = counting.count;
        const t0 = nowNs(io);
        for (0..iters) |_| {
            link.setLinkCredit(1000);
            session.remote_incoming_window = 1000;
            _ = link.send(payload, .{ .settled = true }) catch |e| {
                std.debug.print("send failed: {s}\n", .{@errorName(e)});
                break;
            };
        }
        const t1 = nowNs(io);
        std.debug.print("transfer send 1KiB    {d:>9} ns/op  {d:>6} allocs/op\n", .{
            @divTrunc(t1 - t0, iters), (counting.count - c0) / iters,
        });

        // ── 4. receiving a transfer through the whole stack ───────────
        link.on_transfer_received = struct {
            fn cb(_: ?*anyopaque, _: uamqp.definitions.Transfer, _: []const u8) ?uamqp.definitions.DeliveryState {
                return .accepted;
            }
        }.cb;

        var rlink = try uamqp.link.Link.init(ca, &session, "r", .receiver, .{ .address = "q" }, .{ .address = "q" });
        defer rlink.deinit();
        rlink.on_transfer_received = struct {
            fn cb(_: ?*anyopaque, _: uamqp.definitions.Transfer, _: []const u8) ?uamqp.definitions.DeliveryState {
                return .accepted;
            }
        }.cb;
        try rlink.attach();
        try conn.onBytesReceived(try peerFrame(gpa, 1, .{ .attach = .{ .name = "r", .handle = 8, .role = .sender, .initial_delivery_count = 0 } }, &scratch));

        var frame_buf: [2048]u8 = undefined;
        var body = uamqp.encoder.Buffer.initDynamic(gpa);
        defer body.deinit();
        try uamqp.described.encodePerformative(gpa, .{ .transfer = .{
            .handle = rlink.endpoint().?.input_handle.?,
            .delivery_id = 0,
            .delivery_tag = "tag",
            .message_format = 0,
            .settled = true,
        } }, &body);
        try body.writeAll(payload);
        const total = uamqp.frame.frame_header_size + body.written().len;
        const hdr = uamqp.frame.FrameHeader{ .size = @intCast(total), .doff = 2, .frame_type = .amqp, .channel = 1 };
        @memcpy(frame_buf[0..uamqp.frame.frame_header_size], &hdr.serialize());
        @memcpy(frame_buf[uamqp.frame.frame_header_size..total], body.written());
        const wire = frame_buf[0..total];

        const c1 = counting.count;
        const t2 = nowNs(io);
        for (0..iters) |_| {
            rlink.setLinkCredit(1000);
            session.incoming_window = 1000;
            try conn.onBytesReceived(wire);
        }
        const t3 = nowNs(io);
        std.debug.print("transfer recv 1KiB    {d:>9} ns/op  {d:>6} allocs/op\n", .{
            @divTrunc(t3 - t2, iters), (counting.count - c1) / iters,
        });
    }
}
