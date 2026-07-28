///! A scripted AMQP peer for tests.
///!
///! It stands in for the transport: everything the code under test writes is
///! kept for inspection, and `frame` builds the bytes a real peer would write
///! back. Mirrors the `test_peer` helpers in azure-sdk-for-zig.
const std = @import("std");
const Allocator = std.mem.Allocator;
const frame_mod = @import("frame.zig");
const FrameHeader = frame_mod.FrameHeader;
const defs = @import("definitions.zig");
const described = @import("described.zig");
const encoder = @import("../types/encoder.zig");
const Connection = @import("connection.zig").Connection;
const Session = @import("session.zig").Session;

/// A transport that keeps everything written to it, and can build the frames
/// a real peer would write back.
/// One decoded frame the code under test wrote.
pub const Frame = struct {
    channel: u16,
    performative: defs.Performative,
    payload: []const u8,
};

pub const TestPeer = struct {
    sent: std.ArrayList(u8) = .empty,
    scratch: std.ArrayList([]u8) = .empty,
    decoded: std.ArrayList(described.Decoded(defs.Performative)) = .empty,
    frames: std.ArrayList([]Frame) = .empty,
    allocator: Allocator,

    pub fn init(allocator: Allocator) TestPeer {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *TestPeer) void {
        for (self.decoded.items) |*d| d.deinit();
        self.decoded.deinit(self.allocator);
        for (self.frames.items) |f| self.allocator.free(f);
        self.frames.deinit(self.allocator);
        for (self.scratch.items) |buf| self.allocator.free(buf);
        self.scratch.deinit(self.allocator);
        self.sent.deinit(self.allocator);
    }

    pub fn send(context: ?*anyopaque, data: []const u8) anyerror!void {
        const self: *TestPeer = @ptrCast(@alignCast(context.?));
        try self.sent.appendSlice(self.allocator, data);
    }

    pub fn attach(self: *TestPeer, conn: *Connection) void {
        conn.setIo(send, self);
    }

    pub fn written(self: *TestPeer) []const u8 {
        return self.sent.items;
    }

    pub fn clear(self: *TestPeer) void {
        self.sent.clearRetainingCapacity();
    }

    /// The bytes a peer would send for one performative on a channel.
    pub fn frame(self: *TestPeer, channel: u16, performative: defs.Performative) ![]const u8 {
        return self.framePayload(channel, performative, &.{});
    }

    /// The bytes a peer would send for one performative and the payload that
    /// follows it in the same frame.
    pub fn framePayload(
        self: *TestPeer,
        channel: u16,
        performative: defs.Performative,
        payload: []const u8,
    ) ![]const u8 {
        var body = encoder.Buffer.initDynamic(self.allocator);
        defer body.deinit();
        try described.encodePerformative(self.allocator, performative, &body);
        try body.writeAll(payload);

        const total = frame_mod.frame_header_size + body.written().len;
        const buf = try self.allocator.alloc(u8, total);
        errdefer self.allocator.free(buf);
        try self.scratch.append(self.allocator, buf);

        const header = FrameHeader{
            .size = @intCast(total),
            .doff = 2,
            .frame_type = .amqp,
            .channel = channel,
        };
        @memcpy(buf[0..frame_mod.frame_header_size], &header.serialize());
        @memcpy(buf[frame_mod.frame_header_size..], body.written());
        return buf;
    }

    /// Drive a connection through the header and Open exchange and leave it
    /// opened, with nothing of the handshake left in `written`.
    pub fn openConnection(self: *TestPeer, conn: *Connection) !void {
        return self.openConnectionAdvertising(conn, null);
    }

    /// `openConnection`, with the peer advertising an idle timeout of its own.
    pub fn openConnectionAdvertising(self: *TestPeer, conn: *Connection, idle_time_out: ?u32) !void {
        self.attach(conn);
        try conn.open();
        try conn.onBytesReceived(&frame_mod.amqp_header);
        const open_bytes = try self.frame(0, .{ .open = .{
            .container_id = "peer",
            .max_frame_size = 65536,
            .channel_max = 16,
            .idle_time_out = idle_time_out,
        } });
        try conn.onBytesReceived(open_bytes);
        self.clear();
    }

    /// Every performative the code under test has written since the last
    /// `clear`, in order. Freed with the peer.
    pub fn performatives(self: *TestPeer) ![]const Frame {
        var out: std.ArrayList(Frame) = .empty;
        errdefer out.deinit(self.allocator);
        var rest = self.written();
        while (rest.len >= frame_mod.frame_header_size) {
            const header = try FrameHeader.parse(rest[0..frame_mod.frame_header_size]);
            const body = rest[header.doff * 4 .. header.size];
            if (body.len > 0) {
                const decoded = try described.decodePerformative(self.allocator, body);
                try self.decoded.append(self.allocator, decoded);
                try out.append(self.allocator, .{
                    .channel = header.channel,
                    .performative = decoded.value,
                    .payload = body[decoded.bytes_consumed..],
                });
            }
            rest = rest[header.size..];
        }
        const slice = try out.toOwnedSlice(self.allocator);
        try self.frames.append(self.allocator, slice);
        return slice;
    }

    /// The single performative written since the last `clear`.
    pub fn onlyPerformative(self: *TestPeer) !Frame {
        const all = try self.performatives();
        if (all.len != 1) return error.ExpectedExactlyOneFrame;
        return all[0];
    }

    /// Decode the one performative the connection sent, skipping its header.
    pub fn lastPerformative(self: *TestPeer) !described.Decoded(defs.Performative) {
        const bytes = self.written();
        const header = try FrameHeader.parse(bytes[0..8]);
        return described.decodePerformative(self.allocator, bytes[8..header.size]);
    }
};

/// A clock the test winds by hand, so timeouts are exercised in microseconds
/// rather than minutes.
pub const ManualClock = struct {
    ms: i64 = 0,

    fn read(context: ?*anyopaque) i64 {
        const self: *ManualClock = @ptrCast(@alignCast(context.?));
        return self.ms;
    }

    pub fn clock(self: *ManualClock) @import("connection.zig").Clock {
        return .{ .context = self, .read_ms = read };
    }

    pub fn advance(self: *ManualClock, by: i64) void {
        self.ms += by;
    }
};

/// A connection and session already open, for tests of the layers above them.
///
/// Heap-allocated because a connection and a session both register the
/// address they live at: the fixture must not move once `init` returns.
pub const Fixture = struct {
    peer: TestPeer,
    conn: Connection,
    session: Session = undefined,
    allocator: Allocator,

    pub fn init(allocator: Allocator) !*Fixture {
        const self = try allocator.create(Fixture);
        errdefer allocator.destroy(self);
        self.* = .{
            .peer = TestPeer.init(allocator),
            .conn = Connection.init(allocator, "container", null, .{}),
            .allocator = allocator,
        };
        errdefer self.peer.deinit();
        errdefer self.conn.deinit();
        try self.peer.openConnection(&self.conn);
        self.session = Session.init(allocator, &self.conn, .{});
        errdefer self.session.deinit();
        try self.session.begin();
        try self.conn.onBytesReceived(try self.peer.frame(1, .{ .begin = .{
            .remote_channel = 0,
            .next_outgoing_id = 0,
            .incoming_window = 1024,
            .outgoing_window = 1024,
        } }));
        self.peer.clear();
        return self;
    }

    pub fn deinit(self: *Fixture) void {
        self.session.deinit();
        self.conn.deinit();
        self.peer.deinit();
        self.allocator.destroy(self);
    }

    /// Bring a link to `attached`, leaving `peer.written()` holding whatever
    /// the code under test wrote *in response* to the peer's Attach and
    /// nothing of the handshake itself.
    pub fn attach(self: *Fixture, link: anytype, peer_handle: u32) !void {
        try link.attach();
        try self.respondAttach(link, peer_handle);
    }

    /// Answer an Attach the link has already sent, for a link something else
    /// attached.
    pub fn respondAttach(self: *Fixture, link: anytype, peer_handle: u32) !void {
        self.peer.clear();
        try self.conn.onBytesReceived(try self.peer.frame(1, .{ .attach = .{
            .name = link.name,
            .handle = peer_handle,
            .role = if (link.role == .sender) .receiver else .sender,
            .initial_delivery_count = if (link.role == .receiver) 0 else null,
        } }));
    }

    /// Feed the link a Flow granting it credit, as a receiver would.
    ///
    /// Clears first rather than last, so that anything sent because credit
    /// arrived is still there to inspect.
    pub fn grant(self: *Fixture, link: anytype, credit: u32) !void {
        self.peer.clear();
        try self.conn.onBytesReceived(try self.peer.frame(1, .{ .flow = .{
            .next_incoming_id = 0,
            .incoming_window = 1024,
            .next_outgoing_id = 0,
            .outgoing_window = 1024,
            .handle = link.endpoint().?.input_handle,
            .delivery_count = link.delivery_count,
            .link_credit = credit,
        } }));
    }
};
