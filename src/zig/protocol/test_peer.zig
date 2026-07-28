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
