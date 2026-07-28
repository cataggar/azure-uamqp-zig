const std = @import("std");
const Allocator = std.mem.Allocator;
const frame = @import("frame.zig");
const FrameHeader = frame.FrameHeader;

/// Callback invoked when a complete frame has been received.
pub const OnFrameReceived = *const fn (context: ?*anyopaque, header: FrameHeader, body: []const u8) void;

/// A streaming frame codec that accumulates bytes and emits complete frames.
///
/// Replaces frame_codec.c — handles the stateful parsing of AMQP frames
/// from a byte stream, managing partial reads and frame assembly.
pub const FrameCodec = struct {
    allocator: Allocator,
    max_frame_size: u32,
    state: State,
    header_buf: [frame.frame_header_size]u8,
    header_bytes_received: usize,
    frame_body: std.ArrayList(u8),
    body_bytes_needed: usize,
    current_header: ?FrameHeader,

    // Subscription list for frame type dispatch
    subscriptions: std.ArrayList(Subscription),

    const Subscription = struct {
        frame_type: frame.FrameType,
        callback: OnFrameReceived,
        context: ?*anyopaque,
    };

    const State = enum {
        reading_header,
        reading_body,
    };

    pub fn init(allocator: Allocator, max_frame_size: u32) FrameCodec {
        return .{
            .allocator = allocator,
            .max_frame_size = max_frame_size,
            .state = .reading_header,
            .header_buf = undefined,
            .header_bytes_received = 0,
            .frame_body = .empty,
            .body_bytes_needed = 0,
            .current_header = null,
            .subscriptions = .empty,
        };
    }

    pub fn deinit(self: *FrameCodec) void {
        self.frame_body.deinit(self.allocator);
        self.subscriptions.deinit(self.allocator);
    }

    /// Subscribe to receive frames of a given type.
    pub fn subscribe(self: *FrameCodec, frame_type: frame.FrameType, callback: OnFrameReceived, context: ?*anyopaque) Allocator.Error!void {
        try self.subscriptions.append(self.allocator, .{
            .frame_type = frame_type,
            .callback = callback,
            .context = context,
        });
    }

    /// Unsubscribe from a frame type.
    pub fn unsubscribe(self: *FrameCodec, frame_type: frame.FrameType) void {
        var i: usize = 0;
        while (i < self.subscriptions.items.len) {
            if (self.subscriptions.items[i].frame_type == frame_type) {
                _ = self.subscriptions.orderedRemove(i);
            } else {
                i += 1;
            }
        }
    }

    /// Feed bytes into the codec. Complete frames are dispatched to subscribers.
    pub fn receiveBytes(self: *FrameCodec, data: []const u8) !void {
        var offset: usize = 0;
        while (offset < data.len) {
            switch (self.state) {
                .reading_header => {
                    const needed = frame.frame_header_size - self.header_bytes_received;
                    const available = @min(needed, data.len - offset);
                    @memcpy(
                        self.header_buf[self.header_bytes_received .. self.header_bytes_received + available],
                        data[offset .. offset + available],
                    );
                    self.header_bytes_received += available;
                    offset += available;

                    if (self.header_bytes_received == frame.frame_header_size) {
                        const hdr = FrameHeader.parse(&self.header_buf) catch return error.InvalidFrame;

                        if (hdr.size > self.max_frame_size) return error.FrameTooLarge;

                        const body_size = hdr.bodySize();
                        self.current_header = hdr;
                        self.body_bytes_needed = body_size;
                        self.frame_body.clearRetainingCapacity();

                        if (body_size == 0) {
                            self.dispatchFrame(hdr, &.{});
                            self.resetForNextFrame();
                        } else {
                            try self.frame_body.ensureTotalCapacity(self.allocator, body_size);
                            self.state = .reading_body;
                        }
                    }
                },
                .reading_body => {
                    const needed = self.body_bytes_needed - self.frame_body.items.len;
                    const available = @min(needed, data.len - offset);
                    try self.frame_body.appendSlice(self.allocator, data[offset .. offset + available]);
                    offset += available;

                    if (self.frame_body.items.len == self.body_bytes_needed) {
                        self.dispatchFrame(self.current_header.?, self.frame_body.items);
                        self.resetForNextFrame();
                    }
                },
            }
        }
    }

    /// Encode and return a frame with the given body.
    pub fn encodeFrame(
        self: *FrameCodec,
        frame_type: frame.FrameType,
        channel: u16,
        body: []const u8,
    ) ![]u8 {
        const total_size: u32 = @intCast(frame.frame_header_size + body.len);
        if (total_size > self.max_frame_size) return error.FrameTooLarge;

        const hdr = FrameHeader{
            .size = total_size,
            .doff = 2,
            .frame_type = frame_type,
            .channel = channel,
        };

        const buf = try self.allocator.alloc(u8, total_size);
        const header_bytes = hdr.serialize();
        @memcpy(buf[0..frame.frame_header_size], &header_bytes);
        @memcpy(buf[frame.frame_header_size..], body);
        return buf;
    }

    fn dispatchFrame(self: *FrameCodec, header: FrameHeader, body: []const u8) void {
        for (self.subscriptions.items) |sub| {
            if (sub.frame_type == header.frame_type) {
                sub.callback(sub.context, header, body);
            }
        }
    }

    fn resetForNextFrame(self: *FrameCodec) void {
        self.state = .reading_header;
        self.header_bytes_received = 0;
        self.current_header = null;
        // Note: frame_body is cleared at the start of the next header-complete
        // transition (line above ensureTotalCapacity). We intentionally do NOT
        // clear here so dispatched body slices remain valid until the next frame.
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

var test_received_header: ?FrameHeader = null;
var test_received_body: ?[]const u8 = null;

fn testCallback(_: ?*anyopaque, header: FrameHeader, body: []const u8) void {
    test_received_header = header;
    test_received_body = body;
}

test "FrameCodec receives complete frame" {
    const allocator = std.testing.allocator;

    test_received_header = null;
    test_received_body = null;

    var codec = FrameCodec.init(allocator, 4096);
    defer codec.deinit();

    try codec.subscribe(.amqp, testCallback, null);

    // Build a frame: header (8 bytes) + body "hello"
    const body = "hello";
    const total_size: u32 = @intCast(frame.frame_header_size + body.len);
    const hdr = FrameHeader{
        .size = total_size,
        .doff = 2,
        .frame_type = .amqp,
        .channel = 0,
    };
    const header_bytes = hdr.serialize();

    var frame_bytes: [13]u8 = undefined;
    @memcpy(frame_bytes[0..8], &header_bytes);
    @memcpy(frame_bytes[8..13], body);

    try codec.receiveBytes(&frame_bytes);

    try std.testing.expect(test_received_header != null);
    try std.testing.expectEqual(@as(u32, 13), test_received_header.?.size);
    try std.testing.expectEqual(frame.FrameType.amqp, test_received_header.?.frame_type);
    try std.testing.expectEqualStrings("hello", test_received_body.?);
}

test "FrameCodec handles partial reads" {
    const allocator = std.testing.allocator;

    test_received_header = null;
    test_received_body = null;

    var codec = FrameCodec.init(allocator, 4096);
    defer codec.deinit();

    try codec.subscribe(.amqp, testCallback, null);

    const body = "hi";
    const total_size: u32 = @intCast(frame.frame_header_size + body.len);
    const hdr = FrameHeader{
        .size = total_size,
        .doff = 2,
        .frame_type = .amqp,
        .channel = 1,
    };
    const header_bytes = hdr.serialize();

    var frame_bytes: [10]u8 = undefined;
    @memcpy(frame_bytes[0..8], &header_bytes);
    @memcpy(frame_bytes[8..10], body);

    // Feed one byte at a time
    for (&frame_bytes) |*b| {
        try codec.receiveBytes(b[0..1]);
    }

    try std.testing.expect(test_received_header != null);
    try std.testing.expectEqual(@as(u16, 1), test_received_header.?.channel);
    try std.testing.expectEqualStrings("hi", test_received_body.?);
}

test "a hostile frame header is rejected by the codec" {
    const allocator = std.testing.allocator;

    var codec = FrameCodec.init(allocator, 4096);
    defer codec.deinit();

    try codec.subscribe(.amqp, testCallback, null);

    // Under max_frame_size, so that guard never sees it; the body size is what
    // is hostile. Left unchecked this reaches ensureTotalCapacity as ~4 GiB.
    const hostile = [_]u8{ 0, 0, 0, 8, 200, 0, 0, 0 };
    try std.testing.expectError(error.InvalidFrame, codec.receiveBytes(&hostile));
}

test "FrameCodec encodeFrame" {
    const allocator = std.testing.allocator;

    var codec = FrameCodec.init(allocator, 4096);
    defer codec.deinit();

    const body = "test payload";
    const encoded = try codec.encodeFrame(.amqp, 3, body);
    defer allocator.free(encoded);

    const hdr = try FrameHeader.parse(encoded[0..8]);
    try std.testing.expectEqual(@as(u32, @intCast(frame.frame_header_size + body.len)), hdr.size);
    try std.testing.expectEqual(frame.FrameType.amqp, hdr.frame_type);
    try std.testing.expectEqual(@as(u16, 3), hdr.channel);
    try std.testing.expectEqualStrings(body, encoded[8..]);
}

test "fuzz: receiving arbitrary bytes never crashes" {
    const Checker = struct {
        max_frame_size: u32,
        violation: bool = false,

        fn onFrame(context: ?*anyopaque, header: FrameHeader, body: []const u8) void {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            // A dispatched frame must be self-consistent: within the negotiated
            // maximum, and carrying exactly the bytes the header accounts for.
            if (header.size > self.max_frame_size) self.violation = true;
            if (header.size < frame.frame_header_size) self.violation = true;
            if (body.len != header.size - @as(u32, header.doff) * 4) self.violation = true;
        }
    };

    try std.testing.fuzz({}, struct {
        fn one(_: void, smith: *std.testing.Smith) anyerror!void {
            var buf: [4096]u8 = undefined;
            const len = smith.slice(&buf);
            const data = buf[0..len];

            // The same bytes must parse the same way however the transport
            // happens to split them across reads.
            for ([_]usize{ 1, 3, 8, 64, 4096 }) |chunk| {
                var checker = Checker{ .max_frame_size = 1024 };
                var codec = FrameCodec.init(std.testing.allocator, checker.max_frame_size);
                defer codec.deinit();
                try codec.subscribe(.amqp, Checker.onFrame, &checker);
                try codec.subscribe(.sasl, Checker.onFrame, &checker);

                var offset: usize = 0;
                while (offset < data.len) {
                    const end = @min(offset + chunk, data.len);
                    codec.receiveBytes(data[offset..end]) catch break;
                    offset = end;
                }
                try std.testing.expect(!checker.violation);
            }
        }
    }.one, .{});
}
