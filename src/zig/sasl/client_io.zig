///! SASL client negotiation (OASIS spec §5.3)
///!
///! Runs the SASL half of a connection: the SASL protocol header, the
///! mechanisms the server offers, the chosen mechanism's init and challenge
///! exchange, and the outcome. Once the outcome is accepted, the bytes that
///! follow belong to the AMQP connection and are handed on untouched.
///!
///! This is the Zig equivalent of saslclientio.c, without the I/O: the caller
///! owns the transport, as it does for `Connection`.
const std = @import("std");
const Allocator = std.mem.Allocator;
const Mechanism = @import("mechanism.zig").Mechanism;
const frame_mod = @import("../protocol/frame.zig");
const FrameHeader = frame_mod.FrameHeader;
const defs = @import("../protocol/definitions.zig");
const described = @import("../protocol/described.zig");
const encoder = @import("../types/encoder.zig");

const log = std.log.scoped(.sasl_client_io);

pub const SaslState = enum {
    not_started,
    header_sent,
    header_exchanged,
    waiting_for_mechanisms,
    init_sent,
    waiting_for_outcome,
    complete,
    err,
};

/// Bytes that arrive after SASL is done: the AMQP protocol header and
/// everything behind it, which a peer may pipeline into the same read.
pub const OnAmqpBytes = *const fn (context: ?*anyopaque, data: []const u8) anyerror!void;

pub const SaslClientIo = struct {
    allocator: Allocator,
    state: SaslState,
    mechanism: Mechanism,
    hostname: ?[]const u8,

    /// The largest SASL frame that will be read. The spec's floor is 512
    /// (§5.3.2), but nothing is negotiated before the outcome, and tokens
    /// used as PLAIN passwords are routinely longer than that.
    max_frame_size: u32,

    /// Bytes not yet forming a whole frame, kept for the next read.
    recv_buf: std.ArrayList(u8),
    header_bytes_received: usize,

    // I/O callbacks
    io_send: ?*const fn (context: ?*anyopaque, data: []const u8) anyerror!void,
    io_context: ?*anyopaque,

    // Completion callback
    on_open_complete: ?*const fn (context: ?*anyopaque, success: bool) void,
    on_open_complete_context: ?*anyopaque,

    on_amqp_bytes: ?OnAmqpBytes,
    on_amqp_bytes_context: ?*anyopaque,

    pub fn init(allocator: Allocator, mechanism: Mechanism, hostname: ?[]const u8) SaslClientIo {
        return .{
            .allocator = allocator,
            .state = .not_started,
            .mechanism = mechanism,
            .hostname = hostname,
            .max_frame_size = 65536,
            .recv_buf = .empty,
            .header_bytes_received = 0,
            .io_send = null,
            .io_context = null,
            .on_open_complete = null,
            .on_open_complete_context = null,
            .on_amqp_bytes = null,
            .on_amqp_bytes_context = null,
        };
    }

    pub fn deinit(self: *SaslClientIo) void {
        self.recv_buf.deinit(self.allocator);
    }

    pub fn setIo(
        self: *SaslClientIo,
        send_fn: *const fn (context: ?*anyopaque, data: []const u8) anyerror!void,
        context: ?*anyopaque,
    ) void {
        self.io_send = send_fn;
        self.io_context = context;
    }

    /// Where the bytes behind the SASL outcome go — normally
    /// `Connection.onBytesReceived`.
    pub fn setOnAmqpBytes(self: *SaslClientIo, cb: OnAmqpBytes, context: ?*anyopaque) void {
        self.on_amqp_bytes = cb;
        self.on_amqp_bytes_context = context;
    }

    /// Begin SASL negotiation by sending the SASL protocol header.
    pub fn open(self: *SaslClientIo) !void {
        if (self.state != .not_started) return error.InvalidState;
        try self.sendBytes(&frame_mod.sasl_header);
        self.state = .header_sent;
    }

    /// Process received bytes during SASL negotiation.
    pub fn onBytesReceived(self: *SaslClientIo, data: []const u8) !void {
        // Everything after the outcome belongs to the AMQP connection.
        if (self.state == .complete) return self.forwardToAmqp(data);
        if (self.state == .err) return error.InvalidState;
        if (self.state == .not_started) return error.InvalidState;

        try self.recv_buf.appendSlice(self.allocator, data);
        try self.receiveHeaderBytes();
        try self.receiveFrames();

        // The peer may pipeline the AMQP header into the same read as the
        // outcome, so whatever is left once SASL is done is not ours.
        if (self.state == .complete and self.recv_buf.items.len > 0) {
            const rest = try self.allocator.dupe(u8, self.recv_buf.items);
            defer self.allocator.free(rest);
            self.recv_buf.clearRetainingCapacity();
            try self.forwardToAmqp(rest);
        }
    }

    /// Handle SASL mechanisms being offered by the server.
    pub fn onMechanismsReceived(self: *SaslClientIo, mechanisms: defs.SaslMechanisms) !void {
        const our_name = self.mechanism.getMechanismName();

        for (mechanisms.sasl_server_mechanisms) |mech_name| {
            if (!std.mem.eql(u8, mech_name, our_name)) continue;

            try self.sendPerformative(.{ .sasl_init = .{
                .mechanism = our_name,
                .initial_response = self.mechanism.getInitBytes(),
                .hostname = self.hostname,
            } });
            self.state = .init_sent;
            return;
        }

        log.warn("Mechanism '{s}' not offered by server", .{our_name});
        self.state = .err;
        return error.MechanismNotOffered;
    }

    /// Handle a SASL challenge by asking the mechanism for its response.
    pub fn onChallengeReceived(self: *SaslClientIo, challenge: defs.SaslChallenge) !void {
        switch (self.state) {
            .init_sent, .waiting_for_outcome => {},
            else => {
                self.state = .err;
                return error.InvalidState;
            },
        }
        const response = self.mechanism.onChallenge(challenge.challenge) orelse &.{};
        try self.sendPerformative(.{ .sasl_response = .{ .response = response } });
        self.state = .waiting_for_outcome;
    }

    /// Handle SASL outcome from server.
    pub fn onOutcomeReceived(self: *SaslClientIo, outcome: defs.SaslOutcome) void {
        if (outcome.code == .ok) {
            self.state = .complete;
            log.debug("SASL authentication successful", .{});
            if (self.on_open_complete) |cb| {
                cb(self.on_open_complete_context, true);
            }
        } else {
            self.state = .err;
            log.warn("SASL authentication failed with code: {d}", .{@intFromEnum(outcome.code)});
            if (self.on_open_complete) |cb| {
                cb(self.on_open_complete_context, false);
            }
        }
    }

    pub fn isComplete(self: *const SaslClientIo) bool {
        return self.state == .complete;
    }

    // ── Internal ──────────────────────────────────────────────────────

    fn sendBytes(self: *SaslClientIo, data: []const u8) !void {
        if (self.io_send) |send_fn| {
            try send_fn(self.io_context, data);
        } else {
            return error.NoIoConfigured;
        }
    }

    fn forwardToAmqp(self: *SaslClientIo, data: []const u8) !void {
        if (data.len == 0) return;
        const cb = self.on_amqp_bytes orelse return error.NoAmqpSink;
        try cb(self.on_amqp_bytes_context, data);
    }

    /// Match the server's SASL header, however few bytes at a time it arrives.
    fn receiveHeaderBytes(self: *SaslClientIo) !void {
        if (self.header_bytes_received >= frame_mod.sasl_header.len) return;

        const have = @min(frame_mod.sasl_header.len, self.recv_buf.items.len);
        // Compare as it arrives, so a wrong header is caught on its first
        // byte rather than after the peer has finished sending it.
        if (!std.mem.eql(u8, self.recv_buf.items[0..have], frame_mod.sasl_header[0..have])) {
            log.warn("Invalid SASL header received", .{});
            self.state = .err;
            return error.InvalidProtocolHeader;
        }
        self.header_bytes_received = have;
        if (have < frame_mod.sasl_header.len) return;

        self.recv_buf.replaceRangeAssumeCapacity(0, have, &.{});
        self.state = .waiting_for_mechanisms;
    }

    fn receiveFrames(self: *SaslClientIo) !void {
        if (self.header_bytes_received < frame_mod.sasl_header.len) return;

        while (self.recv_buf.items.len >= frame_mod.frame_header_size) {
            const header = try FrameHeader.parse(self.recv_buf.items[0..frame_mod.frame_header_size]);
            if (header.frame_type != .sasl) return error.UnexpectedFrameType;
            // SASL frames carry no channel (§5.3.1).
            if (header.channel != 0) return error.InvalidFrame;
            if (header.size > self.max_frame_size) return error.FrameTooLarge;
            if (self.recv_buf.items.len < header.size) return;

            const body = self.recv_buf.items[header.doff * 4 .. header.size];
            if (body.len > 0) try self.handleFrame(body);
            self.recv_buf.replaceRangeAssumeCapacity(0, header.size, &.{});

            // Anything behind the outcome is the AMQP connection's.
            if (self.state == .complete or self.state == .err) return;
        }
    }

    fn handleFrame(self: *SaslClientIo, body: []const u8) !void {
        var decoded = try described.decodePerformative(self.allocator, body);
        defer decoded.deinit();

        switch (decoded.value) {
            .sasl_mechanisms => |mechanisms| try self.onMechanismsReceived(mechanisms),
            .sasl_challenge => |challenge| try self.onChallengeReceived(challenge),
            .sasl_outcome => |outcome| self.onOutcomeReceived(outcome),
            else => {
                log.warn("Unexpected performative during SASL: {s}", .{@tagName(decoded.value)});
                self.state = .err;
                return error.UnexpectedPerformative;
            },
        }
    }

    fn sendPerformative(self: *SaslClientIo, performative: defs.Performative) !void {
        var body = encoder.Buffer.initDynamic(self.allocator);
        defer body.deinit();
        try described.encodePerformative(self.allocator, performative, &body);

        const total = frame_mod.frame_header_size + body.written().len;
        const buf = try self.allocator.alloc(u8, total);
        defer self.allocator.free(buf);

        const header = FrameHeader{
            .size = @intCast(total),
            .doff = 2,
            .frame_type = .sasl,
            .channel = 0,
        };
        @memcpy(buf[0..frame_mod.frame_header_size], &header.serialize());
        @memcpy(buf[frame_mod.frame_header_size..], body.written());
        try self.sendBytes(buf);
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

const testing = std.testing;

/// A SASL server that keeps what the client wrote and builds what a server
/// would write back.
const TestServer = struct {
    sent: std.ArrayList(u8) = .empty,
    scratch: std.ArrayList([]u8) = .empty,
    amqp: std.ArrayList(u8) = .empty,
    allocator: Allocator,

    fn init(allocator: Allocator) TestServer {
        return .{ .allocator = allocator };
    }

    fn deinit(self: *TestServer) void {
        for (self.scratch.items) |buf| self.allocator.free(buf);
        self.scratch.deinit(self.allocator);
        self.sent.deinit(self.allocator);
        self.amqp.deinit(self.allocator);
    }

    fn send(context: ?*anyopaque, data: []const u8) anyerror!void {
        const self: *TestServer = @ptrCast(@alignCast(context.?));
        try self.sent.appendSlice(self.allocator, data);
    }

    fn takeAmqp(context: ?*anyopaque, data: []const u8) anyerror!void {
        const self: *TestServer = @ptrCast(@alignCast(context.?));
        try self.amqp.appendSlice(self.allocator, data);
    }

    fn attach(self: *TestServer, sasl: *SaslClientIo) void {
        sasl.setIo(send, self);
        sasl.setOnAmqpBytes(takeAmqp, self);
    }

    fn written(self: *TestServer) []const u8 {
        return self.sent.items;
    }

    fn clear(self: *TestServer) void {
        self.sent.clearRetainingCapacity();
    }

    /// The bytes a server would send for one SASL performative.
    fn frame(self: *TestServer, performative: defs.Performative) ![]const u8 {
        var body = encoder.Buffer.initDynamic(self.allocator);
        defer body.deinit();
        try described.encodePerformative(self.allocator, performative, &body);

        const total = frame_mod.frame_header_size + body.written().len;
        const buf = try self.allocator.alloc(u8, total);
        try self.scratch.append(self.allocator, buf);

        const header = FrameHeader{
            .size = @intCast(total),
            .doff = 2,
            .frame_type = .sasl,
            .channel = 0,
        };
        @memcpy(buf[0..frame_mod.frame_header_size], &header.serialize());
        @memcpy(buf[frame_mod.frame_header_size..], body.written());
        return buf;
    }

    /// The one performative the client wrote, decoded.
    fn onlyPerformative(self: *TestServer) !described.Decoded(defs.Performative) {
        const bytes = self.written();
        const header = try FrameHeader.parse(bytes[0..frame_mod.frame_header_size]);
        try testing.expectEqual(frame_mod.FrameType.sasl, header.frame_type);
        try testing.expectEqual(@as(usize, bytes.len), header.size);
        return described.decodePerformative(self.allocator, bytes[header.doff * 4 .. header.size]);
    }
};

test "SaslClientIo init" {
    const allocator = testing.allocator;
    var anon = @import("anonymous.zig").Anonymous{};
    const mech = anon.mechanism();

    var sasl = SaslClientIo.init(allocator, mech, "localhost");
    defer sasl.deinit();
    try testing.expectEqual(SaslState.not_started, sasl.state);
    try testing.expect(!sasl.isComplete());
}

test "SaslClientIo outcome handling" {
    const allocator = testing.allocator;
    var anon = @import("anonymous.zig").Anonymous{};
    const mech = anon.mechanism();

    var sasl = SaslClientIo.init(allocator, mech, null);
    defer sasl.deinit();
    sasl.state = .waiting_for_outcome;

    sasl.onOutcomeReceived(.{ .code = .ok });
    try testing.expectEqual(SaslState.complete, sasl.state);
    try testing.expect(sasl.isComplete());
}

test "a full ANONYMOUS negotiation" {
    const allocator = testing.allocator;
    var server = TestServer.init(allocator);
    defer server.deinit();
    var anon = @import("anonymous.zig").Anonymous{};

    var sasl = SaslClientIo.init(allocator, anon.mechanism(), "example.host");
    defer sasl.deinit();
    server.attach(&sasl);

    try sasl.open();
    try testing.expectEqualSlices(u8, &frame_mod.sasl_header, server.written());
    server.clear();

    // Header, then the mechanisms on offer.
    try sasl.onBytesReceived(&frame_mod.sasl_header);
    try testing.expectEqual(SaslState.waiting_for_mechanisms, sasl.state);
    try sasl.onBytesReceived(try server.frame(.{ .sasl_mechanisms = .{
        .sasl_server_mechanisms = &.{ "PLAIN", "ANONYMOUS" },
    } }));

    // Choosing a mechanism means sending an Init, which is what never
    // happened before.
    try testing.expectEqual(SaslState.init_sent, sasl.state);
    var init_frame = try server.onlyPerformative();
    defer init_frame.deinit();
    try testing.expectEqualStrings("ANONYMOUS", init_frame.value.sasl_init.mechanism);
    try testing.expectEqualStrings("example.host", init_frame.value.sasl_init.hostname.?);
    server.clear();

    try sasl.onBytesReceived(try server.frame(.{ .sasl_outcome = .{ .code = .ok } }));
    try testing.expect(sasl.isComplete());
}

test "a PLAIN negotiation carries the credentials" {
    const allocator = testing.allocator;
    var server = TestServer.init(allocator);
    defer server.deinit();

    var plain = @import("plain.zig").Plain.init(allocator, "user", "s3cret", null);
    defer plain.deinit();

    var sasl = SaslClientIo.init(allocator, plain.mechanism(), null);
    defer sasl.deinit();
    server.attach(&sasl);

    try sasl.open();
    server.clear();
    try sasl.onBytesReceived(&frame_mod.sasl_header);
    try sasl.onBytesReceived(try server.frame(.{ .sasl_mechanisms = .{
        .sasl_server_mechanisms = &.{"PLAIN"},
    } }));

    var init_frame = try server.onlyPerformative();
    defer init_frame.deinit();
    try testing.expectEqualStrings("PLAIN", init_frame.value.sasl_init.mechanism);
    // RFC 4616: \0authcid\0passwd
    try testing.expectEqualStrings("\x00user\x00s3cret", init_frame.value.sasl_init.initial_response.?);
    try testing.expectEqual(@as(?[]const u8, null), init_frame.value.sasl_init.hostname);
}

test "a challenge is answered with the mechanism's response" {
    const allocator = testing.allocator;
    var server = TestServer.init(allocator);
    defer server.deinit();

    const Echo = struct {
        last: [32]u8 = undefined,
        len: usize = 0,

        fn name(_: *anyopaque) []const u8 {
            return "ECHO";
        }
        fn initBytes(_: *anyopaque) ?[]const u8 {
            return null;
        }
        fn challenge(ptr: *anyopaque, data: []const u8) ?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            @memcpy(self.last[0..data.len], data);
            self.len = data.len;
            return self.last[0..data.len];
        }
        const vtable = Mechanism.VTable{
            .get_mechanism_name = name,
            .get_init_bytes = initBytes,
            .on_challenge = challenge,
        };
    };
    var echo = Echo{};

    var sasl = SaslClientIo.init(allocator, .{ .ptr = &echo, .vtable = &Echo.vtable }, null);
    defer sasl.deinit();
    server.attach(&sasl);

    try sasl.open();
    try sasl.onBytesReceived(&frame_mod.sasl_header);
    try sasl.onBytesReceived(try server.frame(.{ .sasl_mechanisms = .{
        .sasl_server_mechanisms = &.{"ECHO"},
    } }));
    server.clear();

    try sasl.onBytesReceived(try server.frame(.{ .sasl_challenge = .{ .challenge = "nonce-42" } }));
    try testing.expectEqual(SaslState.waiting_for_outcome, sasl.state);
    var response = try server.onlyPerformative();
    defer response.deinit();
    try testing.expectEqualStrings("nonce-42", response.value.sasl_response.response);
    server.clear();

    try sasl.onBytesReceived(try server.frame(.{ .sasl_outcome = .{ .code = .ok } }));
    try testing.expect(sasl.isComplete());
}

test "the negotiation survives being split at every byte" {
    const allocator = testing.allocator;
    var server = TestServer.init(allocator);
    defer server.deinit();
    var anon = @import("anonymous.zig").Anonymous{};

    var sasl = SaslClientIo.init(allocator, anon.mechanism(), null);
    defer sasl.deinit();
    server.attach(&sasl);
    try sasl.open();
    server.clear();

    var script: std.ArrayList(u8) = .empty;
    defer script.deinit(allocator);
    try script.appendSlice(allocator, &frame_mod.sasl_header);
    try script.appendSlice(allocator, try server.frame(.{ .sasl_mechanisms = .{
        .sasl_server_mechanisms = &.{"ANONYMOUS"},
    } }));
    try script.appendSlice(allocator, try server.frame(.{ .sasl_outcome = .{ .code = .ok } }));

    for (script.items) |byte| {
        try sasl.onBytesReceived(&.{byte});
    }
    try testing.expect(sasl.isComplete());
}

test "the AMQP header behind the outcome is handed on, not parsed as SASL" {
    const allocator = testing.allocator;
    var server = TestServer.init(allocator);
    defer server.deinit();
    var anon = @import("anonymous.zig").Anonymous{};

    var sasl = SaslClientIo.init(allocator, anon.mechanism(), null);
    defer sasl.deinit();
    server.attach(&sasl);
    try sasl.open();
    try sasl.onBytesReceived(&frame_mod.sasl_header);
    try sasl.onBytesReceived(try server.frame(.{ .sasl_mechanisms = .{
        .sasl_server_mechanisms = &.{"ANONYMOUS"},
    } }));
    server.clear();

    // The server pipelines its AMQP header into the same read as the
    // outcome. Feeding those bytes to a frame parser reads 'AMQP' as a
    // 1095586128-byte frame.
    var burst: std.ArrayList(u8) = .empty;
    defer burst.deinit(allocator);
    try burst.appendSlice(allocator, try server.frame(.{ .sasl_outcome = .{ .code = .ok } }));
    try burst.appendSlice(allocator, &frame_mod.amqp_header);
    try burst.appendSlice(allocator, "trailing");

    try sasl.onBytesReceived(burst.items);
    try testing.expect(sasl.isComplete());
    try testing.expectEqualStrings("AMQP\x00\x01\x00\x00trailing", server.amqp.items);

    // And everything after it, too.
    try sasl.onBytesReceived("more");
    try testing.expectEqualStrings("AMQP\x00\x01\x00\x00trailingmore", server.amqp.items);
}

test "a rejected outcome fails the negotiation" {
    const allocator = testing.allocator;
    var server = TestServer.init(allocator);
    defer server.deinit();
    var anon = @import("anonymous.zig").Anonymous{};

    const Done = struct {
        var success: ?bool = null;
        fn onComplete(_: ?*anyopaque, ok: bool) void {
            success = ok;
        }
    };
    Done.success = null;

    var sasl = SaslClientIo.init(allocator, anon.mechanism(), null);
    defer sasl.deinit();
    server.attach(&sasl);
    sasl.on_open_complete = Done.onComplete;

    try sasl.open();
    try sasl.onBytesReceived(&frame_mod.sasl_header);
    try sasl.onBytesReceived(try server.frame(.{ .sasl_mechanisms = .{
        .sasl_server_mechanisms = &.{"ANONYMOUS"},
    } }));
    try sasl.onBytesReceived(try server.frame(.{ .sasl_outcome = .{ .code = .auth } }));

    try testing.expectEqual(SaslState.err, sasl.state);
    try testing.expectEqual(@as(?bool, false), Done.success);
    try testing.expect(!sasl.isComplete());
}

test "a server that does not offer our mechanism is rejected" {
    const allocator = testing.allocator;
    var server = TestServer.init(allocator);
    defer server.deinit();
    var anon = @import("anonymous.zig").Anonymous{};

    var sasl = SaslClientIo.init(allocator, anon.mechanism(), null);
    defer sasl.deinit();
    server.attach(&sasl);
    try sasl.open();
    try sasl.onBytesReceived(&frame_mod.sasl_header);
    server.clear();

    try testing.expectError(error.MechanismNotOffered, sasl.onBytesReceived(try server.frame(.{
        .sasl_mechanisms = .{ .sasl_server_mechanisms = &.{ "PLAIN", "EXTERNAL" } },
    })));
    try testing.expectEqual(SaslState.err, sasl.state);
    try testing.expectEqual(@as(usize, 0), server.written().len);
}

test "a wrong protocol header is rejected on the byte that is wrong" {
    const allocator = testing.allocator;
    var server = TestServer.init(allocator);
    defer server.deinit();
    var anon = @import("anonymous.zig").Anonymous{};

    var sasl = SaslClientIo.init(allocator, anon.mechanism(), null);
    defer sasl.deinit();
    server.attach(&sasl);
    try sasl.open();

    // An AMQP header where a SASL one belongs: the fifth byte is the tell.
    try sasl.onBytesReceived("AMQP");
    try testing.expectError(error.InvalidProtocolHeader, sasl.onBytesReceived(&.{0}));
    try testing.expectEqual(SaslState.err, sasl.state);
}

test "an oversized SASL frame is refused before it is allocated" {
    const allocator = testing.allocator;
    var server = TestServer.init(allocator);
    defer server.deinit();
    var anon = @import("anonymous.zig").Anonymous{};

    var sasl = SaslClientIo.init(allocator, anon.mechanism(), null);
    defer sasl.deinit();
    server.attach(&sasl);
    sasl.max_frame_size = 512;
    try sasl.open();
    try sasl.onBytesReceived(&frame_mod.sasl_header);

    // A header claiming 64 KiB of body, and nothing behind it.
    var hostile: [8]u8 = undefined;
    std.mem.writeInt(u32, hostile[0..4], 65536, .big);
    hostile[4] = 2;
    hostile[5] = @intFromEnum(frame_mod.FrameType.sasl);
    std.mem.writeInt(u16, hostile[6..8], 0, .big);
    try testing.expectError(error.FrameTooLarge, sasl.onBytesReceived(&hostile));
}

test "an AMQP frame during SASL negotiation is refused" {
    const allocator = testing.allocator;
    var server = TestServer.init(allocator);
    defer server.deinit();
    var anon = @import("anonymous.zig").Anonymous{};

    var sasl = SaslClientIo.init(allocator, anon.mechanism(), null);
    defer sasl.deinit();
    server.attach(&sasl);
    try sasl.open();
    try sasl.onBytesReceived(&frame_mod.sasl_header);

    var amqp_frame: [8]u8 = undefined;
    std.mem.writeInt(u32, amqp_frame[0..4], 8, .big);
    amqp_frame[4] = 2;
    amqp_frame[5] = @intFromEnum(frame_mod.FrameType.amqp);
    std.mem.writeInt(u16, amqp_frame[6..8], 0, .big);
    try testing.expectError(error.UnexpectedFrameType, sasl.onBytesReceived(&amqp_frame));
}
