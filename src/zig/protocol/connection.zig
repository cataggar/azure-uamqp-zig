///! AMQP 1.0 Connection state machine (OASIS spec §2.4)
///!
///! Manages the lifecycle of an AMQP connection: protocol header exchange,
///! Open/Close performatives, idle timeout, and frame dispatching.
const std = @import("std");
const Allocator = std.mem.Allocator;
const frame_mod = @import("frame.zig");
const FrameHeader = frame_mod.FrameHeader;
const FrameCodec = @import("frame_codec.zig").FrameCodec;
const defs = @import("definitions.zig");
const described = @import("described.zig");
const encoder = @import("../types/encoder.zig");
const TestPeer = @import("test_peer.zig").TestPeer;
const ManualClock = @import("test_peer.zig").ManualClock;
const testing = std.testing;

const log = std.log.scoped(.amqp_connection);

/// A source of milliseconds, supplied by the caller.
///
/// Reading a clock in Zig 0.16 goes through an `Io`, and this library owns
/// neither the transport nor an event loop, so it does not own the clock
/// either: pass one in, the same way the send function is passed in. Tests
/// pass one they can wind by hand.
///
/// Elapsed times computed from it are clamped at zero, so a clock that steps
/// backwards costs a heartbeat rather than panicking.
pub const Clock = struct {
    context: ?*anyopaque = null,
    read_ms: *const fn (context: ?*anyopaque) i64,

    pub fn nowMs(self: Clock) i64 {
        return self.read_ms(self.context);
    }
};

/// Connection states per AMQP 1.0 §2.4.6
pub const ConnectionState = enum {
    start,
    hdr_rcvd,
    hdr_sent,
    hdr_exch,
    open_pipe,
    open_sent,
    open_rcvd,
    opened,
    close_pipe,
    close_sent,
    close_rcvd,
    end,
    err,
    discarding,
};

/// Callback signatures
pub const OnConnectionStateChanged = *const fn (
    context: ?*anyopaque,
    new_state: ConnectionState,
    previous_state: ConnectionState,
) void;

/// Neither `performative` nor `payload` outlives the call: the performative is
/// decoded into an arena released once the frame has been handled. Copy
/// anything worth keeping.
pub const OnEndpointFrameReceived = *const fn (
    context: ?*anyopaque,
    performative: defs.Performative,
    channel: u16,
    payload: []const u8,
) void;

/// An endpoint (session) registered on a connection.
pub const Endpoint = struct {
    id: u32,
    on_frame_received: OnEndpointFrameReceived,
    context: ?*anyopaque,
    incoming_channel: ?u16 = null,
    outgoing_channel: ?u16 = null,
};

/// AMQP Connection — manages protocol header exchange, open/close,
/// idle timeout tracking, and frame dispatch to session endpoints.
pub const Connection = struct {
    allocator: Allocator,
    state: ConnectionState,
    container_id: []const u8,
    hostname: ?[]const u8,
    max_frame_size: u32,
    channel_max: u16,
    idle_timeout_ms: ?u32,

    // Remote peer settings (populated after receiving Open)
    remote_max_frame_size: u32,
    remote_channel_max: u16,
    remote_idle_timeout_ms: ?u32,

    // Frame codec
    frame_codec: FrameCodec,

    // Endpoints (sessions)
    endpoints: std.ArrayList(Endpoint),
    next_endpoint_id: u32,

    // Protocol header exchange. The header can arrive in as few as one byte at
    // a time, so it is accumulated rather than required whole.
    header_buf: [frame_mod.amqp_header.len]u8,
    header_bytes_received: usize,

    /// Set once this connection has subscribed to its own frame codec. The
    /// codec holds `self`, so it cannot be done in `init`, which returns by
    /// value.
    subscribed: bool,

    /// An error raised while handling a received frame. The codec's dispatch
    /// callback cannot fail, so the error is parked here and returned by
    /// `onBytesReceived`.
    pending_error: ?anyerror,

    // Timing. Null until `setClock`; without it there is nothing to compare
    // an idle timeout against.
    clock: ?Clock,
    last_frame_received_ms: i64,
    last_frame_sent_ms: i64,

    // Callbacks
    on_state_changed: ?OnConnectionStateChanged,
    on_state_changed_context: ?*anyopaque,

    // I/O (abstracted)
    io_send: ?*const fn (context: ?*anyopaque, data: []const u8) anyerror!void,
    io_context: ?*anyopaque,

    pub fn init(
        allocator: Allocator,
        container_id: []const u8,
        hostname: ?[]const u8,
        opts: struct {
            max_frame_size: u32 = 4294967295,
            channel_max: u16 = 65535,
            idle_timeout_ms: ?u32 = null,
            clock: ?Clock = null,
        },
    ) Connection {
        return .{
            .allocator = allocator,
            .state = .start,
            .container_id = container_id,
            .hostname = hostname,
            .max_frame_size = opts.max_frame_size,
            .channel_max = opts.channel_max,
            .idle_timeout_ms = opts.idle_timeout_ms,
            .remote_max_frame_size = frame_mod.min_max_frame_size,
            .remote_channel_max = 0,
            .remote_idle_timeout_ms = null,
            .frame_codec = FrameCodec.init(allocator, opts.max_frame_size),
            .endpoints = .empty,
            .next_endpoint_id = 0,
            .header_buf = undefined,
            .header_bytes_received = 0,
            .subscribed = false,
            .pending_error = null,
            .clock = opts.clock,
            .last_frame_received_ms = if (opts.clock) |c| c.nowMs() else 0,
            .last_frame_sent_ms = if (opts.clock) |c| c.nowMs() else 0,
            .on_state_changed = null,
            .on_state_changed_context = null,
            .io_send = null,
            .io_context = null,
        };
    }

    pub fn deinit(self: *Connection) void {
        self.frame_codec.deinit();
        self.endpoints.deinit(self.allocator);
    }

    /// Set the I/O send callback.
    pub fn setIo(
        self: *Connection,
        send_fn: *const fn (context: ?*anyopaque, data: []const u8) anyerror!void,
        context: ?*anyopaque,
    ) void {
        self.io_send = send_fn;
        self.io_context = context;
    }

    /// Supply the clock used for idle timeouts and keep-alives, and start
    /// both intervals from now.
    pub fn setClock(self: *Connection, clock: Clock) void {
        self.clock = clock;
        self.last_frame_received_ms = clock.nowMs();
        self.last_frame_sent_ms = clock.nowMs();
    }

    /// Set the state change callback.
    pub fn setOnStateChanged(self: *Connection, cb: OnConnectionStateChanged, context: ?*anyopaque) void {
        self.on_state_changed = cb;
        self.on_state_changed_context = context;
    }

    /// Register an endpoint (session) on this connection and return its id.
    ///
    /// Endpoints are addressed by id, never by a stored pointer: the backing
    /// array moves when it grows, and removing an endpoint shifts the ones
    /// after it. Look one up with `endpoint` and treat that pointer as valid
    /// only until the next call that adds or removes an endpoint.
    /// The endpoint is given the lowest unused outgoing channel, which is the
    /// channel its frames are sent on for the life of the connection (§2.5.1).
    pub fn createEndpoint(self: *Connection, callback: OnEndpointFrameReceived, context: ?*anyopaque) !u32 {
        const channel = self.nextFreeOutgoingChannel() orelse return error.TooManyChannels;
        const id = self.next_endpoint_id;
        try self.endpoints.append(self.allocator, .{
            .id = id,
            .on_frame_received = callback,
            .context = context,
            .outgoing_channel = channel,
        });
        self.next_endpoint_id += 1;
        return id;
    }

    fn nextFreeOutgoingChannel(self: *Connection) ?u16 {
        var candidate: u16 = 0;
        while (true) : (candidate += 1) {
            const taken = for (self.endpoints.items) |ep| {
                if (ep.outgoing_channel == candidate) break true;
            } else false;
            if (!taken) return candidate;
            if (candidate == self.channel_max) return null;
        }
    }

    /// Look up an endpoint by id. The pointer is valid only until the next
    /// call that adds or removes an endpoint.
    pub fn endpoint(self: *Connection, id: u32) ?*Endpoint {
        for (self.endpoints.items) |*ep| {
            if (ep.id == id) return ep;
        }
        return null;
    }

    /// Unregister an endpoint. Does nothing if the id is unknown, so
    /// destroying twice is safe.
    pub fn destroyEndpoint(self: *Connection, id: u32) void {
        for (self.endpoints.items, 0..) |*ep, i| {
            if (ep.id == id) {
                // Order carries no meaning — endpoints are found by id.
                _ = self.endpoints.swapRemove(i);
                return;
            }
        }
    }

    /// Initiate the connection by sending the AMQP protocol header.
    ///
    /// The Open performative follows once the peer's header comes back: it may
    /// not be sent before the headers have been exchanged (§2.4.1).
    pub fn open(self: *Connection) !void {
        if (self.state != .start) return error.InvalidState;
        try self.ensureSubscribed();
        try self.sendBytes(&frame_mod.amqp_header);
        self.setState(.hdr_sent);
    }

    /// Send a Close performative to gracefully shut down.
    pub fn close(self: *Connection, err_condition: ?[]const u8, err_description: ?[]const u8) !void {
        const next: ConnectionState = switch (self.state) {
            // Closing before the peer has answered our Open is allowed; so is
            // closing while opened.
            .open_sent, .open_rcvd, .opened => .close_sent,
            // The peer closed first, so this is the reply that ends it.
            .close_rcvd => .end,
            else => return error.InvalidState,
        };

        try self.sendPerformative(0, .{ .close = .{
            .err = if (err_condition) |condition| .{
                .condition = condition,
                .description = err_description,
            } else null,
        } }, &.{});
        self.setState(next);
    }

    /// Send a performative on a channel, with an optional payload after it.
    ///
    /// This is how a session puts a frame on the wire; the connection owns the
    /// framing and the negotiated limits.
    pub fn sendPerformative(
        self: *Connection,
        channel: u16,
        performative: defs.Performative,
        payload: []const u8,
    ) !void {
        if (channel > self.remote_channel_max) return error.ChannelOutOfRange;

        var body = encoder.Buffer.initDynamic(self.allocator);
        defer body.deinit();
        try described.encodePerformative(self.allocator, performative, &body);
        try body.writeAll(payload);

        try self.sendFrame(.amqp, channel, body.written());
    }

    /// Process incoming bytes from the transport.
    pub fn onBytesReceived(self: *Connection, data: []const u8) !void {
        try self.ensureSubscribed();

        var rest = data;
        if (self.header_bytes_received < frame_mod.amqp_header.len) {
            rest = try self.receiveHeaderBytes(data);
            if (rest.len == 0) return;
        }

        switch (self.state) {
            .hdr_exch, .open_sent, .open_rcvd, .opened, .close_sent, .close_rcvd => {
                try self.frame_codec.receiveBytes(rest);
                if (self.pending_error) |err| {
                    self.pending_error = null;
                    return err;
                }
            },
            else => {
                log.warn("Bytes received in unexpected state: {s}", .{@tagName(self.state)});
            },
        }
    }

    /// Accumulate the 8-byte protocol header, however few bytes at a time it
    /// arrives in, and return whatever follows it.
    fn receiveHeaderBytes(self: *Connection, data: []const u8) ![]const u8 {
        if (self.state != .start and self.state != .hdr_sent) {
            self.setState(.err);
            return error.InvalidState;
        }

        const wanted = frame_mod.amqp_header.len - self.header_bytes_received;
        const take = @min(wanted, data.len);
        @memcpy(self.header_buf[self.header_bytes_received..][0..take], data[0..take]);
        self.header_bytes_received += take;

        // Compare as it arrives, so a wrong header is caught on its first byte
        // rather than after the peer has finished sending it.
        if (!std.mem.eql(u8, self.header_buf[0..self.header_bytes_received], frame_mod.amqp_header[0..self.header_bytes_received])) {
            // Returned to the caller as well; this is a breadcrumb, not the report.
            log.warn("Invalid protocol header received", .{});
            self.setState(.err);
            return error.InvalidProtocolHeader;
        }
        if (self.header_bytes_received < frame_mod.amqp_header.len) return &.{};

        if (self.state == .hdr_sent) {
            self.setState(.hdr_exch);
        } else {
            self.setState(.hdr_rcvd);
            try self.sendBytes(&frame_mod.amqp_header);
            self.setState(.hdr_exch);
        }
        try self.sendOpen();
        return data[take..];
    }

    /// Called periodically to send keep-alives and detect a peer that has
    /// gone quiet. Returns `error.IdleTimeout` once the peer has been silent
    /// for longer than the idle timeout we advertised.
    pub fn doWork(self: *Connection) !void {
        if (self.state != .opened) return;
        if (self.idle_timeout_ms == null and self.remote_idle_timeout_ms == null) return;
        // Idle handling is entirely about elapsed time, so there is nothing
        // honest to do without a clock — quietly doing nothing would look
        // like a healthy connection.
        if (self.clock == null) return error.NoClockConfigured;

        // The peer must send something within the idle timeout *we*
        // advertised in our Open — that is the promise it made by accepting
        // it (§2.4.5).
        if (self.idle_timeout_ms) |timeout| {
            if (self.elapsedSince(self.last_frame_received_ms) > timeout) {
                log.warn("Peer idle for longer than the {d}ms timeout we advertised", .{timeout});
                // Saying why is a courtesy the peer may not be alive to hear.
                self.close("amqp:resource-limit-exceeded", "idle timeout exceeded") catch {};
                self.setState(.err);
                return error.IdleTimeout;
            }
        }

        // Conversely, the peer will drop us if we are silent for longer than
        // the timeout *it* advertised, so an empty frame goes out at half
        // that, leaving a whole interval of slack for a slow round trip.
        if (self.remote_idle_timeout_ms) |timeout| {
            if (timeout > 0 and self.elapsedSince(self.last_frame_sent_ms) >= timeout / 2) {
                try self.sendEmptyFrame();
            }
        }
    }

    // ── Internal ──────────────────────────────────────────────────────

    /// Subscribe to the frame codec this connection owns.
    ///
    /// The codec stores `self`, so this cannot happen in `init` — that returns
    /// by value, and the address of a temporary is not the address the caller
    /// ends up with.
    fn ensureSubscribed(self: *Connection) Allocator.Error!void {
        if (self.subscribed) return;
        try self.frame_codec.subscribe(.amqp, onFrameReceived, self);
        self.subscribed = true;
    }

    fn onFrameReceived(context: ?*anyopaque, header: FrameHeader, body: []const u8) void {
        const self: *Connection = @ptrCast(@alignCast(context.?));
        self.handleFrame(header, body) catch |err| {
            // The codec's callback cannot fail, so park the error for
            // `onBytesReceived` to return.
            log.warn("Frame handling failed: {s}", .{@errorName(err)});
            if (self.pending_error == null) self.pending_error = err;
            self.setState(.err);
        };
    }

    fn handleFrame(self: *Connection, header: FrameHeader, body: []const u8) !void {
        // Any frame at all proves the peer is alive, heartbeats included.
        self.last_frame_received_ms = self.nowMs();
        if (header.channel > self.channel_max) return error.ChannelOutOfRange;

        // An empty frame is a heartbeat: it says the peer is alive and nothing
        // more (§2.4.5).
        if (body.len == 0) return;

        var decoded = try described.decodePerformative(self.allocator, body);
        defer decoded.deinit();
        const payload = body[decoded.bytes_consumed..];

        switch (decoded.value) {
            .open => |open_perf| try self.onOpenReceived(open_perf),
            .close => |close_perf| try self.onCloseReceived(close_perf),
            else => self.dispatchToEndpoint(header.channel, decoded.value, payload),
        }
    }

    /// Hand a session-scoped performative to whichever endpoint owns the
    /// channel it arrived on.
    fn dispatchToEndpoint(
        self: *Connection,
        channel: u16,
        performative: defs.Performative,
        payload: []const u8,
    ) void {
        for (self.endpoints.items) |ep| {
            if (ep.incoming_channel == channel) {
                ep.on_frame_received(ep.context, performative, channel, payload);
                return;
            }
        }
        // A channel is claimed by the Begin that opens the session on it
        // (§2.5.1), and by nothing else: a stray frame on an unmapped channel
        // must not be handed to a session that has not been told it owns one.
        if (performative == .begin) {
            // The peer echoes our channel back in `remote-channel`, which says
            // exactly which of our sessions its channel pairs with.
            if (performative.begin.remote_channel) |ours| {
                for (self.endpoints.items) |*ep| {
                    if (ep.outgoing_channel == ours and ep.incoming_channel == null) {
                        ep.incoming_channel = channel;
                        ep.on_frame_received(ep.context, performative, channel, payload);
                        return;
                    }
                }
            }
            // The peer began the session, so pair it with one still waiting.
            for (self.endpoints.items) |*ep| {
                if (ep.incoming_channel == null) {
                    ep.incoming_channel = channel;
                    ep.on_frame_received(ep.context, performative, channel, payload);
                    return;
                }
            }
        }
        log.warn("Frame on unmapped channel {d} dropped", .{channel});
    }

    fn onOpenReceived(self: *Connection, open_perf: defs.Open) !void {
        // A peer that advertises less than the spec minimum is not one this
        // library can talk to without violating §2.4.1.
        if (open_perf.max_frame_size < frame_mod.min_max_frame_size) return error.FrameSizeTooSmall;

        self.remote_max_frame_size = open_perf.max_frame_size;
        self.remote_channel_max = open_perf.channel_max;
        self.remote_idle_timeout_ms = open_perf.idle_time_out;

        switch (self.state) {
            .open_sent => self.setState(.opened),
            .hdr_exch => {
                self.setState(.open_rcvd);
                try self.sendOpen();
            },
            else => {
                log.warn("Open received in unexpected state: {s}", .{@tagName(self.state)});
                return error.InvalidState;
            },
        }
    }

    fn onCloseReceived(self: *Connection, close_perf: defs.Close) !void {
        if (close_perf.err) |err| {
            log.warn("Peer closed with {s}: {s}", .{ err.condition, err.description orelse "" });
        }
        switch (self.state) {
            // We asked to close and the peer agreed.
            .close_sent, .discarding => self.setState(.end),
            else => {
                self.setState(.close_rcvd);
                // Answering is mandatory, and it is the last thing sent.
                try self.sendPerformative(0, .{ .close = .{} }, &.{});
                self.setState(.end);
            },
        }
    }

    fn sendOpen(self: *Connection) !void {
        if (self.state != .hdr_exch and self.state != .open_rcvd) return;
        try self.sendPerformative(0, .{ .open = .{
            .container_id = self.container_id,
            .hostname = self.hostname,
            .max_frame_size = self.max_frame_size,
            .channel_max = self.channel_max,
            .idle_time_out = self.idle_timeout_ms,
        } }, &.{});
        self.setState(if (self.state == .open_rcvd) .opened else .open_sent);
    }

    /// Frame a body and hand it to the transport, respecting the frame size
    /// the peer asked for in its Open.
    fn sendFrame(self: *Connection, frame_type: frame_mod.FrameType, channel: u16, body: []const u8) !void {
        const total = frame_mod.frame_header_size + body.len;
        if (total > self.remote_max_frame_size) return error.FrameTooLarge;

        const buf = try self.allocator.alloc(u8, total);
        defer self.allocator.free(buf);

        const header = FrameHeader{
            .size = @intCast(total),
            .doff = 2,
            .frame_type = frame_type,
            .channel = channel,
        };
        @memcpy(buf[0..frame_mod.frame_header_size], &header.serialize());
        @memcpy(buf[frame_mod.frame_header_size..], body);
        try self.sendBytes(buf);
    }

    /// The current time, or the last time a frame arrived when no clock has
    /// been supplied. Public so the layers above can share one clock.
    pub fn nowMs(self: *Connection) i64 {
        const clock = self.clock orelse return self.last_frame_received_ms;
        return clock.nowMs();
    }

    /// Milliseconds since `since`, never negative: a wall clock can step
    /// backwards, and the subtraction used to be `@intCast`, which panics.
    fn elapsedSince(self: *Connection, since: i64) u64 {
        const delta = self.nowMs() -| since;
        return if (delta < 0) 0 else @intCast(delta);
    }

    fn setState(self: *Connection, new_state: ConnectionState) void {
        if (self.state == new_state) return;
        const prev = self.state;
        self.state = new_state;
        log.info("Connection state: {s} -> {s}", .{ @tagName(prev), @tagName(new_state) });
        if (self.on_state_changed) |cb| {
            cb(self.on_state_changed_context, new_state, prev);
        }
    }

    fn sendBytes(self: *Connection, data: []const u8) !void {
        if (self.io_send) |send_fn| {
            try send_fn(self.io_context, data);
            self.last_frame_sent_ms = self.nowMs();
        } else {
            return error.NoIoConfigured;
        }
    }

    fn sendEmptyFrame(self: *Connection) !void {
        const hdr = FrameHeader{
            .size = frame_mod.frame_header_size,
            .doff = 2,
            .frame_type = .amqp,
            .channel = 0,
        };
        const bytes = hdr.serialize();
        try self.sendBytes(&bytes);
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "Connection init and state" {
    const allocator = std.testing.allocator;
    var conn = Connection.init(allocator, "test-container", "localhost", .{});
    defer conn.deinit();
    try std.testing.expectEqual(ConnectionState.start, conn.state);
}

test "Connection state transitions" {
    const allocator = std.testing.allocator;

    var sent_data: ?[]const u8 = null;
    const S = struct {
        fn send(ctx: ?*anyopaque, data: []const u8) anyerror!void {
            const ptr: *?[]const u8 = @ptrCast(@alignCast(ctx.?));
            ptr.* = data;
        }
    };

    var conn = Connection.init(allocator, "test", null, .{});
    defer conn.deinit();
    conn.setIo(S.send, @ptrCast(&sent_data));

    try conn.open();
    try std.testing.expectEqual(ConnectionState.hdr_sent, conn.state);
    try std.testing.expect(sent_data != null);
}

test "endpoints stay addressable as the connection's storage changes" {
    const allocator = std.testing.allocator;
    var conn = Connection.init(allocator, "test", null, .{});
    defer conn.deinit();

    const S = struct {
        fn onFrame(_: ?*anyopaque, _: defs.Performative, _: u16, _: []const u8) void {}
    };

    var ids: [64]u32 = undefined;
    for (&ids, 0..) |*id, i| {
        id.* = try conn.createEndpoint(S.onFrame, null);
        try std.testing.expectEqual(@as(u32, @intCast(i)), id.*);
    }

    // Growth used to invalidate every pointer handed out before it.
    conn.endpoint(ids[0]).?.incoming_channel = 7;
    conn.endpoint(ids[63]).?.incoming_channel = 9;
    try std.testing.expectEqual(@as(?u16, 7), conn.endpoint(ids[0]).?.incoming_channel);

    // Removal used to shift every later endpoint onto its neighbour's data.
    conn.destroyEndpoint(ids[1]);
    try std.testing.expect(conn.endpoint(ids[1]) == null);
    try std.testing.expectEqual(@as(?u16, 7), conn.endpoint(ids[0]).?.incoming_channel);
    try std.testing.expectEqual(@as(?u16, 9), conn.endpoint(ids[63]).?.incoming_channel);
    try std.testing.expectEqual(@as(usize, 63), conn.endpoints.items.len);

    conn.destroyEndpoint(ids[1]);
    try std.testing.expectEqual(@as(usize, 63), conn.endpoints.items.len);
}

test "opening exchanges headers and Opens" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "container-1", "example.host", .{
        .max_frame_size = 65536,
        .channel_max = 16,
        .idle_timeout_ms = 30000,
    });
    defer conn.deinit();
    peer.attach(&conn);

    try conn.open();
    try testing.expectEqual(ConnectionState.hdr_sent, conn.state);
    try testing.expectEqualSlices(u8, &frame_mod.amqp_header, peer.written());

    // The peer answers the header; ours goes out with an Open behind it.
    peer.clear();
    try conn.onBytesReceived(&frame_mod.amqp_header);
    try testing.expectEqual(ConnectionState.open_sent, conn.state);

    var sent = try peer.lastPerformative();
    defer sent.deinit();
    try testing.expectEqualStrings("container-1", sent.value.open.container_id);
    try testing.expectEqualStrings("example.host", sent.value.open.hostname.?);
    try testing.expectEqual(@as(u32, 65536), sent.value.open.max_frame_size);
    try testing.expectEqual(@as(u16, 16), sent.value.open.channel_max);
    try testing.expectEqual(@as(?u32, 30000), sent.value.open.idle_time_out);

    // And the peer's Open completes it.
    try conn.onBytesReceived(try peer.frame(0, .{ .open = .{
        .container_id = "peer",
        .max_frame_size = 4096,
        .channel_max = 8,
        .idle_time_out = 60000,
    } }));
    try testing.expectEqual(ConnectionState.opened, conn.state);
    try testing.expectEqual(@as(u32, 4096), conn.remote_max_frame_size);
    try testing.expectEqual(@as(u16, 8), conn.remote_channel_max);
    try testing.expectEqual(@as(?u32, 60000), conn.remote_idle_timeout_ms);
}

test "the peer's header may arrive one byte at a time" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "c", null, .{});
    defer conn.deinit();
    peer.attach(&conn);

    try conn.open();
    for (frame_mod.amqp_header[0..7]) |byte| {
        try conn.onBytesReceived(&.{byte});
        try testing.expectEqual(ConnectionState.hdr_sent, conn.state);
    }
    try conn.onBytesReceived(frame_mod.amqp_header[7..8]);
    try testing.expectEqual(ConnectionState.open_sent, conn.state);
}

test "a header and the frame behind it may arrive together" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "c", null, .{});
    defer conn.deinit();
    peer.attach(&conn);

    try conn.open();

    const open_frame = try peer.frame(0, .{ .open = .{ .container_id = "peer" } });
    var both = std.ArrayList(u8).empty;
    defer both.deinit(allocator);
    try both.appendSlice(allocator, &frame_mod.amqp_header);
    try both.appendSlice(allocator, open_frame);

    try conn.onBytesReceived(both.items);
    try testing.expectEqual(ConnectionState.opened, conn.state);
}

test "a wrong protocol header is rejected on the byte that is wrong" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "c", null, .{});
    defer conn.deinit();
    peer.attach(&conn);

    try conn.open();
    // A SASL header where an AMQP one was expected: same first four bytes.
    try conn.onBytesReceived("AMQP");
    try testing.expectError(error.InvalidProtocolHeader, conn.onBytesReceived(&.{3}));
    try testing.expectEqual(ConnectionState.err, conn.state);
}

test "a peer that opens first is answered" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "c", null, .{});
    defer conn.deinit();
    peer.attach(&conn);

    // No local open(): the peer speaks first.
    try conn.onBytesReceived(&frame_mod.amqp_header);
    try testing.expectEqualSlices(u8, &frame_mod.amqp_header, peer.written()[0..8]);
    try testing.expectEqual(ConnectionState.open_sent, conn.state);

    try conn.onBytesReceived(try peer.frame(0, .{ .open = .{ .container_id = "peer" } }));
    try testing.expectEqual(ConnectionState.opened, conn.state);
}

/// Drive a connection to `opened` with the peer's limits.
fn openedConnection(peer: *TestPeer, conn: *Connection, peer_open: defs.Open) !void {
    peer.attach(conn);
    try conn.open();
    try conn.onBytesReceived(&frame_mod.amqp_header);
    try conn.onBytesReceived(try peer.frame(0, .{ .open = peer_open }));
    peer.clear();
}

test "closing is a Close each way" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "c", null, .{});
    defer conn.deinit();
    try openedConnection(&peer, &conn, .{ .container_id = "peer" });

    try conn.close("amqp:connection:forced", "shutting down");
    try testing.expectEqual(ConnectionState.close_sent, conn.state);

    var sent = try peer.lastPerformative();
    defer sent.deinit();
    try testing.expectEqualStrings("amqp:connection:forced", sent.value.close.err.?.condition);
    try testing.expectEqualStrings("shutting down", sent.value.close.err.?.description.?);

    try conn.onBytesReceived(try peer.frame(0, .{ .close = .{} }));
    try testing.expectEqual(ConnectionState.end, conn.state);
}

test "a peer closing first is answered before ending" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "c", null, .{});
    defer conn.deinit();
    try openedConnection(&peer, &conn, .{ .container_id = "peer" });

    try conn.onBytesReceived(try peer.frame(0, .{ .close = .{
        .err = .{ .condition = "amqp:internal-error" },
    } }));

    var reply = try peer.lastPerformative();
    defer reply.deinit();
    try testing.expect(reply.value == .close);
    try testing.expectEqual(@as(?defs.AmqpError, null), reply.value.close.err);
    try testing.expectEqual(ConnectionState.end, conn.state);
}

test "the negotiated frame size and channel limit are enforced" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "c", null, .{ .channel_max = 4 });
    defer conn.deinit();
    try openedConnection(&peer, &conn, .{
        .container_id = "peer",
        .max_frame_size = 512,
        .channel_max = 2,
    });

    // The peer said 512 bytes; a payload past that cannot go out.
    const big = try allocator.alloc(u8, 600);
    defer allocator.free(big);
    @memset(big, 0);
    try testing.expectError(
        error.FrameTooLarge,
        conn.sendPerformative(0, .{ .flow = .{ .incoming_window = 1, .next_outgoing_id = 0, .outgoing_window = 1 } }, big),
    );

    // The peer said channel-max 2, so channel 3 does not exist for it.
    try testing.expectError(
        error.ChannelOutOfRange,
        conn.sendPerformative(3, .{ .end = .{} }, &.{}),
    );

    // And a frame arriving above our own channel-max is a protocol error.
    try testing.expectError(error.ChannelOutOfRange, conn.onBytesReceived(try peer.frame(5, .{ .end = .{} })));
}

test "a peer offering less than the minimum frame size is refused" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "c", null, .{});
    defer conn.deinit();
    peer.attach(&conn);

    try conn.open();
    try conn.onBytesReceived(&frame_mod.amqp_header);
    try testing.expectError(error.FrameSizeTooSmall, conn.onBytesReceived(try peer.frame(0, .{ .open = .{
        .container_id = "peer",
        .max_frame_size = 511,
    } })));
    try testing.expectEqual(ConnectionState.err, conn.state);
}

test "an empty frame is a heartbeat and changes nothing" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "c", null, .{});
    defer conn.deinit();
    try openedConnection(&peer, &conn, .{ .container_id = "peer" });

    const heartbeat = (FrameHeader{
        .size = @intCast(frame_mod.frame_header_size),
        .doff = 2,
        .frame_type = .amqp,
        .channel = 0,
    }).serialize();
    try conn.onBytesReceived(&heartbeat);

    try testing.expectEqual(ConnectionState.opened, conn.state);
    try testing.expectEqual(@as(usize, 0), peer.written().len);
}

test "session frames reach the endpoint that owns the channel" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "c", null, .{});
    defer conn.deinit();
    try openedConnection(&peer, &conn, .{ .container_id = "peer" });

    const Seen = struct {
        var channel: ?u16 = null;
        var tag: ?std.meta.Tag(defs.Performative) = null;
        var payload_len: usize = 0;
        fn onFrame(_: ?*anyopaque, performative: defs.Performative, channel_in: u16, payload: []const u8) void {
            channel = channel_in;
            tag = std.meta.activeTag(performative);
            payload_len = payload.len;
        }
        fn reset() void {
            channel = null;
            tag = null;
            payload_len = 0;
        }
    };
    Seen.reset();

    const first = try conn.createEndpoint(Seen.onFrame, null);
    const second = try conn.createEndpoint(Seen.onFrame, null);
    conn.endpoint(second).?.incoming_channel = 7;

    // Channel 7 is claimed, so it goes there.
    try conn.onBytesReceived(try peer.frame(7, .{ .end = .{} }));
    try testing.expectEqual(@as(?u16, 7), Seen.channel);
    try testing.expectEqual(std.meta.Tag(defs.Performative).end, Seen.tag.?);

    // Channel 2 is not, so the endpoint still waiting for one takes it — this
    // is how a peer-initiated Begin finds its session.
    Seen.reset();
    try conn.onBytesReceived(try peer.frame(2, .{ .begin = .{
        .next_outgoing_id = 0,
        .incoming_window = 1,
        .outgoing_window = 1,
    } }));
    try testing.expectEqual(@as(?u16, 2), Seen.channel);
    try testing.expectEqual(@as(?u16, 2), conn.endpoint(first).?.incoming_channel);

    // Open and Close stay with the connection and never reach an endpoint.
    Seen.reset();
    try conn.onBytesReceived(try peer.frame(0, .{ .close = .{} }));
    try testing.expectEqual(@as(?u16, null), Seen.channel);
}

test "a transfer's payload is handed over separately from its performative" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "c", null, .{});
    defer conn.deinit();
    try openedConnection(&peer, &conn, .{ .container_id = "peer" });

    const Seen = struct {
        var payload: []const u8 = &.{};
        fn onFrame(_: ?*anyopaque, _: defs.Performative, _: u16, p: []const u8) void {
            payload = p;
        }
    };
    Seen.payload = &.{};

    const id = try conn.createEndpoint(Seen.onFrame, null);
    conn.endpoint(id).?.incoming_channel = 1;

    // Build a transfer frame by hand so a body follows the performative.
    var body = encoder.Buffer.initDynamic(allocator);
    defer body.deinit();
    try described.encodePerformative(allocator, .{ .transfer = .{ .handle = 0, .delivery_id = 1 } }, &body);
    try body.writeAll("message-bytes");

    const total = frame_mod.frame_header_size + body.written().len;
    const buf = try allocator.alloc(u8, total);
    defer allocator.free(buf);
    @memcpy(buf[0..frame_mod.frame_header_size], &(FrameHeader{
        .size = @intCast(total),
        .doff = 2,
        .frame_type = .amqp,
        .channel = 1,
    }).serialize());
    @memcpy(buf[frame_mod.frame_header_size..], body.written());

    try conn.onBytesReceived(buf);
    try testing.expectEqualStrings("message-bytes", Seen.payload);
}

test "sending before the connection is open is refused" {
    const allocator = testing.allocator;
    var conn = Connection.init(allocator, "c", null, .{});
    defer conn.deinit();

    // No transport configured at all.
    try testing.expectError(error.NoIoConfigured, conn.open());
    // Close is not a thing that can happen from `start`.
    try testing.expectError(error.InvalidState, conn.close(null, null));
}

/// The 8 bytes of an empty frame: a header claiming a body that is not there.
fn isHeartbeat(bytes: []const u8) bool {
    if (bytes.len != frame_mod.frame_header_size) return false;
    const header = FrameHeader.parse(bytes[0..frame_mod.frame_header_size]) catch return false;
    return header.frame_type == .amqp and header.channel == 0 and header.bodySize() == 0;
}

test "a keep-alive goes out at half the timeout the peer advertised" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var clock = ManualClock{ .ms = 1_000_000 };
    var conn = Connection.init(allocator, "c", null, .{ .clock = clock.clock() });
    defer conn.deinit();
    try peer.openConnectionAdvertising(&conn, 10_000);

    // A whisker short of half is still too early: the point of halving is to
    // leave a full interval of slack, not to send twice as often.
    clock.advance(4_999);
    try conn.doWork();
    try testing.expectEqual(@as(usize, 0), peer.written().len);

    clock.advance(1);
    try conn.doWork();
    try testing.expect(isHeartbeat(peer.written()));

    // Having just sent one, the interval starts over.
    peer.clear();
    try conn.doWork();
    try testing.expectEqual(@as(usize, 0), peer.written().len);

    clock.advance(5_000);
    try conn.doWork();
    try testing.expect(isHeartbeat(peer.written()));
}

test "no keep-alives when the peer advertised no idle timeout" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var clock = ManualClock{};
    var conn = Connection.init(allocator, "c", null, .{
        .idle_timeout_ms = 30_000,
        .clock = clock.clock(),
    });
    defer conn.deinit();
    try peer.openConnection(&conn);

    // The peer never promised to drop us, so silence costs nothing.
    clock.advance(29_000);
    try conn.doWork();
    try testing.expectEqual(@as(usize, 0), peer.written().len);
}

test "anything else we send postpones the keep-alive" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var clock = ManualClock{};
    var conn = Connection.init(allocator, "c", null, .{ .clock = clock.clock() });
    defer conn.deinit();
    try peer.openConnectionAdvertising(&conn, 10_000);

    clock.advance(4_000);
    try conn.sendPerformative(0, .{ .begin = .{ .next_outgoing_id = 0, .incoming_window = 1, .outgoing_window = 1 } }, &.{});
    peer.clear();

    // A heartbeat exists only to prove liveness, and the Begin just did.
    clock.advance(4_000);
    try conn.doWork();
    try testing.expectEqual(@as(usize, 0), peer.written().len);

    clock.advance(1_000);
    try conn.doWork();
    try testing.expect(isHeartbeat(peer.written()));
}

test "a peer quiet past the timeout we advertised fails the connection" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var clock = ManualClock{};
    var conn = Connection.init(allocator, "c", null, .{
        .idle_timeout_ms = 10_000,
        .clock = clock.clock(),
    });
    defer conn.deinit();
    try peer.openConnectionAdvertising(&conn, 10_000);

    // Exactly the timeout is within the promise; past it is not.
    clock.advance(10_000);
    peer.clear();
    try conn.doWork();
    try testing.expectEqual(ConnectionState.opened, conn.state);

    clock.advance(1);
    peer.clear();
    try testing.expectError(error.IdleTimeout, conn.doWork());
    try testing.expectEqual(ConnectionState.err, conn.state);

    // The peer is told why, in case it is still listening.
    const sent = try peer.onlyPerformative();
    try testing.expectEqualStrings("amqp:resource-limit-exceeded", sent.performative.close.err.?.condition);
}

test "a heartbeat from the peer postpones the idle timeout" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var clock = ManualClock{};
    var conn = Connection.init(allocator, "c", null, .{
        .idle_timeout_ms = 10_000,
        .clock = clock.clock(),
    });
    defer conn.deinit();
    try peer.openConnection(&conn);

    clock.advance(9_000);
    const empty_frame = (FrameHeader{
        .size = frame_mod.frame_header_size,
        .doff = 2,
        .frame_type = .amqp,
        .channel = 0,
    }).serialize();
    try conn.onBytesReceived(&empty_frame);

    clock.advance(10_000);
    try conn.doWork();
    try testing.expectEqual(ConnectionState.opened, conn.state);

    clock.advance(1);
    try testing.expectError(error.IdleTimeout, conn.doWork());
}

test "a clock that steps backwards costs a heartbeat, not a panic" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var clock = ManualClock{ .ms = 5_000 };
    var conn = Connection.init(allocator, "c", null, .{
        .idle_timeout_ms = 10_000,
        .clock = clock.clock(),
    });
    defer conn.deinit();
    try peer.openConnectionAdvertising(&conn, 10_000);

    // A wall clock adjusted backwards used to make the subtraction negative,
    // and the `@intCast` of that panicked.
    clock.advance(-4_000);
    try conn.doWork();
    try testing.expectEqual(ConnectionState.opened, conn.state);
    try testing.expectEqual(@as(usize, 0), peer.written().len);
}

test "idle handling needs a clock, and says so" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "c", null, .{ .idle_timeout_ms = 10_000 });
    defer conn.deinit();

    // Nothing is due before the connection is open, clock or not.
    try conn.doWork();

    try peer.openConnection(&conn);
    try testing.expectError(error.NoClockConfigured, conn.doWork());

    // And once one is supplied, both intervals start from now.
    var clock = ManualClock{};
    conn.setClock(clock.clock());
    try conn.doWork();
    clock.advance(10_001);
    try testing.expectError(error.IdleTimeout, conn.doWork());
}

test "a connection with no idle timeouts at either end needs no clock" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "c", null, .{});
    defer conn.deinit();
    try peer.openConnection(&conn);

    try conn.doWork();
    try testing.expectEqual(@as(usize, 0), peer.written().len);
}
