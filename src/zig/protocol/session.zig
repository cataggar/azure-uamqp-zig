///! AMQP 1.0 Session state machine (OASIS spec §2.5)
///!
///! A session is a bidirectional, sequential conversation multiplexed onto one
///! channel of a connection. It carries the link endpoints, routes incoming
///! frames to them by handle, and owns the transfer windows that bound how
///! much either side may send before it is acknowledged.
const std = @import("std");
const Allocator = std.mem.Allocator;
const defs = @import("definitions.zig");
const Connection = @import("connection.zig").Connection;

const log = std.log.scoped(.amqp_session);

/// Session states per AMQP 1.0 §2.5.5
pub const SessionState = enum {
    unmapped,
    begin_sent,
    begin_rcvd,
    mapped,
    end_sent,
    end_rcvd,
    discarding,
    err,
};

pub const OnSessionStateChanged = *const fn (
    context: ?*anyopaque,
    new_state: SessionState,
    previous_state: SessionState,
) void;

pub const OnSessionFlowOn = *const fn (context: ?*anyopaque) void;

/// Neither `performative` nor `payload` outlives the call: the performative is
/// decoded into an arena released once the frame has been handled. Copy
/// anything worth keeping.
pub const OnLinkFrameReceived = *const fn (
    context: ?*anyopaque,
    performative: defs.Performative,
    payload: []const u8,
) void;

/// A link endpoint registered within a session.
///
/// `handle` is ours and appears on everything we send; `input_handle` is the
/// peer's for the same link, learned from its Attach, and is what incoming
/// frames are matched against. The two are independent (§2.6.2).
pub const LinkEndpoint = struct {
    name: []const u8,
    handle: u32,
    input_handle: ?u32 = null,
    on_frame_received: ?OnLinkFrameReceived = null,
    context: ?*anyopaque = null,
};

/// AMQP Session — manages link endpoints and flow control within a connection.
pub const Session = struct {
    allocator: Allocator,
    state: SessionState,
    connection: *Connection,

    // Flow control (§2.5.6). The `next_*_id` counters are the transfer ids we
    // will next assign and next expect; the windows bound how many transfers
    // may be in flight in each direction.
    next_outgoing_id: u32,
    next_incoming_id: u32,
    incoming_window: u32,
    outgoing_window: u32,
    remote_incoming_window: u32,
    remote_outgoing_window: u32,
    handle_max: u32,

    /// The window we advertised in the last Begin or Flow. The current
    /// `incoming_window` is drawn down by each transfer received, and a Flow
    /// restores it to this.
    initial_incoming_window: u32,

    // Link endpoints
    link_endpoints: std.ArrayList(LinkEndpoint),
    next_handle: u32,

    /// The connection endpoint this session is registered as, and the channels
    /// that endpoint carries. Null until `ensureRegistered` runs.
    endpoint_id: ?u32,

    // Callbacks
    on_state_changed: ?OnSessionStateChanged,
    on_state_changed_context: ?*anyopaque,

    /// Frame handling is driven by a callback that cannot fail, so an error is
    /// parked here and returned by the next call that can report it.
    pending_error: ?anyerror,

    pub fn init(allocator: Allocator, connection: *Connection, opts: struct {
        incoming_window: u32 = 2147483647,
        outgoing_window: u32 = 65536,
        handle_max: u32 = 4294967295,
    }) Session {
        return .{
            .allocator = allocator,
            .state = .unmapped,
            .connection = connection,
            .next_outgoing_id = 0,
            .next_incoming_id = 0,
            .incoming_window = opts.incoming_window,
            .outgoing_window = opts.outgoing_window,
            .remote_incoming_window = 0,
            .remote_outgoing_window = 0,
            .handle_max = opts.handle_max,
            .initial_incoming_window = opts.incoming_window,
            .link_endpoints = .empty,
            .next_handle = 0,
            .endpoint_id = null,
            .on_state_changed = null,
            .on_state_changed_context = null,
            .pending_error = null,
        };
    }

    pub fn deinit(self: *Session) void {
        // The connection holds a pointer to this session as its endpoint
        // context, so it has to be told before the session goes away.
        if (self.endpoint_id) |id| {
            self.connection.destroyEndpoint(id);
            self.endpoint_id = null;
        }
        self.link_endpoints.deinit(self.allocator);
    }

    /// Set the state change callback.
    pub fn setOnStateChanged(self: *Session, cb: OnSessionStateChanged, context: ?*anyopaque) void {
        self.on_state_changed = cb;
        self.on_state_changed_context = context;
    }

    /// The channel this session's frames are sent on, once it is registered.
    pub fn outgoingChannel(self: *Session) ?u16 {
        const id = self.endpoint_id orelse return null;
        return (self.connection.endpoint(id) orelse return null).outgoing_channel;
    }

    /// The channel the peer sends this session's frames on, once its Begin has
    /// arrived.
    pub fn incomingChannel(self: *Session) ?u16 {
        const id = self.endpoint_id orelse return null;
        return (self.connection.endpoint(id) orelse return null).incoming_channel;
    }

    /// Create a link endpoint within this session and return its handle.
    ///
    /// Endpoints are addressed by handle, never by a stored pointer: the
    /// backing array moves when it grows, and removing an endpoint shifts the
    /// ones after it. Look an endpoint up with `linkEndpoint` and treat that
    /// pointer as valid only until the next call that adds or removes one.
    pub fn createLinkEndpoint(self: *Session, name: []const u8) !u32 {
        const handle = self.next_handle;
        if (handle > self.handle_max) return error.HandleOutOfRange;
        try self.link_endpoints.append(self.allocator, .{
            .name = name,
            .handle = handle,
        });
        self.next_handle += 1;
        return handle;
    }

    /// Look up a link endpoint by handle. The pointer is valid only until the
    /// next call that adds or removes an endpoint.
    pub fn linkEndpoint(self: *Session, handle: u32) ?*LinkEndpoint {
        for (self.link_endpoints.items) |*ep| {
            if (ep.handle == handle) return ep;
        }
        return null;
    }

    /// Destroy a link endpoint. Does nothing if the handle is unknown, so
    /// destroying twice is safe.
    pub fn destroyLinkEndpoint(self: *Session, handle: u32) void {
        for (self.link_endpoints.items, 0..) |*ep, i| {
            if (ep.handle == handle) {
                // Order carries no meaning — endpoints are found by handle.
                _ = self.link_endpoints.swapRemove(i);
                return;
            }
        }
    }

    /// Initiate the session by sending Begin.
    pub fn begin(self: *Session) !void {
        if (self.state != .unmapped and self.state != .begin_rcvd) return error.InvalidState;
        try self.ensureRegistered();
        try self.sendBegin();
    }

    /// End the session, optionally reporting why.
    pub fn end(self: *Session, err: ?defs.AmqpError) !void {
        const next: SessionState = switch (self.state) {
            .mapped, .begin_sent => .end_sent,
            // The peer ended first, so this is the reply that unmaps it.
            .end_rcvd => .unmapped,
            else => return error.InvalidState,
        };
        try self.sendPerformative(.{ .end = .{ .err = err } }, &.{});
        self.setState(next);
    }

    /// Send a performative on this session's channel, with an optional payload.
    ///
    /// This is the send path links use. A Transfer additionally spends one of
    /// the peer's incoming-window slots, and is refused when that window is
    /// closed rather than overrunning it (§2.5.6).
    pub fn sendPerformative(self: *Session, performative: defs.Performative, payload: []const u8) !void {
        const channel = self.outgoingChannel() orelse return error.NotRegistered;
        // Session frames may not precede our own Open (§2.4.1). Sending them
        // between Open and the peer's answer is fine — that is pipelining, not
        // a violation.
        switch (self.connection.state) {
            .open_sent, .open_rcvd, .opened => {},
            else => return error.ConnectionNotOpen,
        }

        // Take the window before the frame goes out: if the send fails the id
        // is still spent, and reusing it would put two transfers on the wire
        // with the same id.
        const is_transfer = performative == .transfer;
        if (is_transfer) {
            if (self.remote_incoming_window == 0) return error.SessionWindowClosed;
            self.remote_incoming_window -= 1;
            self.next_outgoing_id +%= 1;
        }
        errdefer if (is_transfer) {
            self.remote_incoming_window += 1;
            self.next_outgoing_id -%= 1;
        };

        try self.connection.sendPerformative(channel, performative, payload);
    }

    /// Tell the peer where our windows stand, optionally carrying a link's
    /// credit state.
    pub fn sendFlow(self: *Session, link: ?LinkFlow) !void {
        // Restoring the window is the point of an unsolicited Flow.
        self.incoming_window = self.initial_incoming_window;
        try self.sendPerformative(.{ .flow = self.flowPerformative(link, false) }, &.{});
    }

    /// The link-scoped half of a Flow (§2.7.4).
    pub const LinkFlow = struct {
        handle: u32,
        delivery_count: ?u32 = null,
        link_credit: ?u32 = null,
        available: ?u32 = null,
        drain: bool = false,
        echo: bool = false,
    };

    /// Handle a received Begin performative.
    pub fn onBeginReceived(self: *Session, begin_perf: defs.Begin) void {
        self.remote_incoming_window = begin_perf.incoming_window;
        self.remote_outgoing_window = begin_perf.outgoing_window;
        self.next_incoming_id = begin_perf.next_outgoing_id;

        switch (self.state) {
            .begin_sent => self.setState(.mapped),
            .unmapped => self.setState(.begin_rcvd),
            else => {
                log.warn("Begin received in unexpected state: {s}", .{@tagName(self.state)});
                self.setState(.err);
            },
        }
    }

    /// Handle a received Flow performative.
    pub fn onFlowReceived(self: *Session, flow: defs.Flow) void {
        // How much room the peer has left for us: everything it has told us it
        // can accept, minus what we have already sent (§2.5.6).
        const next_incoming = flow.next_incoming_id orelse self.next_outgoing_id;
        self.remote_incoming_window = next_incoming +% flow.incoming_window -% self.next_outgoing_id;
        self.remote_outgoing_window = flow.outgoing_window;
        self.next_incoming_id = flow.next_outgoing_id;

        if (flow.handle) |handle| {
            self.routeByInputHandle(handle, .{ .flow = flow }, &.{});
        }
        if (flow.echo) {
            self.sendFlow(null) catch |err| self.park(err);
        }
    }

    // ── Internal ──────────────────────────────────────────────────────

    /// Register with the connection, which assigns the channel this session
    /// speaks on.
    ///
    /// This cannot happen in `init`: that returns by value, so the address it
    /// could capture is not the address the caller ends up holding.
    fn ensureRegistered(self: *Session) !void {
        if (self.endpoint_id != null) return;
        self.endpoint_id = try self.connection.createEndpoint(onConnectionFrame, self);
    }

    fn onConnectionFrame(
        context: ?*anyopaque,
        performative: defs.Performative,
        channel: u16,
        payload: []const u8,
    ) void {
        _ = channel;
        const self: *Session = @ptrCast(@alignCast(context.?));
        self.handleFrame(performative, payload) catch |err| {
            log.warn("Session frame handling failed: {s}", .{@errorName(err)});
            self.park(err);
        };
    }

    fn handleFrame(self: *Session, performative: defs.Performative, payload: []const u8) !void {
        switch (performative) {
            .begin => |begin_perf| {
                self.onBeginReceived(begin_perf);
                // A session the peer began still needs our Begin to map it.
                if (self.state == .begin_rcvd) try self.sendBegin();
            },
            .end => |end_perf| try self.onEndReceived(end_perf),
            .flow => |flow| self.onFlowReceived(flow),
            .attach => |attach| try self.onAttachReceived(attach, payload),
            .transfer => |transfer| try self.onTransferReceived(transfer, payload),
            .detach => |detach| self.onDetachReceived(detach, payload),
            // A disposition names deliveries, not links, so every link gets a
            // look at it and ignores the ids that are not its own (§2.7.5).
            .disposition => self.broadcast(performative, payload),
            else => log.warn("Unexpected performative on session: {s}", .{@tagName(performative)}),
        }
    }

    fn onEndReceived(self: *Session, end_perf: defs.End) !void {
        if (end_perf.err) |err| {
            log.warn("Peer ended session with {s}: {s}", .{ err.condition, err.description orelse "" });
        }
        switch (self.state) {
            // We asked to end and the peer agreed.
            .end_sent, .discarding => self.setState(.unmapped),
            else => {
                self.setState(.end_rcvd);
                // Answering is mandatory, and it is the last thing sent.
                try self.sendPerformative(.{ .end = .{} }, &.{});
                self.setState(.unmapped);
            },
        }
    }

    /// An Attach is what pairs the peer's handle with ours: it is matched by
    /// link name, which is the only thing both sides agree on beforehand
    /// (§2.6.1).
    fn onAttachReceived(self: *Session, attach: defs.Attach, payload: []const u8) !void {
        if (attach.handle > self.handle_max) return error.HandleOutOfRange;
        for (self.link_endpoints.items) |*ep| {
            if (std.mem.eql(u8, ep.name, attach.name)) {
                ep.input_handle = attach.handle;
                if (ep.on_frame_received) |cb| cb(ep.context, .{ .attach = attach }, payload);
                return;
            }
        }
        log.warn("Attach for unknown link: {s}", .{attach.name});
    }

    fn onTransferReceived(self: *Session, transfer: defs.Transfer, payload: []const u8) !void {
        // Receiving more than we said we could accept is a session error, not
        // something to absorb quietly (§2.5.6).
        if (self.incoming_window == 0) return error.SessionWindowExceeded;
        self.incoming_window -= 1;
        self.next_incoming_id +%= 1;

        self.routeByInputHandle(transfer.handle, .{ .transfer = transfer }, payload);

        // Replenish before the window runs out, so the peer never has to stop.
        if (self.incoming_window <= self.initial_incoming_window / 2) {
            try self.sendFlow(null);
        }
    }

    fn onDetachReceived(self: *Session, detach: defs.Detach, payload: []const u8) void {
        if (detach.err) |err| {
            log.warn("Peer detached link with {s}: {s}", .{ err.condition, err.description orelse "" });
        }
        self.routeByInputHandle(detach.handle, .{ .detach = detach }, payload);
        // The handle is free for the peer to reuse for a different link.
        for (self.link_endpoints.items) |*ep| {
            if (ep.input_handle == detach.handle) ep.input_handle = null;
        }
    }

    fn routeByInputHandle(
        self: *Session,
        input_handle: u32,
        performative: defs.Performative,
        payload: []const u8,
    ) void {
        for (self.link_endpoints.items) |*ep| {
            if (ep.input_handle == input_handle) {
                if (ep.on_frame_received) |cb| cb(ep.context, performative, payload);
                return;
            }
        }
        log.warn("{s} for unknown input handle: {d}", .{ @tagName(performative), input_handle });
    }

    fn broadcast(self: *Session, performative: defs.Performative, payload: []const u8) void {
        for (self.link_endpoints.items) |*ep| {
            if (ep.on_frame_received) |cb| cb(ep.context, performative, payload);
        }
    }

    fn sendBegin(self: *Session) !void {
        try self.ensureRegistered();
        self.incoming_window = self.initial_incoming_window;
        try self.sendPerformative(.{
            .begin = .{
                // Set only when answering: it names the peer's channel, which we
                // do not know until its Begin has arrived (§2.7.2).
                .remote_channel = self.incomingChannel(),
                .next_outgoing_id = self.next_outgoing_id,
                .incoming_window = self.incoming_window,
                .outgoing_window = self.outgoing_window,
                .handle_max = self.handle_max,
            },
        }, &.{});
        self.setState(if (self.state == .begin_rcvd) .mapped else .begin_sent);
    }

    fn flowPerformative(self: *Session, link: ?LinkFlow, echo: bool) defs.Flow {
        return .{
            .next_incoming_id = if (self.state == .unmapped) null else self.next_incoming_id,
            .incoming_window = self.incoming_window,
            .next_outgoing_id = self.next_outgoing_id,
            .outgoing_window = self.outgoing_window,
            .handle = if (link) |l| l.handle else null,
            .delivery_count = if (link) |l| l.delivery_count else null,
            .link_credit = if (link) |l| l.link_credit else null,
            .available = if (link) |l| l.available else null,
            .drain = if (link) |l| l.drain else false,
            .echo = if (link) |l| l.echo else echo,
        };
    }

    fn park(self: *Session, err: anyerror) void {
        if (self.pending_error == null) self.pending_error = err;
        self.setState(.err);
    }

    /// Return and clear the error a frame callback could not report.
    pub fn takePendingError(self: *Session) ?anyerror {
        defer self.pending_error = null;
        return self.pending_error;
    }

    fn setState(self: *Session, new_state: SessionState) void {
        if (self.state == new_state) return;
        const prev = self.state;
        self.state = new_state;
        log.info("Session state: {s} -> {s}", .{ @tagName(prev), @tagName(new_state) });
        if (self.on_state_changed) |cb| {
            cb(self.on_state_changed_context, new_state, prev);
        }
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

const testing = std.testing;
const TestPeer = @import("test_peer.zig").TestPeer;

/// Records what a link endpoint was handed.
const LinkSpy = struct {
    seen: std.ArrayList(defs.Performative) = .empty,
    payloads: std.ArrayList([]const u8) = .empty,
    allocator: Allocator,

    fn init(allocator: Allocator) LinkSpy {
        return .{ .allocator = allocator };
    }

    fn deinit(self: *LinkSpy) void {
        self.seen.deinit(self.allocator);
        self.payloads.deinit(self.allocator);
    }

    fn onFrame(context: ?*anyopaque, performative: defs.Performative, payload: []const u8) void {
        const self: *LinkSpy = @ptrCast(@alignCast(context.?));
        self.seen.append(self.allocator, performative) catch unreachable;
        self.payloads.append(self.allocator, payload) catch unreachable;
    }

    fn attach(self: *LinkSpy, session: *Session, handle: u32) void {
        const ep = session.linkEndpoint(handle).?;
        ep.on_frame_received = onFrame;
        ep.context = self;
    }
};

test "Session init" {
    const allocator = testing.allocator;
    var conn = Connection.init(allocator, "test", null, .{});
    defer conn.deinit();

    var session = Session.init(allocator, &conn, .{});
    defer session.deinit();

    try testing.expectEqual(SessionState.unmapped, session.state);
    try testing.expectEqual(@as(u32, 2147483647), session.incoming_window);
}

test "Session create link endpoint" {
    const allocator = testing.allocator;
    var conn = Connection.init(allocator, "test", null, .{});
    defer conn.deinit();

    var session = Session.init(allocator, &conn, .{});
    defer session.deinit();

    const h1 = try session.createLinkEndpoint("my-link");
    const h2 = try session.createLinkEndpoint("my-link-2");
    try testing.expectEqual(@as(u32, 0), h1);
    try testing.expectEqual(@as(u32, 1), h2);

    // Read the first endpoint only after the second exists: the old API
    // returned `&items[len - 1]`, which the append above could have moved.
    try testing.expectEqualStrings("my-link", session.linkEndpoint(h1).?.name);
    try testing.expectEqualStrings("my-link-2", session.linkEndpoint(h2).?.name);
}

test "link endpoints survive the session's storage growing" {
    const allocator = testing.allocator;
    var conn = Connection.init(allocator, "test", null, .{});
    defer conn.deinit();

    var session = Session.init(allocator, &conn, .{});
    defer session.deinit();

    var names: [64][8]u8 = undefined;
    var handles: [64]u32 = undefined;
    for (&names, 0..) |*name, i| {
        name.* = ("link-" ++ "\x00" ** 3).*;
        _ = std.fmt.bufPrint(name, "link-{d:0>2}", .{i}) catch unreachable;
        handles[i] = try session.createLinkEndpoint(name);
    }

    // Every handle still names its own endpoint, however many reallocations
    // the appends went through.
    for (handles, 0..) |handle, i| {
        const ep = session.linkEndpoint(handle).?;
        try testing.expectEqual(@as(u32, @intCast(i)), ep.handle);
        try testing.expectEqualStrings(&names[i], ep.name);
    }
}

test "destroying a link endpoint leaves the others addressable" {
    const allocator = testing.allocator;
    var conn = Connection.init(allocator, "test", null, .{});
    defer conn.deinit();

    var session = Session.init(allocator, &conn, .{});
    defer session.deinit();

    const first = try session.createLinkEndpoint("first");
    const middle = try session.createLinkEndpoint("middle");
    const last = try session.createLinkEndpoint("last");

    session.destroyLinkEndpoint(middle);

    // The removal used to shift the array, so `last` named `middle`'s slot.
    try testing.expect(session.linkEndpoint(middle) == null);
    try testing.expectEqualStrings("first", session.linkEndpoint(first).?.name);
    try testing.expectEqualStrings("last", session.linkEndpoint(last).?.name);
    try testing.expectEqual(@as(usize, 2), session.link_endpoints.items.len);

    // Destroying an unknown handle is a no-op, so a double destroy is safe.
    session.destroyLinkEndpoint(middle);
    try testing.expectEqual(@as(usize, 2), session.link_endpoints.items.len);

    // Handles are never reused, so a stale one cannot address a new endpoint.
    const fresh = try session.createLinkEndpoint("fresh");
    try testing.expectEqual(@as(u32, 3), fresh);
    try testing.expect(session.linkEndpoint(middle) == null);
}

test "begin registers with the connection and puts Begin on the wire" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "container", null, .{});
    defer conn.deinit();
    try peer.openConnection(&conn);

    var session = Session.init(allocator, &conn, .{ .incoming_window = 100, .outgoing_window = 50 });
    defer session.deinit();

    // The session was never registered before: `createEndpoint` had no caller.
    try testing.expect(session.endpoint_id == null);
    try session.begin();
    try testing.expect(session.endpoint_id != null);
    try testing.expectEqual(SessionState.begin_sent, session.state);
    try testing.expectEqual(@as(?u16, 0), session.outgoingChannel());

    const frame = try peer.onlyPerformative();
    try testing.expectEqual(@as(u16, 0), frame.channel);
    const begin_perf = frame.performative.begin;
    // Nothing to echo yet: the peer's channel is unknown until its Begin.
    try testing.expectEqual(@as(?u16, null), begin_perf.remote_channel);
    try testing.expectEqual(@as(u32, 0), begin_perf.next_outgoing_id);
    try testing.expectEqual(@as(u32, 100), begin_perf.incoming_window);
    try testing.expectEqual(@as(u32, 50), begin_perf.outgoing_window);
}

test "the peer's Begin maps the session and records its windows" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "container", null, .{});
    defer conn.deinit();
    try peer.openConnection(&conn);

    var session = Session.init(allocator, &conn, .{});
    defer session.deinit();
    try session.begin();
    peer.clear();

    try conn.onBytesReceived(try peer.frame(3, .{ .begin = .{
        .remote_channel = 0,
        .next_outgoing_id = 7,
        .incoming_window = 20,
        .outgoing_window = 30,
    } }));

    try testing.expectEqual(SessionState.mapped, session.state);
    try testing.expectEqual(@as(?u16, 3), session.incomingChannel());
    try testing.expectEqual(@as(u32, 20), session.remote_incoming_window);
    try testing.expectEqual(@as(u32, 30), session.remote_outgoing_window);
    try testing.expectEqual(@as(u32, 7), session.next_incoming_id);
    // Answering is the peer's job here; we began.
    try testing.expectEqual(@as(usize, 0), peer.written().len);
}

test "a session the peer begins answers with its own Begin" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "container", null, .{});
    defer conn.deinit();
    try peer.openConnection(&conn);

    var session = Session.init(allocator, &conn, .{});
    defer session.deinit();
    // Registered, but no Begin sent: the peer opens the session.
    try session.ensureRegistered();

    try conn.onBytesReceived(try peer.frame(5, .{ .begin = .{
        .next_outgoing_id = 0,
        .incoming_window = 10,
        .outgoing_window = 10,
    } }));

    try testing.expectEqual(SessionState.mapped, session.state);
    const frame = try peer.onlyPerformative();
    // The answer names the channel the peer's Begin arrived on (§2.7.2).
    try testing.expectEqual(@as(?u16, 5), frame.performative.begin.remote_channel);
}

test "each session gets its own channel and frames route by it" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "container", null, .{});
    defer conn.deinit();
    try peer.openConnection(&conn);

    var first = Session.init(allocator, &conn, .{});
    defer first.deinit();
    var second = Session.init(allocator, &conn, .{});
    defer second.deinit();
    try first.begin();
    try second.begin();
    peer.clear();

    try testing.expectEqual(@as(?u16, 0), first.outgoingChannel());
    try testing.expectEqual(@as(?u16, 1), second.outgoingChannel());

    // Answered out of order, and each Begin still finds its own session
    // because it echoes the channel it is answering.
    try conn.onBytesReceived(try peer.frame(9, .{ .begin = .{
        .remote_channel = 1,
        .next_outgoing_id = 0,
        .incoming_window = 1,
        .outgoing_window = 1,
    } }));
    try conn.onBytesReceived(try peer.frame(8, .{ .begin = .{
        .remote_channel = 0,
        .next_outgoing_id = 0,
        .incoming_window = 2,
        .outgoing_window = 2,
    } }));

    try testing.expectEqual(@as(?u16, 8), first.incomingChannel());
    try testing.expectEqual(@as(?u16, 9), second.incomingChannel());
    try testing.expectEqual(@as(u32, 2), first.remote_incoming_window);
    try testing.expectEqual(@as(u32, 1), second.remote_incoming_window);
}

/// Bring a session to `mapped` with the peer on `peer_channel`, and leave
/// nothing of the handshake in `peer.written()`.
fn mapped(peer: *TestPeer, conn: *Connection, session: *Session, peer_channel: u16) !void {
    try session.begin();
    try conn.onBytesReceived(try peer.frame(peer_channel, .{ .begin = .{
        .remote_channel = session.outgoingChannel(),
        .next_outgoing_id = 0,
        .incoming_window = 8,
        .outgoing_window = 8,
    } }));
    peer.clear();
}

test "end is sent and answered from either side" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "container", null, .{});
    defer conn.deinit();
    try peer.openConnection(&conn);

    var session = Session.init(allocator, &conn, .{});
    defer session.deinit();
    try mapped(&peer, &conn, &session, 1);

    // We end: End goes out and the session waits for the answer.
    try session.end(.{ .condition = "amqp:internal-error", .description = "bye" });
    try testing.expectEqual(SessionState.end_sent, session.state);
    const sent = try peer.onlyPerformative();
    try testing.expectEqualStrings("amqp:internal-error", sent.performative.end.err.?.condition);
    peer.clear();

    try conn.onBytesReceived(try peer.frame(1, .{ .end = .{} }));
    try testing.expectEqual(SessionState.unmapped, session.state);
    try testing.expectEqual(@as(usize, 0), peer.written().len);

    // The peer ends first: answering is mandatory.
    var other = Session.init(allocator, &conn, .{});
    defer other.deinit();
    try mapped(&peer, &conn, &other, 2);
    try conn.onBytesReceived(try peer.frame(2, .{ .end = .{} }));
    try testing.expectEqual(SessionState.unmapped, other.state);
    const answer = try peer.onlyPerformative();
    try testing.expect(answer.performative == .end);
}

test "Attach pairs the peer's handle with ours and later frames route by it" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "container", null, .{});
    defer conn.deinit();
    try peer.openConnection(&conn);

    var session = Session.init(allocator, &conn, .{});
    defer session.deinit();
    try mapped(&peer, &conn, &session, 1);

    const ours = try session.createLinkEndpoint("link-a");
    var spy = LinkSpy.init(allocator);
    defer spy.deinit();
    spy.attach(&session, ours);

    // The peer's handle space is its own: 41 here, 0 for us.
    try conn.onBytesReceived(try peer.frame(1, .{ .attach = .{
        .name = "link-a",
        .handle = 41,
        .role = .sender,
    } }));
    try testing.expectEqual(@as(?u32, 41), session.linkEndpoint(ours).?.input_handle);
    try testing.expectEqual(@as(usize, 1), spy.seen.items.len);

    // A transfer on the peer's handle reaches the link it attached.
    try conn.onBytesReceived(try peer.frame(1, .{ .transfer = .{ .handle = 41 } }));
    try testing.expectEqual(@as(usize, 2), spy.seen.items.len);
    try testing.expect(spy.seen.items[1] == .transfer);

    // Our own handle is not the peer's, and must not be routed as if it were.
    try conn.onBytesReceived(try peer.frame(1, .{ .transfer = .{ .handle = 0 } }));
    try testing.expectEqual(@as(usize, 2), spy.seen.items.len);
}

test "a transfer's payload reaches the link" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "container", null, .{});
    defer conn.deinit();
    try peer.openConnection(&conn);

    var session = Session.init(allocator, &conn, .{});
    defer session.deinit();
    try mapped(&peer, &conn, &session, 1);

    const ours = try session.createLinkEndpoint("link-a");
    var spy = LinkSpy.init(allocator);
    defer spy.deinit();
    spy.attach(&session, ours);
    session.linkEndpoint(ours).?.input_handle = 0;

    try conn.onBytesReceived(try peer.framePayload(1, .{ .transfer = .{
        .handle = 0,
        .delivery_id = 3,
    } }, "hello-body"));

    try testing.expectEqual(@as(usize, 1), spy.seen.items.len);
    try testing.expectEqualStrings("hello-body", spy.payloads.items[0]);
}

test "receiving transfers draws the window down and a Flow restores it" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "container", null, .{});
    defer conn.deinit();
    try peer.openConnection(&conn);

    var session = Session.init(allocator, &conn, .{ .incoming_window = 4 });
    defer session.deinit();
    try mapped(&peer, &conn, &session, 1);

    const ours = try session.createLinkEndpoint("link-a");
    session.linkEndpoint(ours).?.input_handle = 0;

    // Half the window is spent without a word.
    try conn.onBytesReceived(try peer.frame(1, .{ .transfer = .{ .handle = 0 } }));
    try testing.expectEqual(@as(u32, 3), session.incoming_window);
    try testing.expectEqual(@as(u32, 1), session.next_incoming_id);
    try testing.expectEqual(@as(usize, 0), peer.written().len);

    // At half, the peer is told it has room again, before it runs out.
    try conn.onBytesReceived(try peer.frame(1, .{ .transfer = .{ .handle = 0 } }));
    try testing.expectEqual(@as(u32, 4), session.incoming_window);
    const flow = (try peer.onlyPerformative()).performative.flow;
    try testing.expectEqual(@as(u32, 4), flow.incoming_window);
    try testing.expectEqual(@as(?u32, 2), flow.next_incoming_id);
    try testing.expectEqual(@as(?u32, null), flow.handle);
}

test "a transfer beyond the advertised window is a session error" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "container", null, .{});
    defer conn.deinit();
    try peer.openConnection(&conn);

    var session = Session.init(allocator, &conn, .{ .incoming_window = 1 });
    defer session.deinit();
    try mapped(&peer, &conn, &session, 1);
    const ours = try session.createLinkEndpoint("link-a");
    session.linkEndpoint(ours).?.input_handle = 0;

    // The one slot is used, and refilled by the automatic Flow.
    try conn.onBytesReceived(try peer.frame(1, .{ .transfer = .{ .handle = 0 } }));
    try testing.expectEqual(@as(u32, 1), session.incoming_window);

    // Pretend the peer ignored the Flow and overran a closed window.
    session.incoming_window = 0;
    try conn.onBytesReceived(try peer.frame(1, .{ .transfer = .{ .handle = 0 } }));
    try testing.expectEqual(@as(?anyerror, error.SessionWindowExceeded), session.takePendingError());
    try testing.expectEqual(SessionState.err, session.state);
    try testing.expectEqual(@as(?anyerror, null), session.takePendingError());
}

test "sending a transfer spends the peer's window and stops when it closes" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "container", null, .{});
    defer conn.deinit();
    try peer.openConnection(&conn);

    var session = Session.init(allocator, &conn, .{});
    defer session.deinit();
    try session.begin();
    try conn.onBytesReceived(try peer.frame(1, .{ .begin = .{
        .remote_channel = 0,
        .next_outgoing_id = 0,
        .incoming_window = 2,
        .outgoing_window = 2,
    } }));
    peer.clear();

    try session.sendPerformative(.{ .transfer = .{ .handle = 0 } }, "one");
    try testing.expectEqual(@as(u32, 1), session.remote_incoming_window);
    try testing.expectEqual(@as(u32, 1), session.next_outgoing_id);

    try session.sendPerformative(.{ .transfer = .{ .handle = 0 } }, "two");
    try testing.expectEqual(@as(u32, 0), session.remote_incoming_window);

    // The third would overrun the peer, so it is refused rather than sent.
    try testing.expectError(error.SessionWindowClosed, session.sendPerformative(.{ .transfer = .{ .handle = 0 } }, "three"));
    try testing.expectEqual(@as(u32, 2), session.next_outgoing_id);
    try testing.expectEqual(@as(usize, 2), (try peer.performatives()).len);

    // A Flow reopens it: the peer will accept ids up to next_incoming_id +
    // incoming_window, and we have already sent 2.
    session.onFlowReceived(.{
        .next_incoming_id = 2,
        .incoming_window = 3,
        .next_outgoing_id = 0,
        .outgoing_window = 8,
    });
    try testing.expectEqual(@as(u32, 3), session.remote_incoming_window);
    try session.sendPerformative(.{ .transfer = .{ .handle = 0 } }, "three");
}

test "a Flow that asks for an echo gets one" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "container", null, .{});
    defer conn.deinit();
    try peer.openConnection(&conn);

    var session = Session.init(allocator, &conn, .{ .incoming_window = 6, .outgoing_window = 7 });
    defer session.deinit();
    try mapped(&peer, &conn, &session, 1);

    try conn.onBytesReceived(try peer.frame(1, .{ .flow = .{
        .next_incoming_id = 0,
        .incoming_window = 5,
        .next_outgoing_id = 4,
        .outgoing_window = 9,
        .echo = true,
    } }));

    try testing.expectEqual(@as(u32, 9), session.remote_outgoing_window);
    try testing.expectEqual(@as(u32, 4), session.next_incoming_id);
    const answer = (try peer.onlyPerformative()).performative.flow;
    try testing.expectEqual(@as(u32, 6), answer.incoming_window);
    try testing.expectEqual(@as(u32, 7), answer.outgoing_window);
    try testing.expectEqual(@as(?u32, 4), answer.next_incoming_id);
    // An echo answers the peer; it must not ask for another one back.
    try testing.expectEqual(false, answer.echo);
}

test "Flow dispatches to the link named by its handle" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "container", null, .{});
    defer conn.deinit();
    try peer.openConnection(&conn);

    var session = Session.init(allocator, &conn, .{});
    defer session.deinit();
    try mapped(&peer, &conn, &session, 1);

    _ = try session.createLinkEndpoint("first");
    const target = try session.createLinkEndpoint("target");
    _ = try session.createLinkEndpoint("third");
    session.linkEndpoint(target).?.input_handle = 12;

    var spy = LinkSpy.init(allocator);
    defer spy.deinit();
    spy.attach(&session, target);

    session.onFlowReceived(.{
        .incoming_window = 1,
        .next_outgoing_id = 0,
        .outgoing_window = 1,
        .handle = 12,
    });
    try testing.expectEqual(@as(usize, 1), spy.seen.items.len);
    try testing.expectEqual(@as(?u32, 12), spy.seen.items[0].flow.handle);

    // An unknown handle is dropped, not delivered to whatever sits at that
    // index.
    session.onFlowReceived(.{
        .incoming_window = 1,
        .next_outgoing_id = 0,
        .outgoing_window = 1,
        .handle = 99,
    });
    try testing.expectEqual(@as(usize, 1), spy.seen.items.len);
}

test "Detach frees the peer's handle for reuse" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "container", null, .{});
    defer conn.deinit();
    try peer.openConnection(&conn);

    var session = Session.init(allocator, &conn, .{});
    defer session.deinit();
    try mapped(&peer, &conn, &session, 1);

    const first = try session.createLinkEndpoint("first");
    const second = try session.createLinkEndpoint("second");
    var spy = LinkSpy.init(allocator);
    defer spy.deinit();
    spy.attach(&session, second);

    try conn.onBytesReceived(try peer.frame(1, .{ .attach = .{ .name = "first", .handle = 4, .role = .sender } }));
    try conn.onBytesReceived(try peer.frame(1, .{ .detach = .{ .handle = 4, .closed = true } }));
    try testing.expectEqual(@as(?u32, null), session.linkEndpoint(first).?.input_handle);

    // Handle 4 now means a different link, and goes to that one.
    try conn.onBytesReceived(try peer.frame(1, .{ .attach = .{ .name = "second", .handle = 4, .role = .receiver } }));
    try conn.onBytesReceived(try peer.frame(1, .{ .transfer = .{ .handle = 4 } }));
    try testing.expectEqual(@as(?u32, 4), session.linkEndpoint(second).?.input_handle);
    try testing.expectEqual(@as(usize, 2), spy.seen.items.len);
}

test "a Disposition is offered to every link" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "container", null, .{});
    defer conn.deinit();
    try peer.openConnection(&conn);

    var session = Session.init(allocator, &conn, .{});
    defer session.deinit();
    try mapped(&peer, &conn, &session, 1);

    const a = try session.createLinkEndpoint("a");
    const b = try session.createLinkEndpoint("b");
    var spy_a = LinkSpy.init(allocator);
    defer spy_a.deinit();
    var spy_b = LinkSpy.init(allocator);
    defer spy_b.deinit();
    spy_a.attach(&session, a);
    spy_b.attach(&session, b);

    // A disposition carries delivery ids, not a handle, so it cannot be
    // routed to one link (§2.7.6).
    try conn.onBytesReceived(try peer.frame(1, .{ .disposition = .{
        .role = .receiver,
        .first = 0,
        .last = 3,
        .settled = true,
    } }));
    try testing.expectEqual(@as(usize, 1), spy_a.seen.items.len);
    try testing.expectEqual(@as(usize, 1), spy_b.seen.items.len);
}

test "sending before the session is registered is refused" {
    const allocator = testing.allocator;
    var conn = Connection.init(allocator, "container", null, .{});
    defer conn.deinit();

    var session = Session.init(allocator, &conn, .{});
    defer session.deinit();

    try testing.expectError(error.NotRegistered, session.sendPerformative(.{ .flow = .{
        .incoming_window = 1,
        .next_outgoing_id = 0,
        .outgoing_window = 1,
    } }, &.{}));
    try testing.expectError(error.InvalidState, session.end(null));
}

test "a Begin may not precede the connection's Open" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "container", null, .{});
    defer conn.deinit();
    peer.attach(&conn);

    var session = Session.init(allocator, &conn, .{});
    defer session.deinit();

    // Only the protocol header has been sent.
    try conn.open();
    try testing.expectError(error.ConnectionNotOpen, session.begin());
    try testing.expectEqual(SessionState.unmapped, session.state);

    // Once our Open is out, a Begin may follow without waiting for the
    // peer's: pipelining is allowed.
    try conn.onBytesReceived(&@import("frame.zig").amqp_header);
    peer.clear();
    try session.begin();
    try testing.expectEqual(SessionState.begin_sent, session.state);
    try testing.expect((try peer.onlyPerformative()).performative == .begin);
}

test "a session deregisters from its connection when it goes away" {
    const allocator = testing.allocator;
    var peer = TestPeer.init(allocator);
    defer peer.deinit();
    var conn = Connection.init(allocator, "container", null, .{});
    defer conn.deinit();
    try peer.openConnection(&conn);

    {
        var session = Session.init(allocator, &conn, .{});
        defer session.deinit();
        try session.begin();
        try testing.expectEqual(@as(usize, 1), conn.endpoints.items.len);
    }
    // The connection held a pointer to the session as its endpoint context;
    // leaving it registered would dispatch into a dead stack frame.
    try testing.expectEqual(@as(usize, 0), conn.endpoints.items.len);

    // The freed channel is handed to the next session.
    var next = Session.init(allocator, &conn, .{});
    defer next.deinit();
    try next.begin();
    try testing.expectEqual(@as(?u16, 0), next.outgoingChannel());
}
