///! AMQP 1.0 Link state machine (OASIS spec §2.6)
///!
///! A link is one direction of message flow between two terminuses, running
///! inside a session. It owns the credit that lets a sender send, the
///! delivery-count that both ends use to agree on how much has moved, and the
///! deliveries that are in flight but not yet settled.
const std = @import("std");
const Allocator = std.mem.Allocator;
const defs = @import("definitions.zig");
const described = @import("described.zig");
const encoder = @import("../types/encoder.zig");
const frame_mod = @import("frame.zig");
const Session = @import("session.zig").Session;
const LinkEndpoint = @import("session.zig").LinkEndpoint;

const log = std.log.scoped(.amqp_link);

/// A delivery tag is at most 32 bytes (§3.2.6), so it is carried inline
/// rather than owned: a pending delivery outlives the call that sent it.
pub const max_delivery_tag_len = 32;

/// A `max-message-size` of zero means unset, the same as absent (§3.5.3), so
/// both spellings of "no limit" collapse to null before anything compares
/// against them.
fn sizeLimit(field: ?u64) ?u64 {
    const value = field orelse return null;
    return if (value == 0) null else value;
}

/// Link states per AMQP 1.0 §2.6.4
pub const LinkState = enum {
    detached,
    half_attached_attach_sent,
    half_attached_attach_received,
    attached,
    err,
};

pub const OnLinkStateChanged = *const fn (
    context: ?*anyopaque,
    new_state: LinkState,
    previous_state: LinkState,
) void;

/// Called with a complete message, however many transfer frames carried it.
///
/// Neither `transfer` nor `payload` outlives the call: the performative is
/// decoded into an arena that is released when the frame has been handled, and
/// the payload buffer is reused by the next delivery. Copy anything worth
/// keeping.
///
/// The returned state settles the delivery; returning null leaves it
/// unsettled, for a receiver that decides later.
pub const OnTransferReceived = *const fn (
    context: ?*anyopaque,
    transfer: defs.Transfer,
    payload: []const u8,
) ?defs.DeliveryState;

/// Called when a sender that had run out of credit is given some again, so
/// that work parked for want of credit can resume.
pub const OnLinkFlowOn = *const fn (context: ?*anyopaque) void;

pub const OnDeliverySettled = *const fn (
    context: ?*anyopaque,
    delivery_id: u32,
    state: ?defs.DeliveryState,
    settled: bool,
) void;

/// Tracks a delivery that has been sent but not yet settled.
pub const Delivery = struct {
    delivery_id: u32,
    tag_buf: [max_delivery_tag_len]u8 = undefined,
    tag_len: u8 = 0,
    settled: bool = false,
    on_settled: ?OnDeliverySettled = null,
    context: ?*anyopaque = null,

    pub fn tag(self: *const Delivery) []const u8 {
        return self.tag_buf[0..self.tag_len];
    }
};

/// AMQP Link — a unidirectional channel for message transfer.
pub const Link = struct {
    allocator: Allocator,
    state: LinkState,
    name: []const u8,
    role: defs.Role,
    session: *Session,
    /// Identifies this link's endpoint within the session. Not a pointer:
    /// the session's endpoint storage moves as endpoints come and go.
    handle: u32,

    // Settle modes
    snd_settle_mode: defs.SenderSettleMode,
    rcv_settle_mode: defs.ReceiverSettleMode,
    peer_snd_settle_mode: defs.SenderSettleMode,
    peer_rcv_settle_mode: defs.ReceiverSettleMode,

    // Source and target, as configured locally. The peer's terminus is not
    // kept: a performative is only valid for the length of the callback that
    // received it, so anything worth keeping has to be copied. The address is
    // the part that matters — a dynamic node only has one after the peer's
    // Attach names it.
    source: ?defs.Source,
    target: ?defs.Target,
    peer_source_address: ?[]u8,
    peer_target_address: ?[]u8,

    // Flow control (§2.6.7). `delivery_count` counts deliveries that have
    // moved; `link_credit` is how many more the receiver will accept.
    delivery_count: u32,
    link_credit: u32,
    available: u32,
    drain: bool,

    // Delivery tracking
    pending_deliveries: std.ArrayList(Delivery),
    next_delivery_tag: u32,

    /// A delivery arriving across several transfer frames is accumulated here
    /// until the frame that says there is no more (§2.6.14). The tag is copied
    /// out of the first frame's performative, which does not outlive it.
    incoming_payload: std.ArrayList(u8),
    incoming_transfer: ?defs.Transfer,
    incoming_tag_buf: [max_delivery_tag_len]u8,
    incoming_tag_len: u8,

    /// The largest message this endpoint will accept, advertised to the peer
    /// in our Attach and enforced on both the send and the receive side.
    ///
    /// Null — and, per §3.5.3, zero — mean no limit, which leaves the memory a
    /// single delivery may occupy up to the peer: a sender that never stops
    /// setting `more` is reassembled until the allocator gives out. Set this
    /// on any link fed by a peer you do not control.
    max_message_size: ?u64,
    peer_max_message_size: ?u64,

    /// Set once the session endpoint points at this link. `init` returns by
    /// value, so the address to register cannot be known there.
    subscribed: bool,

    /// Frame handling is driven by a callback that cannot fail, so an error is
    /// parked here and returned by the next call that can report it.
    pending_error: ?anyerror,

    // Callbacks
    on_state_changed: ?OnLinkStateChanged,
    on_state_changed_context: ?*anyopaque,
    on_transfer_received: ?OnTransferReceived,
    on_transfer_received_context: ?*anyopaque,
    on_flow_on: ?OnLinkFlowOn,
    on_flow_on_context: ?*anyopaque,

    /// The session is told where this link lives by `listen` or `attach`, so
    /// the link must not be moved after either has been called.
    pub fn init(
        allocator: Allocator,
        session: *Session,
        name: []const u8,
        role: defs.Role,
        source: ?defs.Source,
        target: ?defs.Target,
    ) !Link {
        const handle = try session.createLinkEndpoint(name);
        return .{
            .allocator = allocator,
            .state = .detached,
            .name = name,
            .role = role,
            .session = session,
            .handle = handle,
            .snd_settle_mode = .mixed,
            .rcv_settle_mode = .first,
            .peer_snd_settle_mode = .mixed,
            .peer_rcv_settle_mode = .first,
            .source = source,
            .target = target,
            .peer_source_address = null,
            .peer_target_address = null,
            .delivery_count = 0,
            .link_credit = 0,
            .available = 0,
            .drain = false,
            .pending_deliveries = .empty,
            .next_delivery_tag = 0,
            .incoming_payload = .empty,
            .incoming_transfer = null,
            .incoming_tag_buf = undefined,
            .incoming_tag_len = 0,
            .max_message_size = null,
            .peer_max_message_size = null,
            .subscribed = false,
            .pending_error = null,
            .on_state_changed = null,
            .on_state_changed_context = null,
            .on_transfer_received = null,
            .on_transfer_received_context = null,
            .on_flow_on = null,
            .on_flow_on_context = null,
        };
    }

    pub fn deinit(self: *Link) void {
        if (self.peer_source_address) |a| self.allocator.free(a);
        if (self.peer_target_address) |a| self.allocator.free(a);
        self.incoming_payload.deinit(self.allocator);
        self.pending_deliveries.deinit(self.allocator);
        self.session.destroyLinkEndpoint(self.handle);
    }

    /// This link's endpoint in the session, or null once it has been
    /// destroyed. The pointer is valid only until the session adds or removes
    /// an endpoint.
    pub fn endpoint(self: *Link) ?*LinkEndpoint {
        return self.session.linkEndpoint(self.handle);
    }

    /// Start receiving this link's frames without attaching it, for a link the
    /// peer is expected to attach first.
    pub fn listen(self: *Link) !void {
        const ep = self.endpoint() orelse return error.NoEndpoint;
        ep.on_frame_received = onSessionFrame;
        ep.context = self;
        self.subscribed = true;
    }

    /// Initiate link attachment by sending Attach.
    pub fn attach(self: *Link) !void {
        if (self.state != .detached and self.state != .half_attached_attach_received) {
            return error.InvalidState;
        }
        try self.listen();
        try self.sendAttach();
    }

    /// Detach the link, optionally closing it for good and saying why.
    pub fn detach(self: *Link, close: bool, err: ?defs.AmqpError) !void {
        switch (self.state) {
            .attached, .half_attached_attach_sent, .half_attached_attach_received => {},
            else => return error.InvalidState,
        }
        try self.session.sendPerformative(.{ .detach = .{
            .handle = self.handle,
            .closed = close,
            .err = err,
        } }, &.{});
        self.setState(.detached);
    }

    /// Grant the peer credit to send (receiver only) and tell it so.
    pub fn flow(self: *Link, credit: u32, opts: struct { drain: bool = false, echo: bool = false }) !void {
        if (self.role != .receiver) return error.NotAReceiver;
        self.link_credit = credit;
        self.drain = opts.drain;
        try self.session.sendFlow(.{
            .handle = self.handle,
            .delivery_count = self.delivery_count,
            .link_credit = credit,
            .drain = opts.drain,
            .echo = opts.echo,
        });
    }

    /// Set link credit without telling the peer. Prefer `flow`, which does.
    pub fn setLinkCredit(self: *Link, credit: u32) void {
        self.link_credit = credit;
    }

    pub const SendOptions = struct {
        /// Defaults to a counter, which is enough to make deliveries on this
        /// link distinguishable — all a tag has to do (§2.6.12).
        delivery_tag: ?[]const u8 = null,
        message_format: ?u32 = null,
        /// Settled at the sender: the receiver will not report an outcome.
        settled: bool = false,
        on_settled: ?OnDeliverySettled = null,
        context: ?*anyopaque = null,
    };

    /// Send one message, split across as many transfer frames as the peer's
    /// frame size requires, and return its delivery id.
    pub fn send(self: *Link, payload: []const u8, opts: SendOptions) !u32 {
        if (self.role != .sender) return error.NotASender;
        if (self.state != .attached) return error.InvalidState;
        if (self.link_credit == 0) return error.NoCredit;
        if (sizeLimit(self.max_message_size)) |limit| {
            if (payload.len > limit) return error.MessageTooLarge;
        }
        if (sizeLimit(self.peer_max_message_size)) |limit| {
            if (payload.len > limit) return error.MessageTooLarge;
        }

        var tag_buf: [max_delivery_tag_len]u8 = undefined;
        const tag = if (opts.delivery_tag) |t| blk: {
            if (t.len > max_delivery_tag_len) return error.DeliveryTagTooLong;
            break :blk t;
        } else blk: {
            std.mem.writeInt(u32, tag_buf[0..4], self.next_delivery_tag, .big);
            break :blk tag_buf[0..4];
        };

        // The delivery id is the session transfer id the first frame will be
        // sent with (§2.6.12), so it has to be read before anything is sent.
        const delivery_id = self.session.next_outgoing_id;

        var remaining = payload;
        var first = true;
        while (true) {
            var transfer: defs.Transfer = .{ .handle = self.handle };
            if (first) {
                transfer.delivery_id = delivery_id;
                transfer.delivery_tag = tag;
                transfer.message_format = opts.message_format orelse 0;
                transfer.settled = opts.settled;
            }

            // Measure with `more` set: the flag costs a byte when true and
            // nothing when false, so this bounds every frame of the delivery.
            transfer.more = true;
            const budget = try self.payloadBudget(transfer);

            if (remaining.len <= budget) {
                transfer.more = false;
                try self.session.sendPerformative(.{ .transfer = transfer }, remaining);
                break;
            }
            try self.session.sendPerformative(.{ .transfer = transfer }, remaining[0..budget]);
            remaining = remaining[budget..];
            first = false;
        }

        // A delivery costs one credit however many frames carried it.
        self.link_credit -= 1;
        self.delivery_count +%= 1;
        self.next_delivery_tag +%= 1;

        if (!opts.settled) {
            var delivery: Delivery = .{
                .delivery_id = delivery_id,
                .tag_len = @intCast(tag.len),
                .settled = false,
                .on_settled = opts.on_settled,
                .context = opts.context,
            };
            @memcpy(delivery.tag_buf[0..tag.len], tag);
            try self.pending_deliveries.append(self.allocator, delivery);
        }
        return delivery_id;
    }

    /// Settle a received delivery by telling the peer what became of it.
    pub fn disposition(self: *Link, delivery_id: u32, state: ?defs.DeliveryState, settled: bool) !void {
        try self.session.sendPerformative(.{ .disposition = .{
            .role = self.role,
            .first = delivery_id,
            .settled = settled,
            .delivery_state = state,
        } }, &.{});
    }

    /// Handle a received Attach performative from the peer.
    pub fn onAttachReceived(self: *Link, attach_perf: defs.Attach) void {
        self.peer_snd_settle_mode = attach_perf.snd_settle_mode;
        self.peer_rcv_settle_mode = attach_perf.rcv_settle_mode;
        self.peer_max_message_size = attach_perf.max_message_size;
        // The receiver learns where the sender is counting from (§2.6.7).
        if (self.role == .receiver) {
            if (attach_perf.initial_delivery_count) |count| self.delivery_count = count;
        }
        // A dynamic terminus has no address until the peer names one, so the
        // address is copied out: the performative does not outlive this call.
        if (attach_perf.source) |src| self.setPeerAddress(&self.peer_source_address, src.address);
        if (attach_perf.target) |tgt| self.setPeerAddress(&self.peer_target_address, tgt.address);

        switch (self.state) {
            .half_attached_attach_sent => self.setState(.attached),
            .detached => self.setState(.half_attached_attach_received),
            else => {
                log.warn("Attach received in unexpected state: {s}", .{@tagName(self.state)});
                self.setState(.err);
            },
        }
    }

    /// Handle a received Flow performative.
    pub fn onFlowReceived(self: *Link, flow_perf: defs.Flow) void {
        self.drain = flow_perf.drain;
        if (flow_perf.available) |available| self.available = available;

        if (self.role == .sender) {
            // Credit is not a number the receiver hands over: it is the gap
            // between where the receiver has counted to and where we have
            // (§2.6.7). Computing it any other way loses whatever crossed on
            // the wire while the Flow was in flight.
            if (flow_perf.link_credit) |credit| {
                const was_blocked = self.link_credit == 0;
                const their_count = flow_perf.delivery_count orelse 0;
                self.link_credit = their_count +% credit -% self.delivery_count;
                // A sender with nothing to send on has usually parked the
                // work; this is the only moment it can learn to resume.
                if (was_blocked and self.link_credit > 0) {
                    if (self.on_flow_on) |cb| cb(self.on_flow_on_context);
                }
            }
        } else {
            if (flow_perf.delivery_count) |count| self.delivery_count = count;
        }
    }

    /// Handle a received Transfer performative.
    ///
    /// Returns the delivery state the handler chose, if the delivery is
    /// complete and the handler settled it.
    pub fn onTransferReceived(self: *Link, transfer: defs.Transfer, payload: []const u8) !?defs.DeliveryState {
        if (self.role != .receiver) return error.NotAReceiver;
        if (transfer.aborted) {
            // The sender gave up on this delivery; everything of it is void.
            self.incoming_payload.clearRetainingCapacity();
            self.incoming_transfer = null;
            return null;
        }

        if (self.incoming_transfer == null) {
            if (self.link_credit == 0) return error.NoCreditForTransfer;
            self.link_credit -= 1;
            self.delivery_count +%= 1;
            self.incoming_transfer = transfer;
            self.incoming_tag_len = 0;
            if (transfer.delivery_tag) |tag| {
                if (tag.len > max_delivery_tag_len) return error.DeliveryTagTooLong;
                @memcpy(self.incoming_tag_buf[0..tag.len], tag);
                self.incoming_tag_len = @intCast(tag.len);
            }
            // The performative is borrowed; the tag now lives here instead.
            self.incoming_transfer.?.delivery_tag = null;
        }
        try self.appendIncoming(payload);

        if (transfer.more) return null;

        var complete = self.incoming_transfer.?;
        if (self.incoming_tag_len > 0) complete.delivery_tag = self.incoming_tag_buf[0..self.incoming_tag_len];
        defer {
            self.incoming_payload.clearRetainingCapacity();
            self.incoming_transfer = null;
        }

        const cb = self.on_transfer_received orelse return null;
        const state = cb(self.on_transfer_received_context, complete, self.incoming_payload.items);

        // Nothing to report for a delivery the sender already settled.
        if (state) |s| {
            if (!(complete.settled orelse false)) {
                try self.disposition(complete.delivery_id orelse 0, s, true);
            }
        }
        return state;
    }

    /// Accumulate one transfer frame's payload, refusing to grow the
    /// reassembly buffer past what this endpoint advertised it would accept.
    ///
    /// A peer that ignores `max-message-size` is a link error (§2.7.3), and
    /// the partial message is dropped rather than kept: holding the capacity
    /// of a delivery that was rejected for being too big is the same leak the
    /// limit exists to prevent.
    fn appendIncoming(self: *Link, payload: []const u8) !void {
        if (sizeLimit(self.max_message_size)) |limit| {
            const total = @as(u64, self.incoming_payload.items.len) + payload.len;
            if (total > limit) {
                self.incoming_payload.clearAndFree(self.allocator);
                self.incoming_transfer = null;
                self.detach(true, .{
                    .condition = "amqp:link:message-size-exceeded",
                    .description = "message exceeds the advertised max-message-size",
                }) catch |err| {
                    log.warn("detach after oversized message failed: {t}", .{err});
                };
                return error.MessageSizeExceeded;
            }
        }
        try self.incoming_payload.appendSlice(self.allocator, payload);
    }

    /// Handle a received Disposition covering a range of deliveries.
    pub fn onDispositionReceived(self: *Link, disp: defs.Disposition) void {
        // A disposition describes deliveries the *other* role moved, so one
        // sent by our own role says nothing about what we sent.
        if (disp.role == self.role) return;

        const last = disp.last orelse disp.first;
        var i: usize = 0;
        while (i < self.pending_deliveries.items.len) {
            const delivery = self.pending_deliveries.items[i];
            if (delivery.delivery_id >= disp.first and delivery.delivery_id <= last) {
                if (delivery.on_settled) |cb| {
                    cb(delivery.context, delivery.delivery_id, disp.delivery_state, disp.settled);
                }
                if (disp.settled) {
                    // Order carries no meaning — deliveries are found by id.
                    _ = self.pending_deliveries.swapRemove(i);
                    continue;
                }
                self.pending_deliveries.items[i].settled = false;
            }
            i += 1;
        }
    }

    /// Handle a received Detach performative.
    pub fn onDetachReceived(self: *Link, detach_perf: defs.Detach) !void {
        if (detach_perf.err) |err| {
            log.warn("Link '{s}' detached by peer: {s}: {s}", .{
                self.name,
                err.condition,
                err.description orelse "",
            });
        }
        const answer = self.state != .detached;
        self.setState(.detached);
        // Answering is mandatory unless we detached first (§2.6.4).
        if (answer) {
            try self.session.sendPerformative(.{ .detach = .{
                .handle = self.handle,
                .closed = detach_perf.closed,
            } }, &.{});
        }
    }

    /// Check if the link has credit available for sending.
    pub fn hasCredit(self: *const Link) bool {
        return self.link_credit > 0;
    }

    /// Return and clear the error a frame callback could not report.
    pub fn takePendingError(self: *Link) ?anyerror {
        defer self.pending_error = null;
        return self.pending_error;
    }

    // ── Internal ──────────────────────────────────────────────────────

    fn onSessionFrame(context: ?*anyopaque, performative: defs.Performative, payload: []const u8) void {
        const self: *Link = @ptrCast(@alignCast(context.?));
        self.handleFrame(performative, payload) catch |err| {
            log.warn("Link frame handling failed: {s}", .{@errorName(err)});
            if (self.pending_error == null) self.pending_error = err;
            self.setState(.err);
        };
    }

    fn handleFrame(self: *Link, performative: defs.Performative, payload: []const u8) !void {
        switch (performative) {
            .attach => |attach_perf| {
                self.onAttachReceived(attach_perf);
                // A link the peer attached still needs our Attach to pair.
                if (self.state == .half_attached_attach_received) try self.sendAttach();
            },
            .flow => |flow_perf| self.onFlowReceived(flow_perf),
            .transfer => |transfer| _ = try self.onTransferReceived(transfer, payload),
            .disposition => |disp| self.onDispositionReceived(disp),
            .detach => |detach_perf| try self.onDetachReceived(detach_perf),
            else => log.warn("Unexpected performative on link: {s}", .{@tagName(performative)}),
        }
    }

    fn sendAttach(self: *Link) !void {
        try self.session.sendPerformative(.{
            .attach = .{
                .name = self.name,
                .handle = self.handle,
                .role = self.role,
                .snd_settle_mode = self.snd_settle_mode,
                .rcv_settle_mode = self.rcv_settle_mode,
                .source = self.source,
                .target = self.target,
                // Mandatory for a sender, meaningless for a receiver (§2.7.3).
                .initial_delivery_count = if (self.role == .sender) self.delivery_count else null,
                .max_message_size = self.max_message_size,
            },
        }, &.{});
        self.setState(if (self.state == .half_attached_attach_received) .attached else .half_attached_attach_sent);
    }

    /// How much payload fits behind this performative in one frame.
    fn payloadBudget(self: *Link, transfer: defs.Transfer) !usize {
        var buf = encoder.Buffer.initDynamic(self.allocator);
        defer buf.deinit();
        try described.encodePerformative(self.allocator, .{ .transfer = transfer }, &buf);

        const overhead = frame_mod.frame_header_size + buf.written().len;
        const max_frame = self.session.connection.remote_max_frame_size;
        if (overhead >= max_frame) return error.FrameTooSmallForTransfer;
        return max_frame - overhead;
    }

    fn setPeerAddress(self: *Link, slot: *?[]u8, address: ?[]const u8) void {
        const copy = if (address) |a| self.allocator.dupe(u8, a) catch return else null;
        if (slot.*) |old| self.allocator.free(old);
        slot.* = copy;
    }

    fn setState(self: *Link, new_state: LinkState) void {
        if (self.state == new_state) return;
        const prev = self.state;
        self.state = new_state;
        log.debug("Link '{s}' state: {s} -> {s}", .{ self.name, @tagName(prev), @tagName(new_state) });
        if (self.on_state_changed) |cb| {
            cb(self.on_state_changed_context, new_state, prev);
        }
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

const testing = std.testing;
const Connection = @import("connection.zig").Connection;
const TestPeer = @import("test_peer.zig").TestPeer;
const Fixture = @import("test_peer.zig").Fixture;

test "Link init and state" {
    const allocator = testing.allocator;
    var conn = Connection.init(allocator, "test", null, .{});
    defer conn.deinit();
    var session = Session.init(allocator, &conn, .{});
    defer session.deinit();

    var link = try Link.init(
        allocator,
        &session,
        "my-sender",
        .sender,
        .{ .address = "queue1" },
        .{ .address = "queue1" },
    );
    defer link.deinit();

    try testing.expectEqual(LinkState.detached, link.state);
    try testing.expectEqual(defs.Role.sender, link.role);
    try testing.expectEqualStrings("my-sender", link.name);
}

test "a link finds its endpoint by handle, and drops it on deinit" {
    const allocator = testing.allocator;
    var conn = Connection.init(allocator, "test", null, .{});
    defer conn.deinit();
    var session = Session.init(allocator, &conn, .{});
    defer session.deinit();

    var first = try Link.init(allocator, &session, "first", .sender, null, null);
    var second = try Link.init(allocator, &session, "second", .sender, null, null);
    defer second.deinit();

    // `first` was created before `second` forced the endpoint array to grow.
    try testing.expectEqualStrings("first", first.endpoint().?.name);
    try testing.expectEqualStrings("second", second.endpoint().?.name);

    first.deinit();
    try testing.expect(first.endpoint() == null);
    try testing.expectEqualStrings("second", second.endpoint().?.name);
    try testing.expectEqual(@as(usize, 1), session.link_endpoints.items.len);
}

test "attaching puts an Attach on the wire and the answer completes it" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link = try Link.init(allocator, &fx.session, "sender-1", .sender, .{ .address = "src" }, .{ .address = "dst" });
    defer link.deinit();
    link.max_message_size = 1024;

    try link.attach();
    try testing.expectEqual(LinkState.half_attached_attach_sent, link.state);

    const sent = (try fx.peer.onlyPerformative()).performative.attach;
    try testing.expectEqualStrings("sender-1", sent.name);
    try testing.expectEqual(@as(u32, 0), sent.handle);
    try testing.expectEqual(defs.Role.sender, sent.role);
    try testing.expectEqualStrings("dst", sent.target.?.address.?);
    // A sender must say where it is counting from (§2.7.3).
    try testing.expectEqual(@as(?u32, 0), sent.initial_delivery_count);
    try testing.expectEqual(@as(?u64, 1024), sent.max_message_size);
    fx.peer.clear();

    try fx.conn.onBytesReceived(try fx.peer.frame(1, .{ .attach = .{
        .name = "sender-1",
        .handle = 7,
        .role = .receiver,
        .rcv_settle_mode = .second,
        .max_message_size = 512,
    } }));
    try testing.expectEqual(LinkState.attached, link.state);
    try testing.expectEqual(defs.ReceiverSettleMode.second, link.peer_rcv_settle_mode);
    try testing.expectEqual(@as(?u64, 512), link.peer_max_message_size);
    // Answering is the peer's job here; we attached.
    try testing.expectEqual(@as(usize, 0), fx.peer.written().len);
}

test "a link the peer attaches first is answered" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link = try Link.init(allocator, &fx.session, "receiver-1", .receiver, null, null);
    defer link.deinit();
    try link.listen();

    try fx.conn.onBytesReceived(try fx.peer.frame(1, .{ .attach = .{
        .name = "receiver-1",
        .handle = 3,
        .role = .sender,
        .initial_delivery_count = 5,
        .source = .{ .address = "peer-source" },
    } }));

    try testing.expectEqual(LinkState.attached, link.state);
    // The receiver starts counting where the sender says it is (§2.6.7).
    try testing.expectEqual(@as(u32, 5), link.delivery_count);
    try testing.expectEqualStrings("peer-source", link.peer_source_address.?);
    const answer = (try fx.peer.onlyPerformative()).performative.attach;
    try testing.expectEqual(defs.Role.receiver, answer.role);
    try testing.expectEqual(@as(?u32, null), answer.initial_delivery_count);
}

test "a sender may not send without credit" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link = try Link.init(allocator, &fx.session, "s", .sender, null, .{ .address = "q" });
    defer link.deinit();
    try fx.attach(&link, 1);

    try testing.expect(!link.hasCredit());
    try testing.expectError(error.NoCredit, link.send("hello", .{}));
    try testing.expectEqual(@as(usize, 0), fx.peer.written().len);

    try fx.grant(&link, 2);
    try testing.expectEqual(@as(u32, 2), link.link_credit);

    const id = try link.send("hello", .{});
    try testing.expectEqual(@as(u32, 0), id);
    try testing.expectEqual(@as(u32, 1), link.link_credit);
    try testing.expectEqual(@as(u32, 1), link.delivery_count);

    const frame = try fx.peer.onlyPerformative();
    const transfer = frame.performative.transfer;
    try testing.expectEqual(@as(u32, 0), transfer.handle);
    try testing.expectEqual(@as(?u32, 0), transfer.delivery_id);
    try testing.expectEqual(false, transfer.more);
    try testing.expectEqualStrings("hello", frame.payload);
    // Unsettled, so it stays pending until the peer says what became of it.
    try testing.expectEqual(@as(usize, 1), link.pending_deliveries.items.len);
    try testing.expectEqualStrings(&.{ 0, 0, 0, 0 }, link.pending_deliveries.items[0].tag());
}

test "credit is the gap between the peer's count and ours, not the number it sent" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link = try Link.init(allocator, &fx.session, "s", .sender, null, .{ .address = "q" });
    defer link.deinit();
    try fx.attach(&link, 1);
    try fx.grant(&link, 5);

    _ = try link.send("a", .{});
    _ = try link.send("b", .{});
    try testing.expectEqual(@as(u32, 3), link.link_credit);
    try testing.expectEqual(@as(u32, 2), link.delivery_count);
    fx.peer.clear();

    // The peer answers with a Flow it composed before those two arrived: it
    // still thinks the count is 0 and offers 5. Taking that at face value
    // would hand back credit for deliveries already in flight.
    link.onFlowReceived(.{
        .incoming_window = 1024,
        .next_outgoing_id = 0,
        .outgoing_window = 1024,
        .handle = 1,
        .delivery_count = 0,
        .link_credit = 5,
    });
    try testing.expectEqual(@as(u32, 3), link.link_credit);

    // Once it has caught up, the offer means what it says.
    link.onFlowReceived(.{
        .incoming_window = 1024,
        .next_outgoing_id = 0,
        .outgoing_window = 1024,
        .handle = 1,
        .delivery_count = 2,
        .link_credit = 5,
    });
    try testing.expectEqual(@as(u32, 5), link.link_credit);
}

test "a message larger than the frame size is split across transfers" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();
    // The smallest frame size the spec allows, so the split is forced.
    fx.conn.remote_max_frame_size = 512;

    var link = try Link.init(allocator, &fx.session, "s", .sender, null, .{ .address = "q" });
    defer link.deinit();
    try fx.attach(&link, 1);
    try fx.grant(&link, 1);

    var body: [1500]u8 = undefined;
    for (&body, 0..) |*b, i| b.* = @truncate(i);
    const id = try link.send(&body, .{ .delivery_tag = "tag-1" });

    const frames = try fx.peer.performatives();
    try testing.expect(frames.len >= 3);

    var reassembled: std.ArrayList(u8) = .empty;
    defer reassembled.deinit(allocator);
    for (frames, 0..) |f, i| {
        const transfer = f.performative.transfer;
        try testing.expectEqual(@as(u32, 0), transfer.handle);
        // No frame may exceed what the peer said it would read.
        try testing.expect(frame_mod.frame_header_size + f.payload.len <= 512);
        if (i == 0) {
            // Only the first frame carries the delivery's identity (§2.6.14).
            try testing.expectEqual(@as(?u32, id), transfer.delivery_id);
            try testing.expectEqualStrings("tag-1", transfer.delivery_tag.?);
        } else {
            try testing.expectEqual(@as(?u32, null), transfer.delivery_id);
            try testing.expectEqual(@as(?[]const u8, null), transfer.delivery_tag);
        }
        // Every frame but the last says there is more coming.
        try testing.expectEqual(i != frames.len - 1, transfer.more);
        try reassembled.appendSlice(allocator, f.payload);
    }
    try testing.expectEqualSlices(u8, &body, reassembled.items);

    // One delivery, whatever it took to carry it.
    try testing.expectEqual(@as(u32, 0), link.link_credit);
    try testing.expectEqual(@as(u32, 1), link.delivery_count);
    try testing.expectEqual(@as(usize, 1), link.pending_deliveries.items.len);
}

test "a frame too small to hold the performative is refused, not sent empty" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link = try Link.init(allocator, &fx.session, "s", .sender, null, .{ .address = "q" });
    defer link.deinit();
    try fx.attach(&link, 1);
    try fx.grant(&link, 1);
    fx.peer.clear();
    // Smaller than the transfer performative itself.
    fx.conn.remote_max_frame_size = 16;

    try testing.expectError(error.FrameTooSmallForTransfer, link.send("body", .{}));
}

test "a receiver reassembles a multi-frame delivery and settles it" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link = try Link.init(allocator, &fx.session, "r", .receiver, .{ .address = "q" }, null);
    defer link.deinit();
    try fx.attach(&link, 4);

    const Handler = struct {
        // The payload is only valid for the length of the call, so it is
        // copied rather than kept.
        var body_buf: [64]u8 = undefined;
        var body_len: usize = 0;
        var tag: [8]u8 = undefined;
        var tag_len: usize = 0;
        var calls: usize = 0;
        fn onTransfer(_: ?*anyopaque, transfer: defs.Transfer, payload: []const u8) ?defs.DeliveryState {
            calls += 1;
            @memcpy(body_buf[0..payload.len], payload);
            body_len = payload.len;
            const t = transfer.delivery_tag orelse "";
            @memcpy(tag[0..t.len], t);
            tag_len = t.len;
            return .accepted;
        }
    };
    Handler.calls = 0;
    link.on_transfer_received = Handler.onTransfer;

    try link.flow(3, .{});
    const granted = (try fx.peer.onlyPerformative()).performative.flow;
    try testing.expectEqual(@as(?u32, 3), granted.link_credit);
    // A Flow names the link by the sender's own handle, not the peer's.
    try testing.expectEqual(@as(?u32, 0), granted.handle);
    fx.peer.clear();

    try fx.conn.onBytesReceived(try fx.peer.framePayload(1, .{
        .transfer = .{ .handle = 4, .delivery_id = 11, .delivery_tag = "t", .more = true },
    }, "hello "));
    // Nothing is delivered until the sender says the message is complete.
    try testing.expectEqual(@as(usize, 0), Handler.calls);
    try testing.expectEqual(@as(u32, 2), link.link_credit);

    try fx.conn.onBytesReceived(try fx.peer.framePayload(1, .{
        .transfer = .{ .handle = 4 },
    }, "world"));
    try testing.expectEqual(@as(usize, 1), Handler.calls);
    try testing.expectEqualStrings("hello world", Handler.body_buf[0..Handler.body_len]);
    // The tag came from the first frame, whose performative is long gone.
    try testing.expectEqualStrings("t", Handler.tag[0..Handler.tag_len]);
    // A continuation frame is part of the same delivery, so it costs nothing.
    try testing.expectEqual(@as(u32, 2), link.link_credit);
    try testing.expectEqual(@as(u32, 1), link.delivery_count);

    const disp = (try fx.peer.onlyPerformative()).performative.disposition;
    try testing.expectEqual(defs.Role.receiver, disp.role);
    try testing.expectEqual(@as(u32, 11), disp.first);
    try testing.expect(disp.settled);
    try testing.expect(disp.delivery_state.? == .accepted);
}

test "an aborted delivery is discarded, not handed over half-read" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link = try Link.init(allocator, &fx.session, "r", .receiver, .{ .address = "q" }, null);
    defer link.deinit();
    try fx.attach(&link, 4);

    const Handler = struct {
        var calls: usize = 0;
        fn onTransfer(_: ?*anyopaque, _: defs.Transfer, _: []const u8) ?defs.DeliveryState {
            calls += 1;
            return .accepted;
        }
    };
    Handler.calls = 0;
    link.on_transfer_received = Handler.onTransfer;
    link.setLinkCredit(5);

    try fx.conn.onBytesReceived(try fx.peer.framePayload(1, .{
        .transfer = .{ .handle = 4, .delivery_id = 1, .more = true },
    }, "half a mes"));
    try fx.conn.onBytesReceived(try fx.peer.frame(1, .{
        .transfer = .{ .handle = 4, .aborted = true },
    }));
    try testing.expectEqual(@as(usize, 0), Handler.calls);
    try testing.expectEqual(@as(usize, 0), link.incoming_payload.items.len);

    // The next delivery starts clean rather than behind the abandoned bytes.
    try fx.conn.onBytesReceived(try fx.peer.framePayload(1, .{
        .transfer = .{ .handle = 4, .delivery_id = 2 },
    }, "whole"));
    try testing.expectEqual(@as(usize, 1), Handler.calls);
}

test "a transfer beyond the credit granted is a link error" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link = try Link.init(allocator, &fx.session, "r", .receiver, .{ .address = "q" }, null);
    defer link.deinit();
    try fx.attach(&link, 4);

    try fx.conn.onBytesReceived(try fx.peer.framePayload(1, .{
        .transfer = .{ .handle = 4, .delivery_id = 1 },
    }, "unasked for"));
    try testing.expectEqual(@as(?anyerror, error.NoCreditForTransfer), link.takePendingError());
    try testing.expectEqual(LinkState.err, link.state);
}

test "a message beyond the advertised max-message-size is refused, not buffered" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link = try Link.init(allocator, &fx.session, "r", .receiver, .{ .address = "q" }, null);
    defer link.deinit();
    link.max_message_size = 16;
    try fx.attach(&link, 4);

    const Handler = struct {
        var calls: usize = 0;
        fn onTransfer(_: ?*anyopaque, _: defs.Transfer, _: []const u8) ?defs.DeliveryState {
            calls += 1;
            return .accepted;
        }
    };
    Handler.calls = 0;
    link.on_transfer_received = Handler.onTransfer;
    link.setLinkCredit(5);
    fx.peer.clear();

    // Ten bytes at a time: under the limit on its own, over it together.
    try fx.conn.onBytesReceived(try fx.peer.framePayload(1, .{
        .transfer = .{ .handle = 4, .delivery_id = 1, .more = true },
    }, "0123456789"));
    try testing.expectEqual(@as(usize, 10), link.incoming_payload.items.len);

    try fx.conn.onBytesReceived(try fx.peer.framePayload(1, .{
        .transfer = .{ .handle = 4, .more = true },
    }, "0123456789"));

    try testing.expectEqual(@as(?anyerror, error.MessageSizeExceeded), link.takePendingError());
    try testing.expectEqual(LinkState.err, link.state);
    try testing.expectEqual(@as(usize, 0), Handler.calls);
    // The partial message is released, not held: keeping it is the leak the
    // limit is there to stop.
    try testing.expectEqual(@as(usize, 0), link.incoming_payload.items.len);
    try testing.expectEqual(@as(usize, 0), link.incoming_payload.capacity);

    const detach_perf = (try fx.peer.onlyPerformative()).performative.detach;
    try testing.expect(detach_perf.closed);
    try testing.expectEqualStrings("amqp:link:message-size-exceeded", detach_perf.err.?.condition);
}

test "a max-message-size of zero is no limit, not a limit of nothing" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link = try Link.init(allocator, &fx.session, "s", .sender, null, .{ .address = "q" });
    defer link.deinit();
    try fx.attach(&link, 4);
    try fx.grant(&link, 5);
    fx.peer.clear();

    // §3.5.3: zero means the field is not set.
    link.max_message_size = 0;
    link.peer_max_message_size = 0;
    _ = try link.send("a message longer than nothing", .{});
    try testing.expectEqualStrings(
        "a message longer than nothing",
        (try fx.peer.onlyPerformative()).payload,
    );
}

test "a Disposition settles the deliveries it covers and no others" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link = try Link.init(allocator, &fx.session, "s", .sender, null, .{ .address = "q" });
    defer link.deinit();
    try fx.attach(&link, 1);
    try fx.grant(&link, 10);

    const Settled = struct {
        var ids: std.ArrayList(u32) = .empty;
        var states: std.ArrayList(?defs.DeliveryState) = .empty;
        fn onSettled(context: ?*anyopaque, id: u32, state: ?defs.DeliveryState, _: bool) void {
            const alloc: *Allocator = @ptrCast(@alignCast(context.?));
            ids.append(alloc.*, id) catch unreachable;
            states.append(alloc.*, state) catch unreachable;
        }
    };
    var alloc_copy = allocator;
    Settled.ids = .empty;
    Settled.states = .empty;
    defer Settled.ids.deinit(allocator);
    defer Settled.states.deinit(allocator);

    for (0..4) |_| {
        _ = try link.send("m", .{ .on_settled = Settled.onSettled, .context = &alloc_copy });
    }
    try testing.expectEqual(@as(usize, 4), link.pending_deliveries.items.len);
    fx.peer.clear();

    // Deliveries 1 and 2 are accepted together; 0 and 3 are untouched.
    try fx.conn.onBytesReceived(try fx.peer.frame(1, .{ .disposition = .{
        .role = .receiver,
        .first = 1,
        .last = 2,
        .settled = true,
        .delivery_state = .accepted,
    } }));
    try testing.expectEqualSlices(u32, &.{ 1, 2 }, Settled.ids.items);
    try testing.expect(Settled.states.items[0].? == .accepted);
    try testing.expectEqual(@as(usize, 2), link.pending_deliveries.items.len);

    // A disposition from our own role describes deliveries we received, so it
    // says nothing about the ones we sent.
    try fx.conn.onBytesReceived(try fx.peer.frame(1, .{ .disposition = .{
        .role = .sender,
        .first = 0,
        .last = 3,
        .settled = true,
    } }));
    try testing.expectEqual(@as(usize, 2), link.pending_deliveries.items.len);
}

test "a settled send is not tracked and needs no disposition" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link = try Link.init(allocator, &fx.session, "s", .sender, null, .{ .address = "q" });
    defer link.deinit();
    try fx.attach(&link, 1);
    try fx.grant(&link, 1);

    _ = try link.send("fire and forget", .{ .settled = true });
    try testing.expectEqual(@as(usize, 0), link.pending_deliveries.items.len);
    const transfer = (try fx.peer.onlyPerformative()).performative.transfer;
    try testing.expectEqual(@as(?bool, true), transfer.settled);
}

test "detaching is a Detach each way" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link = try Link.init(allocator, &fx.session, "s", .sender, null, .{ .address = "q" });
    defer link.deinit();
    try fx.attach(&link, 1);

    try link.detach(true, .{ .condition = "amqp:link:detach-forced", .description = "done" });
    try testing.expectEqual(LinkState.detached, link.state);
    const sent = (try fx.peer.onlyPerformative()).performative.detach;
    try testing.expect(sent.closed);
    try testing.expectEqualStrings("amqp:link:detach-forced", sent.err.?.condition);
    fx.peer.clear();

    // We detached first, so the peer's answer needs no answer of its own.
    try fx.conn.onBytesReceived(try fx.peer.frame(1, .{ .detach = .{ .handle = 1, .closed = true } }));
    try testing.expectEqual(@as(usize, 0), fx.peer.written().len);

    // A peer that detaches an attached link is answered.
    var other = try Link.init(allocator, &fx.session, "s2", .sender, null, .{ .address = "q" });
    defer other.deinit();
    try fx.attach(&other, 2);
    try fx.conn.onBytesReceived(try fx.peer.frame(1, .{ .detach = .{ .handle = 2, .closed = false } }));
    try testing.expectEqual(LinkState.detached, other.state);
    const answer = (try fx.peer.onlyPerformative()).performative.detach;
    try testing.expectEqual(@as(u32, 1), answer.handle);
}

test "sends that cannot be represented are refused before anything is written" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link = try Link.init(allocator, &fx.session, "s", .sender, null, .{ .address = "q" });
    defer link.deinit();
    try fx.attach(&link, 1);
    try fx.grant(&link, 4);
    fx.peer.clear();

    try testing.expectError(error.DeliveryTagTooLong, link.send("m", .{ .delivery_tag = "x" ** 33 }));

    link.max_message_size = 4;
    try testing.expectError(error.MessageTooLarge, link.send("too long", .{}));
    link.max_message_size = null;
    link.peer_max_message_size = 4;
    try testing.expectError(error.MessageTooLarge, link.send("too long", .{}));

    try testing.expectEqual(@as(usize, 0), fx.peer.written().len);
    try testing.expectEqual(@as(u32, 4), link.link_credit);

    // A receiver is not a sender, and vice versa.
    try testing.expectError(error.NotAReceiver, link.flow(1, .{}));
}

test "a receive that runs out of memory leaves nothing behind" {
    const Case = struct {
        fn accept(_: ?*anyopaque, _: defs.Transfer, _: []const u8) ?defs.DeliveryState {
            return .accepted;
        }

        // Frame handling is driven by callbacks that cannot fail, so an
        // induced OOM is parked rather than returned. Hand it back, or the
        // failure looks like the allocator being ignored.
        fn surface(link: *Link) !void {
            if (link.takePendingError()) |err| return err;
        }

        fn receive(allocator: std.mem.Allocator) !void {
            var fx = try Fixture.init(allocator);
            defer fx.deinit();

            var link = try Link.init(allocator, &fx.session, "r", .receiver, .{ .address = "q" }, null);
            defer link.deinit();
            try fx.attach(&link, 4);
            link.on_transfer_received = accept;
            link.setLinkCredit(5);

            try fx.conn.onBytesReceived(try fx.peer.framePayload(1, .{
                .transfer = .{ .handle = 4, .delivery_id = 1, .delivery_tag = "t", .more = true },
            }, "hello "));
            try surface(&link);
            try fx.conn.onBytesReceived(try fx.peer.framePayload(1, .{
                .transfer = .{ .handle = 4 },
            }, "world"));
            try surface(&link);
        }

        fn send(allocator: std.mem.Allocator) !void {
            var fx = try Fixture.init(allocator);
            defer fx.deinit();

            var link = try Link.init(allocator, &fx.session, "s", .sender, null, .{ .address = "q" });
            defer link.deinit();
            try fx.attach(&link, 4);
            try fx.grant(&link, 5);
            _ = try link.send("a message worth tracking", .{});
            try surface(&link);
        }
    };

    try testing.checkAllAllocationFailures(testing.allocator, Case.receive, .{});
    try testing.checkAllAllocationFailures(testing.allocator, Case.send, .{});
}
