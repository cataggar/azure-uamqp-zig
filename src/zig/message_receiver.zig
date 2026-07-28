//! Receiving messages over an attached receiver link.
//!
//! Replaces message_receiver.c — grants credit, reassembles and decodes each
//! delivery into a `Message`, and settles it with whatever the handler
//! decided.
const std = @import("std");
const Allocator = std.mem.Allocator;

const defs = @import("protocol/definitions.zig");
const link_mod = @import("protocol/link.zig");
const Link = link_mod.Link;
const LinkState = link_mod.LinkState;
const Message = @import("message.zig").Message;

const log = std.log.scoped(.amqp_message_receiver);

pub const MessageReceiverState = enum {
    idle,
    opening,
    open,
    closing,
    err,
};

/// Called with each complete message.
///
/// The message does not outlive the call — it is decoded into memory released
/// as soon as the handler returns — so anything worth keeping must be copied
/// or cloned.
///
/// The returned state settles the delivery. Returning null leaves it
/// unsettled for a handler that decides later, which must then call
/// `settle`.
pub const OnMessageReceived = *const fn (
    context: ?*anyopaque,
    message: *const Message,
) ?defs.DeliveryState;

pub const OnMessageReceiverStateChanged = *const fn (
    context: ?*anyopaque,
    new_state: MessageReceiverState,
    previous_state: MessageReceiverState,
) void;

pub const Options = struct {
    /// Credit granted when the link attaches, and topped back up to as
    /// deliveries are consumed. Zero leaves credit to the caller.
    credit: u32 = 100,
};

pub const MessageReceiver = struct {
    allocator: Allocator,
    link: *Link,
    state: MessageReceiverState,
    options: Options,
    on_message: ?OnMessageReceived = null,
    on_message_context: ?*anyopaque = null,
    on_state_changed: ?OnMessageReceiverStateChanged = null,
    on_state_changed_context: ?*anyopaque = null,
    /// The delivery id of the message being handled, or the last one handled.
    last_delivery_id: ?u32 = null,

    /// The link is borrowed, not owned: the caller keeps the choice of
    /// source, target and settle modes, and destroys it.
    pub fn init(allocator: Allocator, receiver_link: *Link, options: Options) MessageReceiver {
        return .{
            .allocator = allocator,
            .link = receiver_link,
            .state = .idle,
            .options = options,
        };
    }

    pub fn deinit(self: *MessageReceiver) void {
        _ = self;
    }

    pub fn setOnStateChanged(
        self: *MessageReceiver,
        cb: OnMessageReceiverStateChanged,
        context: ?*anyopaque,
    ) void {
        self.on_state_changed = cb;
        self.on_state_changed_context = context;
    }

    /// Take over the link's receiver-side callbacks, attach it if it is not
    /// attached already, and grant the configured credit.
    ///
    /// Registration happens here rather than in `init` because `init` returns
    /// by value, so nothing there can take its own address; the receiver must
    /// not be moved after this call.
    pub fn open(
        self: *MessageReceiver,
        on_message: OnMessageReceived,
        context: ?*anyopaque,
    ) !void {
        if (self.state != .idle) return error.InvalidState;
        if (self.link.role != .receiver) return error.NotAReceiver;

        self.on_message = on_message;
        self.on_message_context = context;
        self.link.on_state_changed = onLinkStateChanged;
        self.link.on_state_changed_context = self;
        self.link.on_transfer_received = onTransfer;
        self.link.on_transfer_received_context = self;

        self.setState(.opening);
        errdefer self.setState(.err);

        switch (self.link.state) {
            .attached => try self.onAttached(),
            .detached => try self.link.attach(),
            else => {},
        }
    }

    pub fn close(self: *MessageReceiver) void {
        if (self.state == .idle or self.state == .closing) return;
        self.setState(.closing);

        self.link.detach(true, null) catch |err| {
            log.warn("Detaching the receiver link failed: {s}", .{@errorName(err)});
        };
        self.setState(.idle);
    }

    /// Settle a delivery the handler left unsettled.
    pub fn settle(self: *MessageReceiver, delivery_id: u32, state: defs.DeliveryState) !void {
        try self.link.disposition(delivery_id, state, true);
    }

    /// Grant the peer more credit than `Options.credit` allows for.
    pub fn grantCredit(self: *MessageReceiver, credit: u32) !void {
        try self.link.flow(credit, .{});
    }

    pub fn linkName(self: *const MessageReceiver) []const u8 {
        return self.link.name;
    }

    // ── Internal ──────────────────────────────────────────────────────

    fn onAttached(self: *MessageReceiver) !void {
        if (self.options.credit > 0) try self.link.flow(self.options.credit, .{});
        self.setState(.open);
    }

    fn onTransfer(
        context: ?*anyopaque,
        transfer: defs.Transfer,
        payload: []const u8,
    ) ?defs.DeliveryState {
        const self: *MessageReceiver = @ptrCast(@alignCast(context.?));
        self.last_delivery_id = transfer.delivery_id;

        var message = Message.decode(self.allocator, payload) catch |err| {
            log.warn("Decoding a received message failed: {s}", .{@errorName(err)});
            // A message we cannot read is the sender's problem to hear about,
            // not a reason to stall the link.
            return .{ .rejected = .{ .err = .{
                .condition = "amqp:decode-error",
                .description = @errorName(err),
            } } };
        };
        defer message.deinit();

        const handler = self.on_message orelse return null;
        const outcome = handler(self.on_message_context, &message);

        // Consuming a delivery spends a credit; without topping up, a
        // receiver goes quiet after `credit` messages.
        if (self.options.credit > 0 and self.link.link_credit < self.options.credit) {
            self.link.flow(self.options.credit, .{}) catch |err| {
                log.warn("Replenishing receiver credit failed: {s}", .{@errorName(err)});
            };
        }
        return outcome;
    }

    fn onLinkStateChanged(context: ?*anyopaque, new_state: LinkState, _: LinkState) void {
        const self: *MessageReceiver = @ptrCast(@alignCast(context.?));
        switch (new_state) {
            .attached => if (self.state == .opening) {
                self.onAttached() catch |err| {
                    log.warn("Opening the message receiver failed: {s}", .{@errorName(err)});
                    self.setState(.err);
                };
            },
            .err => self.setState(.err),
            .detached => if (self.state == .open) self.setState(.idle),
            else => {},
        }
    }

    fn setState(self: *MessageReceiver, new_state: MessageReceiverState) void {
        if (self.state == new_state) return;
        const prev = self.state;
        self.state = new_state;
        if (self.on_state_changed) |cb| cb(self.on_state_changed_context, new_state, prev);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;
const test_peer = @import("protocol/test_peer.zig");
const Fixture = test_peer.Fixture;
const encoder = @import("types/encoder.zig");

const Collector = struct {
    allocator: Allocator,
    bodies: std.ArrayList([]u8) = .empty,
    outcome: ?defs.DeliveryState = .accepted,

    fn deinit(self: *Collector) void {
        for (self.bodies.items) |b| self.allocator.free(b);
        self.bodies.deinit(self.allocator);
    }

    fn onMessage(context: ?*anyopaque, message: *const Message) ?defs.DeliveryState {
        const self: *Collector = @ptrCast(@alignCast(context.?));
        const body = self.allocator.dupe(u8, message.body_data_sections.items[0].bytes) catch return null;
        self.bodies.append(self.allocator, body) catch {
            self.allocator.free(body);
            return null;
        };
        return self.outcome;
    }
};

/// The bytes of a message with one data section.
fn encodedMessage(allocator: Allocator, body: []const u8, out: *encoder.Buffer) ![]const u8 {
    var message = Message.init(allocator);
    defer message.deinit();
    try message.addBodyData(body);
    try message.encode(out);
    return out.written();
}

/// Deliver one message to the receiver as the peer would.
fn deliver(fx: *Fixture, link: *Link, delivery_id: u32, body: []const u8) !void {
    var buf = encoder.Buffer.initDynamic(fx.allocator);
    defer buf.deinit();
    const payload = try encodedMessage(fx.allocator, body, &buf);

    var tag: [4]u8 = undefined;
    std.mem.writeInt(u32, &tag, delivery_id, .big);
    try fx.conn.onBytesReceived(try fx.peer.framePayload(1, .{ .transfer = .{
        .handle = link.endpoint().?.input_handle.?,
        .delivery_id = delivery_id,
        .delivery_tag = &tag,
        .message_format = 0,
    } }, payload));
}

fn openReceiver(fx: *Fixture, receiver: *MessageReceiver, link: *Link, collector: *Collector, options: Options) !void {
    link.* = try Link.init(fx.allocator, &fx.session, "receiver-1", .receiver, .{ .address = "src" }, .{ .address = "dst" });
    try fx.attach(link, 7);
    receiver.* = MessageReceiver.init(fx.allocator, link, options);
    try receiver.open(Collector.onMessage, collector);
    fx.peer.clear();
}

test "MessageReceiver grants credit when the link attaches" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var collector = Collector{ .allocator = allocator };
    defer collector.deinit();

    var link = try Link.init(allocator, &fx.session, "receiver-1", .receiver, .{ .address = "src" }, .{ .address = "dst" });
    defer link.deinit();

    var receiver = MessageReceiver.init(allocator, &link, .{ .credit = 25 });
    defer receiver.deinit();
    try receiver.open(Collector.onMessage, &collector);
    try testing.expectEqual(MessageReceiverState.opening, receiver.state);

    try fx.respondAttach(&link, 7);
    try testing.expectEqual(MessageReceiverState.open, receiver.state);

    const flow = (try fx.peer.onlyPerformative()).performative.flow;
    try testing.expectEqual(@as(?u32, 25), flow.link_credit);
}

test "MessageReceiver decodes a delivery and settles it with the handler's outcome" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var collector = Collector{ .allocator = allocator };
    defer collector.deinit();
    var link: Link = undefined;
    defer link.deinit();
    var receiver: MessageReceiver = undefined;
    defer receiver.deinit();
    try openReceiver(fx, &receiver, &link, &collector, .{ .credit = 10 });

    try deliver(fx, &link, 3, "first");

    try testing.expectEqual(@as(usize, 1), collector.bodies.items.len);
    try testing.expectEqualStrings("first", collector.bodies.items[0]);
    try testing.expectEqual(@as(?u32, 3), receiver.last_delivery_id);

    const sent = try fx.peer.performatives();
    const disp = for (sent) |f| {
        if (f.performative == .disposition) break f.performative.disposition;
    } else return error.NoDisposition;
    try testing.expectEqual(@as(u32, 3), disp.first);
    try testing.expect(disp.delivery_state.? == .accepted);
}

test "MessageReceiver replenishes credit as deliveries are consumed" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var collector = Collector{ .allocator = allocator };
    defer collector.deinit();
    var link: Link = undefined;
    defer link.deinit();
    var receiver: MessageReceiver = undefined;
    defer receiver.deinit();
    try openReceiver(fx, &receiver, &link, &collector, .{ .credit = 2 });

    try deliver(fx, &link, 0, "one");
    try deliver(fx, &link, 1, "two");
    try deliver(fx, &link, 2, "three");

    // Without topping up, credit would have run out after two deliveries.
    try testing.expectEqual(@as(usize, 3), collector.bodies.items.len);
    try testing.expect(link.link_credit > 0);
}

test "MessageReceiver leaves a delivery unsettled when the handler returns null" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var collector = Collector{ .allocator = allocator, .outcome = null };
    defer collector.deinit();
    var link: Link = undefined;
    defer link.deinit();
    var receiver: MessageReceiver = undefined;
    defer receiver.deinit();
    try openReceiver(fx, &receiver, &link, &collector, .{ .credit = 10 });

    try deliver(fx, &link, 4, "later");
    for (try fx.peer.performatives()) |f| {
        try testing.expect(f.performative != .disposition);
    }

    // The handler settles it in its own time.
    fx.peer.clear();
    try receiver.settle(4, .accepted);
    const disp = (try fx.peer.onlyPerformative()).performative.disposition;
    try testing.expectEqual(@as(u32, 4), disp.first);
}

test "MessageReceiver rejects a delivery it cannot decode" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var collector = Collector{ .allocator = allocator };
    defer collector.deinit();
    var link: Link = undefined;
    defer link.deinit();
    var receiver: MessageReceiver = undefined;
    defer receiver.deinit();
    try openReceiver(fx, &receiver, &link, &collector, .{ .credit = 10 });

    var tag = [_]u8{ 0, 0, 0, 9 };
    try fx.conn.onBytesReceived(try fx.peer.framePayload(1, .{ .transfer = .{
        .handle = link.endpoint().?.input_handle.?,
        .delivery_id = 9,
        .delivery_tag = &tag,
        .message_format = 0,
    } }, &[_]u8{ 0xff, 0xff, 0xff }));

    try testing.expectEqual(@as(usize, 0), collector.bodies.items.len);
    const disp = for (try fx.peer.performatives()) |f| {
        if (f.performative == .disposition) break f.performative.disposition;
    } else return error.NoDisposition;
    try testing.expect(disp.delivery_state.? == .rejected);
}

test "MessageReceiver refuses a sender link" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var collector = Collector{ .allocator = allocator };
    defer collector.deinit();

    var link = try Link.init(allocator, &fx.session, "sender-1", .sender, null, null);
    defer link.deinit();

    var receiver = MessageReceiver.init(allocator, &link, .{});
    defer receiver.deinit();
    try testing.expectError(error.NotAReceiver, receiver.open(Collector.onMessage, &collector));
}
