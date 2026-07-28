///! Sending messages over an attached sender link.
///!
///! Replaces message_sender.c — the layer between "a message" and "a link":
///! it serializes the message, waits for credit rather than failing without
///! it, and turns the peer's disposition into a per-send completion.
const std = @import("std");
const Allocator = std.mem.Allocator;

const defs = @import("protocol/definitions.zig");
const encoder = @import("types/encoder.zig");
const link_mod = @import("protocol/link.zig");
const Link = link_mod.Link;
const LinkState = link_mod.LinkState;
const Message = @import("message.zig").Message;

const log = std.log.scoped(.amqp_message_sender);

pub const MessageSenderState = enum {
    idle,
    opening,
    open,
    closing,
    err,
};

/// How a send ended.
///
/// `accepted`, and a delivery the peer settled without saying anything about,
/// are the only outcomes counted as success; `released` means the peer
/// declined to take the message but did not fault it, which is a cancellation
/// rather than an error.
pub const MessageSendResult = enum {
    ok,
    err,
    timeout,
    cancelled,
};

pub const OnMessageSendComplete = *const fn (
    context: ?*anyopaque,
    result: MessageSendResult,
    delivery_state: ?defs.DeliveryState,
) void;

pub const OnMessageSenderStateChanged = *const fn (
    context: ?*anyopaque,
    new_state: MessageSenderState,
    previous_state: MessageSenderState,
) void;

pub const SendOptions = struct {
    /// Settled at the sender: no outcome is expected, so the send completes
    /// as soon as the bytes are handed to the transport.
    settled: bool = false,
    message_format: ?u32 = null,
    delivery_tag: ?[]const u8 = null,
    /// Milliseconds to wait for the peer to settle, measured from the call.
    /// Requires a clock on the connection, and `doWork` to be called.
    timeout_ms: ?u64 = null,
    on_complete: ?OnMessageSendComplete = null,
    context: ?*anyopaque = null,
};

pub const MessageSender = struct {
    allocator: Allocator,
    link: *Link,
    state: MessageSenderState,
    on_state_changed: ?OnMessageSenderStateChanged = null,
    on_state_changed_context: ?*anyopaque = null,
    pending: std.ArrayList(Pending),
    next_send_id: u64,
    /// Set when a flush driven by a callback failed; see `takePendingError`.
    pending_error: ?anyerror = null,

    /// One send in flight, from the call until it is settled, timed out or
    /// cancelled. Sends stay in order: a queued one is not overtaken.
    const Pending = struct {
        send_id: u64,
        /// The encoded message, owned until it has been handed to the link.
        payload: ?[]u8,
        settled: bool,
        message_format: ?u32,
        tag: ?[]u8,
        deadline_ms: ?i64,
        delivery_id: ?u32 = null,
        on_complete: ?OnMessageSendComplete,
        context: ?*anyopaque,
    };

    /// The link is borrowed, not owned: a sender neither attaches nor
    /// destroys it, so several senders may share one link's lifetime and the
    /// caller keeps the choice of source, target and settle modes.
    pub fn init(allocator: Allocator, sender_link: *Link) MessageSender {
        return .{
            .allocator = allocator,
            .link = sender_link,
            .state = .idle,
            .pending = .empty,
            .next_send_id = 1,
        };
    }

    pub fn deinit(self: *MessageSender) void {
        for (self.pending.items) |*p| self.releasePending(p);
        self.pending.deinit(self.allocator);
    }

    pub fn setOnStateChanged(
        self: *MessageSender,
        cb: OnMessageSenderStateChanged,
        context: ?*anyopaque,
    ) void {
        self.on_state_changed = cb;
        self.on_state_changed_context = context;
    }

    /// Take over the link's sender-side callbacks and attach it if it is not
    /// attached already. Registration happens here rather than in `init`
    /// because `init` returns by value, so nothing there can take its own
    /// address; the sender must not be moved after this call.
    pub fn open(self: *MessageSender) !void {
        if (self.state != .idle) return error.InvalidState;
        if (self.link.role != .sender) return error.NotASender;

        self.link.on_state_changed = onLinkStateChanged;
        self.link.on_state_changed_context = self;
        self.link.on_flow_on = onFlowOn;
        self.link.on_flow_on_context = self;

        self.setState(.opening);
        errdefer self.setState(.err);

        switch (self.link.state) {
            .attached => self.setState(.open),
            .detached => try self.link.attach(),
            else => {},
        }
    }

    /// Detach the link and fail everything still outstanding.
    pub fn close(self: *MessageSender) void {
        if (self.state == .idle or self.state == .closing) return;
        self.setState(.closing);

        self.link.detach(true, null) catch |err| {
            log.warn("Detaching the sender link failed: {s}", .{@errorName(err)});
        };

        self.completeAll(.cancelled);
        self.setState(.idle);
    }

    /// Queue a message for sending and return the id identifying it in a
    /// completion.
    ///
    /// The message is serialized here, so the caller may reuse or free it as
    /// soon as this returns. Having no credit is not a failure: the message
    /// waits, and the next Flow that grants credit sends it.
    pub fn send(self: *MessageSender, message: *const Message, opts: SendOptions) !u64 {
        if (self.state != .open) return error.InvalidState;

        var buf = encoder.Buffer.initDynamic(self.allocator);
        defer buf.deinit();
        try message.encode(&buf);
        return self.sendBytes(buf.written(), opts);
    }

    /// Send an already-encoded message body.
    pub fn sendBytes(self: *MessageSender, payload: []const u8, opts: SendOptions) !u64 {
        if (self.state != .open) return error.InvalidState;

        const owned = try self.allocator.dupe(u8, payload);
        errdefer self.allocator.free(owned);

        const tag: ?[]u8 = if (opts.delivery_tag) |t| try self.allocator.dupe(u8, t) else null;
        errdefer if (tag) |t| self.allocator.free(t);

        const send_id = self.next_send_id;
        try self.pending.append(self.allocator, .{
            .send_id = send_id,
            .payload = owned,
            .settled = opts.settled,
            .message_format = opts.message_format,
            .tag = tag,
            .deadline_ms = if (opts.timeout_ms) |ms| self.nowMs() + @as(i64, @intCast(ms)) else null,
            .on_complete = opts.on_complete,
            .context = opts.context,
        });
        self.next_send_id += 1;

        self.flushPending() catch |err| switch (err) {
            error.NoCredit, error.InvalidState => {},
            else => return err,
        };
        return send_id;
    }

    /// Expire sends whose timeout has passed, and retry anything still
    /// queued behind a failed flush. Call it from the same loop that drives
    /// `Connection.doWork`.
    pub fn doWork(self: *MessageSender) void {
        var i: usize = 0;
        while (i < self.pending.items.len) {
            const deadline = self.pending.items[i].deadline_ms orelse {
                i += 1;
                continue;
            };
            if (self.nowMs() < deadline) {
                i += 1;
                continue;
            }
            var expired = self.pending.orderedRemove(i);
            self.releasePending(&expired);
            if (expired.on_complete) |cb| cb(expired.context, .timeout, null);
        }

        // A flush that failed on something transient — an allocator that had
        // nothing to give, a write that did not go out — leaves the message
        // queued. Nothing else would ever pick it back up.
        if (self.state == .open and self.pending.items.len > 0) self.tryFlush();
    }

    /// The last error a flush failed with, cleared by reading it.
    ///
    /// Flushing is driven by callbacks that cannot fail — credit arriving, or
    /// `doWork` — so the error is parked here instead of returned. The
    /// message stays queued and is retried; this says why it is still there.
    pub fn takePendingError(self: *MessageSender) ?anyerror {
        defer self.pending_error = null;
        return self.pending_error;
    }

    /// How many sends have been accepted but not yet completed.
    pub fn pendingCount(self: *const MessageSender) usize {
        return self.pending.items.len;
    }

    // ── Internal ──────────────────────────────────────────────────────

    /// Send everything queued, oldest first, for as long as there is credit.
    fn flushPending(self: *MessageSender) !void {
        var i: usize = 0;
        while (i < self.pending.items.len) {
            const payload = self.pending.items[i].payload orelse {
                i += 1;
                continue;
            };
            if (!self.link.hasCredit()) return error.NoCredit;

            const p = &self.pending.items[i];
            const delivery_id = try self.link.send(payload, .{
                .delivery_tag = p.tag,
                .message_format = p.message_format,
                .settled = p.settled,
                .on_settled = onDeliverySettled,
                .context = self,
            });

            self.allocator.free(payload);
            p.payload = null;
            p.delivery_id = delivery_id;

            // A settled delivery is never reported on, so waiting for a
            // disposition that cannot come would hang the caller.
            if (p.settled) {
                var done = self.pending.orderedRemove(i);
                self.releasePending(&done);
                if (done.on_complete) |cb| cb(done.context, .ok, null);
                continue;
            }
            i += 1;
        }
    }

    fn onFlowOn(context: ?*anyopaque) void {
        const self: *MessageSender = @ptrCast(@alignCast(context.?));
        self.tryFlush();
    }

    /// Flush what is queued, parking anything that goes wrong. No credit and
    /// no link to send on are ordinary: the message keeps its place.
    fn tryFlush(self: *MessageSender) void {
        self.flushPending() catch |err| switch (err) {
            error.NoCredit, error.InvalidState => {},
            else => {
                log.warn("Sending a queued message failed: {s}", .{@errorName(err)});
                if (self.pending_error == null) self.pending_error = err;
            },
        };
    }

    fn onDeliverySettled(
        context: ?*anyopaque,
        delivery_id: u32,
        state: ?defs.DeliveryState,
        _: bool,
    ) void {
        const self: *MessageSender = @ptrCast(@alignCast(context.?));
        for (self.pending.items, 0..) |p, i| {
            if (p.delivery_id != delivery_id) continue;
            var done = self.pending.orderedRemove(i);
            self.releasePending(&done);
            if (done.on_complete) |cb| cb(done.context, resultFor(state), state);
            return;
        }
    }

    fn resultFor(state: ?defs.DeliveryState) MessageSendResult {
        const s = state orelse return .ok;
        return switch (s) {
            .accepted => .ok,
            .released => .cancelled,
            else => .err,
        };
    }

    fn onLinkStateChanged(context: ?*anyopaque, new_state: LinkState, _: LinkState) void {
        const self: *MessageSender = @ptrCast(@alignCast(context.?));
        switch (new_state) {
            .attached => if (self.state == .opening) self.setState(.open),
            .err => {
                self.setState(.err);
                self.completeAll(.err);
            },
            .detached => if (self.state == .open) {
                self.setState(.idle);
                self.completeAll(.cancelled);
            },
            else => {},
        }
    }

    fn completeAll(self: *MessageSender, result: MessageSendResult) void {
        while (self.pending.items.len > 0) {
            var p = self.pending.orderedRemove(0);
            self.releasePending(&p);
            if (p.on_complete) |cb| cb(p.context, result, null);
        }
    }

    fn releasePending(self: *MessageSender, p: *Pending) void {
        if (p.payload) |payload| self.allocator.free(payload);
        p.payload = null;
        if (p.tag) |t| self.allocator.free(t);
        p.tag = null;
    }

    fn nowMs(self: *MessageSender) i64 {
        return self.link.session.connection.nowMs();
    }

    fn setState(self: *MessageSender, new_state: MessageSenderState) void {
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

const Recorder = struct {
    result: ?MessageSendResult = null,
    state: ?defs.DeliveryState = null,
    calls: usize = 0,

    fn onComplete(context: ?*anyopaque, result: MessageSendResult, state: ?defs.DeliveryState) void {
        const self: *Recorder = @ptrCast(@alignCast(context.?));
        self.result = result;
        self.state = state;
        self.calls += 1;
    }
};

fn testMessage(allocator: Allocator, body: []const u8) !Message {
    var message = Message.init(allocator);
    errdefer message.deinit();
    try message.addBodyData(body);
    return message;
}

/// A sender link attached and, unless told otherwise, given credit.
fn openSender(fx: *Fixture, sender: *MessageSender, link: *Link, credit: u32) !void {
    link.* = try Link.init(fx.allocator, &fx.session, "sender-1", .sender, .{ .address = "src" }, .{ .address = "dst" });
    try fx.attach(link, 7);
    sender.* = MessageSender.init(fx.allocator, link);
    try sender.open();
    if (credit > 0) try fx.grant(link, credit);
    fx.peer.clear();
}

/// Tell the sender the peer settled a delivery.
fn settle(fx: *Fixture, delivery_id: u32, state: ?defs.DeliveryState) !void {
    try fx.conn.onBytesReceived(try fx.peer.frame(1, .{ .disposition = .{
        .role = .receiver,
        .first = delivery_id,
        .settled = true,
        .delivery_state = state,
    } }));
}

test "MessageSender holds a message until the peer grants credit" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link: Link = undefined;
    defer link.deinit();
    var sender: MessageSender = undefined;
    defer sender.deinit();
    try openSender(fx, &sender, &link, 0);
    try testing.expectEqual(MessageSenderState.open, sender.state);

    var message = try testMessage(allocator, "hello");
    defer message.deinit();

    // No credit yet, so the message waits rather than failing.
    _ = try sender.send(&message, .{});
    try testing.expectEqual(@as(usize, 0), (try fx.peer.performatives()).len);
    try testing.expectEqual(@as(usize, 1), sender.pendingCount());

    try fx.grant(&link, 5);
    const sent = try fx.peer.onlyPerformative();
    try testing.expect(sent.performative == .transfer);

    var decoded = try Message.decode(allocator, sent.payload);
    defer decoded.deinit();
    try testing.expectEqualStrings("hello", decoded.body_data_sections.items[0].bytes);
}

test "MessageSender completes a send when the peer accepts it" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link: Link = undefined;
    defer link.deinit();
    var sender: MessageSender = undefined;
    defer sender.deinit();
    try openSender(fx, &sender, &link, 5);

    var message = try testMessage(allocator, "payload");
    defer message.deinit();

    var recorder = Recorder{};
    _ = try sender.send(&message, .{ .on_complete = Recorder.onComplete, .context = &recorder });
    try testing.expectEqual(@as(usize, 0), recorder.calls);

    try settle(fx, 0, .accepted);

    try testing.expectEqual(@as(usize, 1), recorder.calls);
    try testing.expectEqual(MessageSendResult.ok, recorder.result.?);
    try testing.expectEqual(@as(usize, 0), sender.pendingCount());
}

test "MessageSender reports a rejected message as an error and a released one as cancelled" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link: Link = undefined;
    defer link.deinit();
    var sender: MessageSender = undefined;
    defer sender.deinit();
    try openSender(fx, &sender, &link, 5);

    var message = try testMessage(allocator, "payload");
    defer message.deinit();

    var rejected = Recorder{};
    _ = try sender.send(&message, .{ .on_complete = Recorder.onComplete, .context = &rejected });
    var released = Recorder{};
    _ = try sender.send(&message, .{ .on_complete = Recorder.onComplete, .context = &released });

    try settle(fx, 0, .{ .rejected = .{} });
    try settle(fx, 1, .released);

    try testing.expectEqual(MessageSendResult.err, rejected.result.?);
    try testing.expectEqual(MessageSendResult.cancelled, released.result.?);
}

test "MessageSender completes a pre-settled send without waiting for a disposition" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link: Link = undefined;
    defer link.deinit();
    var sender: MessageSender = undefined;
    defer sender.deinit();
    try openSender(fx, &sender, &link, 5);

    var message = try testMessage(allocator, "fire and forget");
    defer message.deinit();

    var recorder = Recorder{};
    _ = try sender.send(&message, .{
        .settled = true,
        .on_complete = Recorder.onComplete,
        .context = &recorder,
    });

    try testing.expectEqual(@as(usize, 1), recorder.calls);
    try testing.expectEqual(MessageSendResult.ok, recorder.result.?);
    try testing.expectEqual(@as(usize, 0), sender.pendingCount());
}

test "MessageSender times out a send the peer never settles" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var clock = test_peer.ManualClock{};
    fx.conn.setClock(clock.clock());

    var link: Link = undefined;
    defer link.deinit();
    var sender: MessageSender = undefined;
    defer sender.deinit();
    try openSender(fx, &sender, &link, 5);

    var message = try testMessage(allocator, "slow");
    defer message.deinit();

    var recorder = Recorder{};
    _ = try sender.send(&message, .{
        .timeout_ms = 1000,
        .on_complete = Recorder.onComplete,
        .context = &recorder,
    });

    clock.advance(999);
    sender.doWork();
    try testing.expectEqual(@as(usize, 0), recorder.calls);

    clock.advance(1);
    sender.doWork();
    try testing.expectEqual(MessageSendResult.timeout, recorder.result.?);
    try testing.expectEqual(@as(usize, 0), sender.pendingCount());

    // A disposition arriving after the timeout must not complete it twice.
    try settle(fx, 0, .accepted);
    try testing.expectEqual(@as(usize, 1), recorder.calls);
}

test "MessageSender cancels everything outstanding when it closes" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link: Link = undefined;
    defer link.deinit();
    var sender: MessageSender = undefined;
    defer sender.deinit();
    try openSender(fx, &sender, &link, 5);

    var message = try testMessage(allocator, "abandoned");
    defer message.deinit();

    var recorder = Recorder{};
    _ = try sender.send(&message, .{ .on_complete = Recorder.onComplete, .context = &recorder });

    sender.close();
    try testing.expectEqual(MessageSendResult.cancelled, recorder.result.?);
    try testing.expectEqual(MessageSenderState.idle, sender.state);
}

test "MessageSender refuses a receiver link" {
    const allocator = testing.allocator;
    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link = try Link.init(allocator, &fx.session, "receiver-1", .receiver, null, null);
    defer link.deinit();

    var sender = MessageSender.init(allocator, &link);
    defer sender.deinit();
    try testing.expectError(error.NotASender, sender.open());
}

test "a queued send that runs out of memory leaves nothing behind" {
    const Case = struct {
        fn once(allocator: std.mem.Allocator) !void {
            var fx = try Fixture.init(allocator);
            defer fx.deinit();

            // Built in place rather than through `openSender`, because a
            // deferred `deinit` on a link that never got initialised is a
            // crash rather than the leak report this is looking for.
            var link = try Link.init(allocator, &fx.session, "sender-1", .sender, .{ .address = "src" }, .{ .address = "dst" });
            defer link.deinit();
            try fx.attach(&link, 7);

            var sender = MessageSender.init(allocator, &link);
            defer sender.deinit();
            try sender.open();

            var message = try testMessage(allocator, "hello");
            defer message.deinit();

            // Queued first, so the copy the sender keeps is on the hook too.
            _ = try sender.send(&message, .{});
            try fx.grant(&link, 5);
            if (link.takePendingError()) |err| return err;
            if (sender.takePendingError()) |err| return err;
        }
    };

    try testing.checkAllAllocationFailures(testing.allocator, Case.once, .{});
}

/// An allocator that can be made to refuse the next request, for testing what
/// happens to a message queued behind a failure.
const FlakyAllocator = struct {
    backing: std.mem.Allocator,
    fail: bool = false,

    fn allocator(self: *FlakyAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = alloc,
            .resize = resize,
            .remap = remap,
            .free = free,
        } };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *FlakyAllocator = @ptrCast(@alignCast(ctx));
        if (self.fail) return null;
        return self.backing.rawAlloc(len, alignment, ra);
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) bool {
        const self: *FlakyAllocator = @ptrCast(@alignCast(ctx));
        if (self.fail) return false;
        return self.backing.rawResize(memory, alignment, new_len, ra);
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
        const self: *FlakyAllocator = @ptrCast(@alignCast(ctx));
        if (self.fail) return null;
        return self.backing.rawRemap(memory, alignment, new_len, ra);
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *FlakyAllocator = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(memory, alignment, ra);
    }
};

test "a message queued behind a failed flush is retried, not stranded" {
    var flaky = FlakyAllocator{ .backing = testing.allocator };
    const allocator = flaky.allocator();

    var fx = try Fixture.init(allocator);
    defer fx.deinit();

    var link = try Link.init(allocator, &fx.session, "sender-1", .sender, .{ .address = "src" }, .{ .address = "dst" });
    defer link.deinit();
    try fx.attach(&link, 7);

    var sender = MessageSender.init(allocator, &link);
    defer sender.deinit();
    try sender.open();

    var message = try testMessage(allocator, "hello");
    defer message.deinit();

    // Queued: no credit yet.
    _ = try sender.send(&message, .{});
    link.setLinkCredit(5);
    fx.peer.clear();

    flaky.fail = true;
    sender.doWork();
    flaky.fail = false;

    try testing.expectEqual(@as(?anyerror, error.OutOfMemory), sender.takePendingError());
    try testing.expectEqual(@as(usize, 1), sender.pendingCount());
    try testing.expectEqual(@as(usize, 0), (try fx.peer.performatives()).len);

    // The next turn of the loop picks it back up.
    sender.doWork();
    try testing.expectEqual(@as(?anyerror, null), sender.takePendingError());
    const sent = try fx.peer.onlyPerformative();
    try testing.expect(sent.performative == .transfer);

    var decoded = try Message.decode(allocator, sent.payload);
    defer decoded.deinit();
    try testing.expectEqualStrings("hello", decoded.body_data_sections.items[0].bytes);
}
