//! AMQP Management operations (OASIS AMQP Management spec §3.1)
//!
//! A request-response pair of links to a management node: requests go out on
//! the sender, and each response is matched to its request by correlation-id.
const std = @import("std");
const Allocator = std.mem.Allocator;
const defs = @import("protocol/definitions.zig");
const Session = @import("protocol/session.zig").Session;
const link_mod = @import("protocol/link.zig");
const Link = link_mod.Link;
const LinkState = link_mod.LinkState;
const messaging = @import("messaging.zig");
const Message = @import("message.zig").Message;
const encoder = @import("types/encoder.zig");
const AmqpValue = @import("types/amqp_value.zig").AmqpValue;

const log = std.log.scoped(.amqp_management);

pub const ManagementState = enum {
    idle,
    opening,
    open,
    closing,
    err,
};

pub const ManagementOperationResult = enum {
    /// The response carried a 2xx status (§3.1, which defers to RFC 2616).
    ok,
    /// The response carried some other status, or none at all.
    error_result,
    /// The management instance was closed before the response arrived.
    instance_closed,
};

/// Called once per request, with the response that answered it.
///
/// `response` does not outlive the call; copy anything worth keeping.
/// `message_id` is the correlation-id the response was matched on, which is
/// what `executeOperation` returned.
pub const OnManagementOperationComplete = *const fn (
    context: ?*anyopaque,
    message_id: u64,
    result: ManagementOperationResult,
    status_code: u32,
    status_description: ?[]const u8,
    response: ?*const Message,
) void;

pub const OnManagementStateChanged = *const fn (
    context: ?*anyopaque,
    new_state: ManagementState,
    previous_state: ManagementState,
) void;

pub const Options = struct {
    /// The application-property the status arrives in. The management spec
    /// names it `statusCode`; CBS renames it, so it is a knob.
    status_code_key: []const u8 = "statusCode",
    status_description_key: []const u8 = "statusDescription",
    /// Credit granted to the receiver once it attaches. One response is
    /// outstanding per request, so this caps concurrent requests as much as
    /// it buffers them.
    receiver_credit: u32 = 100,
    /// Set on each request's `reply-to`. The link pair already tells the peer
    /// where to answer and the C library sends none, so this stays null
    /// unless a service asks for it.
    reply_to: ?[]const u8 = null,
    sender_link_name: []const u8 = "management-sender",
    receiver_link_name: []const u8 = "management-receiver",
};

/// AMQP Management — request-response over a management link pair.
///
/// Its links register the address they live at, so a `Management` must not be
/// moved once `open` has been called.
pub const Management = struct {
    allocator: Allocator,
    session: *Session,
    node_address: []const u8,
    options: Options,
    state: ManagementState,
    next_message_id: u64,

    /// Created by `open`, not `init`, because `init` returns by value and a
    /// link registers its own address with the session.
    sender: ?Link,
    receiver: ?Link,

    on_state_changed: ?OnManagementStateChanged,
    on_state_changed_context: ?*anyopaque,

    /// Requests sent but not yet answered, plus requests not yet sent for
    /// want of credit — `sent` says which.
    pending_operations: std.ArrayList(PendingOp),

    const PendingOp = struct {
        message_id: u64,
        /// The encoded request, kept only until it has gone out.
        payload: ?[]u8,
        sent: bool,
        on_complete: OnManagementOperationComplete,
        context: ?*anyopaque,
    };

    pub fn init(allocator: Allocator, session: *Session, node_address: []const u8, options: Options) Management {
        return .{
            .allocator = allocator,
            .session = session,
            .node_address = node_address,
            .options = options,
            .state = .idle,
            .next_message_id = 0,
            .sender = null,
            .receiver = null,
            .on_state_changed = null,
            .on_state_changed_context = null,
            .pending_operations = .empty,
        };
    }

    pub fn deinit(self: *Management) void {
        for (self.pending_operations.items) |op| {
            if (op.payload) |payload| self.allocator.free(payload);
        }
        self.pending_operations.deinit(self.allocator);
        if (self.receiver) |*l| l.deinit();
        if (self.sender) |*l| l.deinit();
    }

    pub fn setOnStateChanged(self: *Management, cb: OnManagementStateChanged, context: ?*anyopaque) void {
        self.on_state_changed = cb;
        self.on_state_changed_context = context;
    }

    /// Attach the link pair. The state reaches `.open` only once the peer has
    /// attached both, which is some frames later.
    pub fn open(self: *Management) !void {
        if (self.state != .idle) return error.InvalidState;
        self.setState(.opening);
        errdefer self.setState(.err);

        const source = messaging.createSource(self.node_address);
        const target = messaging.createTarget(self.node_address);

        self.sender = try Link.init(
            self.allocator,
            self.session,
            self.options.sender_link_name,
            .sender,
            source,
            target,
        );
        self.receiver = try Link.init(
            self.allocator,
            self.session,
            self.options.receiver_link_name,
            .receiver,
            source,
            target,
        );

        const sender = &self.sender.?;
        const receiver = &self.receiver.?;
        sender.on_state_changed = onLinkStateChanged;
        sender.on_state_changed_context = self;
        sender.on_flow_on = onFlowOn;
        sender.on_flow_on_context = self;
        receiver.on_state_changed = onLinkStateChanged;
        receiver.on_state_changed_context = self;
        receiver.on_transfer_received = onResponse;
        receiver.on_transfer_received_context = self;

        try sender.attach();
        try receiver.attach();
    }

    /// Send a management request, and return the correlation-id its response
    /// will carry.
    ///
    /// `request` is copied and the copy decorated with the operation, entity
    /// type and locales (§3.1.1) and a fresh message-id, so the caller may
    /// reuse or free its message as soon as this returns. A request made
    /// before the peer has granted credit is queued and sent when it does.
    pub fn executeOperation(
        self: *Management,
        operation: []const u8,
        entity_type: ?[]const u8,
        locales: ?[]const u8,
        request: *Message,
        on_complete: OnManagementOperationComplete,
        context: ?*anyopaque,
    ) !u64 {
        if (self.state != .open) return error.InvalidState;

        const message_id = self.next_message_id;

        var decorated = try request.clone();
        defer decorated.deinit();
        try decorated.setApplicationProperty("operation", operation);
        if (entity_type) |value| try decorated.setApplicationProperty("type", value);
        if (locales) |value| try decorated.setApplicationProperty("locales", value);

        var properties = decorated.properties orelse defs.Properties{};
        properties.message_id = .{ .ulong = message_id };
        if (self.options.reply_to) |reply_to| properties.reply_to = reply_to;
        decorated.properties = properties;

        var buf = encoder.Buffer.initDynamic(self.allocator);
        defer buf.deinit();
        try decorated.encode(&buf);
        const payload = try self.allocator.dupe(u8, buf.written());
        errdefer self.allocator.free(payload);

        try self.pending_operations.append(self.allocator, .{
            .message_id = message_id,
            .payload = payload,
            .sent = false,
            .on_complete = on_complete,
            .context = context,
        });
        self.next_message_id += 1;

        // Being unable to send now is not a failure: the request stays
        // queued, and the next Flow sends it.
        self.flushPending() catch |err| switch (err) {
            error.NoCredit, error.InvalidState => {},
            else => return err,
        };
        return message_id;
    }

    /// Detach the link pair and fail everything still outstanding.
    pub fn close(self: *Management) void {
        if (self.state == .idle or self.state == .closing) return;
        self.setState(.closing);

        if (self.sender) |*l| l.detach(true, null) catch |err| {
            log.warn("Detaching the management sender failed: {s}", .{@errorName(err)});
        };
        if (self.receiver) |*l| l.detach(true, null) catch |err| {
            log.warn("Detaching the management receiver failed: {s}", .{@errorName(err)});
        };

        // A caller waiting on a response has to be told, or it waits forever.
        self.failPending(.instance_closed);
        self.setState(.idle);
    }

    // ── Internal ──────────────────────────────────────────────────────

    /// Send everything queued, oldest first, for as long as there is credit.
    fn flushPending(self: *Management) !void {
        const sender = if (self.sender) |*l| l else return error.InvalidState;
        for (self.pending_operations.items) |*op| {
            if (op.sent) continue;
            const payload = op.payload orelse continue;
            _ = try sender.send(payload, .{});
            op.sent = true;
            self.allocator.free(payload);
            op.payload = null;
        }
    }

    fn onFlowOn(context: ?*anyopaque) void {
        const self: *Management = @ptrCast(@alignCast(context.?));
        self.flushPending() catch |err| switch (err) {
            error.NoCredit, error.InvalidState => {},
            else => log.warn("Sending a queued management request failed: {s}", .{@errorName(err)}),
        };
    }

    fn onLinkStateChanged(context: ?*anyopaque, new_state: LinkState, _: LinkState) void {
        const self: *Management = @ptrCast(@alignCast(context.?));
        switch (new_state) {
            .attached => self.onLinkAttached(),
            .err => self.setState(.err),
            else => {},
        }
    }

    fn onLinkAttached(self: *Management) void {
        if (self.state != .opening) return;
        const sender = if (self.sender) |*l| l else return;
        const receiver = if (self.receiver) |*l| l else return;
        // Half a pair can neither ask nor be answered.
        if (sender.state != .attached or receiver.state != .attached) return;

        receiver.flow(self.options.receiver_credit, .{}) catch |err| {
            log.warn("Granting credit to the management receiver failed: {s}", .{@errorName(err)});
            self.setState(.err);
            return;
        };
        self.setState(.open);
    }

    fn onResponse(context: ?*anyopaque, _: defs.Transfer, payload: []const u8) ?defs.DeliveryState {
        const self: *Management = @ptrCast(@alignCast(context.?));

        var response = Message.decode(self.allocator, payload) catch |err| {
            log.warn("Undecodable management response: {s}", .{@errorName(err)});
            return .{ .rejected = .{ .err = .{ .condition = "amqp:decode-error" } } };
        };
        defer response.deinit();

        const correlation_id = if (response.properties) |p| p.correlation_id else null;
        const message_id = if (correlation_id) |value| asUnsigned(value) else null;
        const index = if (message_id) |id| self.indexOf(id) else null;

        if (index == null) {
            // Nothing asked for this. Rejecting says so, rather than
            // accepting a response as though it answered something.
            log.warn("Management response matches no pending request", .{});
            return .{ .rejected = .{ .err = .{ .condition = "amqp:not-found" } } };
        }

        const op = self.pending_operations.orderedRemove(index.?);
        if (op.payload) |queued| self.allocator.free(queued);

        const status_code = blk: {
            const value = response.applicationProperty(self.options.status_code_key) orelse break :blk null;
            break :blk asUnsigned(value);
        };
        const status_description = blk: {
            const value = response.applicationProperty(self.options.status_description_key) orelse break :blk null;
            break :blk if (value == .string) value.string else null;
        };

        // §3.1: success is a 2xx. Anything else — including a response with
        // no status at all — is a failure.
        const code = status_code orelse 0;
        const result: ManagementOperationResult = if (code >= 200 and code < 300) .ok else .error_result;

        op.on_complete(op.context, op.message_id, result, @truncate(code), status_description, &response);
        return .accepted;
    }

    fn indexOf(self: *Management, message_id: u64) ?usize {
        for (self.pending_operations.items, 0..) |op, i| {
            if (op.message_id == message_id) return i;
        }
        return null;
    }

    fn failPending(self: *Management, result: ManagementOperationResult) void {
        for (self.pending_operations.items) |op| {
            if (op.payload) |payload| self.allocator.free(payload);
            op.on_complete(op.context, op.message_id, result, 0, null, null);
        }
        self.pending_operations.clearRetainingCapacity();
    }

    fn setState(self: *Management, new_state: ManagementState) void {
        if (self.state == new_state) return;
        const prev = self.state;
        self.state = new_state;
        log.debug("Management state: {s} -> {s}", .{ @tagName(prev), @tagName(new_state) });
        if (self.on_state_changed) |cb| {
            cb(self.on_state_changed_context, new_state, prev);
        }
    }
};

/// A correlation-id or status code as an unsigned number, whichever of the
/// integer encodings the peer chose. Neither has a meaning when negative, so
/// a negative value is rejected rather than wrapped.
pub fn asUnsigned(value: AmqpValue) ?u64 {
    return switch (value) {
        .ubyte => |v| v,
        .ushort => |v| v,
        .uint => |v| v,
        .ulong => |v| v,
        .byte => |v| if (v < 0) null else @intCast(v),
        .short => |v| if (v < 0) null else @intCast(v),
        .int => |v| if (v < 0) null else @intCast(v),
        .long => |v| if (v < 0) null else @intCast(v),
        else => null,
    };
}

// ── Tests ──────────────────────────────────────────────────────────────

const testing = std.testing;
const Fixture = @import("protocol/test_peer.zig").Fixture;

/// Records what the completion callback was told.
const Recorder = struct {
    calls: usize = 0,
    message_id: u64 = 0,
    result: ManagementOperationResult = .instance_closed,
    status_code: u32 = 0,
    description_buf: [64]u8 = undefined,
    description_len: ?usize = null,
    body_buf: [64]u8 = undefined,
    body_len: ?usize = null,

    fn onComplete(
        context: ?*anyopaque,
        message_id: u64,
        result: ManagementOperationResult,
        status_code: u32,
        status_description: ?[]const u8,
        response: ?*const Message,
    ) void {
        const self: *Recorder = @ptrCast(@alignCast(context.?));
        self.calls += 1;
        self.message_id = message_id;
        self.result = result;
        self.status_code = status_code;
        self.description_len = null;
        self.body_len = null;

        // Both the description and the response are released as soon as this
        // returns, so anything kept has to be copied out.
        if (status_description) |text| {
            @memcpy(self.description_buf[0..text.len], text);
            self.description_len = text.len;
        }
        if (response) |msg| {
            if (msg.body_value) |value| {
                if (value == .string) {
                    @memcpy(self.body_buf[0..value.string.len], value.string);
                    self.body_len = value.string.len;
                }
            }
        }
    }

    fn description(self: *const Recorder) ?[]const u8 {
        return self.description_buf[0 .. self.description_len orelse return null];
    }

    fn body(self: *const Recorder) ?[]const u8 {
        return self.body_buf[0 .. self.body_len orelse return null];
    }
};

/// Bring a Management to `.open`, with both links attached and credited.
fn openManagement(fixture: *Fixture, mgmt: *Management) !void {
    try mgmt.open();
    try fixture.respondAttach(&mgmt.sender.?, 100);
    try fixture.respondAttach(&mgmt.receiver.?, 101);
    try fixture.grant(&mgmt.sender.?, 10);
    fixture.peer.clear();
}

/// The frame a peer would send back to answer `message_id`.
fn responseBytes(
    fixture: *Fixture,
    message_id: u64,
    status_code: i32,
    status_description: ?[]const u8,
    body: ?[]const u8,
) ![]const u8 {
    var response = Message.init(fixture.allocator);
    defer response.deinit();
    response.properties = .{ .correlation_id = .{ .ulong = message_id } };
    try response.putApplicationProperty("statusCode", .{ .int = status_code });
    if (status_description) |text| try response.setApplicationProperty("statusDescription", text);
    if (body) |text| try response.setBodyValue(.{ .string = text });

    var buf = encoder.Buffer.initDynamic(fixture.allocator);
    defer buf.deinit();
    try response.encode(&buf);

    return fixture.peer.framePayload(1, .{ .transfer = .{
        .handle = 101,
        .delivery_id = @intCast(message_id),
        .delivery_tag = "t",
        .more = false,
    } }, buf.written());
}

test "opening attaches a link pair to the management node" {
    const allocator = testing.allocator;
    const fixture = try Fixture.init(allocator);
    defer fixture.deinit();

    var mgmt = Management.init(allocator, &fixture.session, "$management", .{});
    defer mgmt.deinit();
    try mgmt.open();

    const attaches = try fixture.peer.performatives();
    try testing.expectEqual(@as(usize, 2), attaches.len);
    try testing.expectEqual(defs.Role.sender, attaches[0].performative.attach.role);
    try testing.expectEqualStrings("$management", attaches[0].performative.attach.target.?.address.?);
    try testing.expectEqual(defs.Role.receiver, attaches[1].performative.attach.role);
    try testing.expectEqualStrings("$management", attaches[1].performative.attach.source.?.address.?);

    // One attached link is not a usable pair.
    try fixture.respondAttach(&mgmt.sender.?, 100);
    try testing.expectEqual(ManagementState.opening, mgmt.state);

    try fixture.respondAttach(&mgmt.receiver.?, 101);
    try testing.expectEqual(ManagementState.open, mgmt.state);
}

test "the management receiver is credited as soon as the pair is up" {
    const allocator = testing.allocator;
    const fixture = try Fixture.init(allocator);
    defer fixture.deinit();

    var mgmt = Management.init(allocator, &fixture.session, "$management", .{ .receiver_credit = 7 });
    defer mgmt.deinit();
    try mgmt.open();
    try fixture.respondAttach(&mgmt.sender.?, 100);
    try fixture.respondAttach(&mgmt.receiver.?, 101);

    // Nothing can answer until the peer is allowed to send.
    const flow = try fixture.peer.onlyPerformative();
    try testing.expectEqual(@as(?u32, 7), flow.performative.flow.link_credit);
}

test "a request carries the operation, type and a fresh message id" {
    const allocator = testing.allocator;
    const fixture = try Fixture.init(allocator);
    defer fixture.deinit();

    var mgmt = Management.init(allocator, &fixture.session, "$management", .{});
    defer mgmt.deinit();
    try openManagement(fixture, &mgmt);

    var request = Message.init(allocator);
    defer request.deinit();
    try request.setApplicationProperty("name", "queue-1");

    var recorder = Recorder{};
    const message_id = try mgmt.executeOperation("READ", "queue", "en-US", &request, Recorder.onComplete, &recorder);
    try testing.expectEqual(@as(u64, 0), message_id);

    const sent = try fixture.peer.onlyPerformative();
    var decoded = try Message.decode(allocator, sent.payload);
    defer decoded.deinit();
    try testing.expectEqualStrings("READ", decoded.applicationProperty("operation").?.string);
    try testing.expectEqualStrings("queue", decoded.applicationProperty("type").?.string);
    try testing.expectEqualStrings("en-US", decoded.applicationProperty("locales").?.string);
    try testing.expectEqualStrings("queue-1", decoded.applicationProperty("name").?.string);
    try testing.expectEqual(@as(u64, 0), decoded.properties.?.message_id.?.ulong);

    // The caller's own message is untouched, and the ids keep counting.
    try testing.expect(request.applicationProperty("operation") == null);
    try testing.expectEqual(
        @as(u64, 1),
        try mgmt.executeOperation("READ", null, null, &request, Recorder.onComplete, &recorder),
    );
}

test "a response completes the request it correlates to" {
    const allocator = testing.allocator;
    const fixture = try Fixture.init(allocator);
    defer fixture.deinit();

    var mgmt = Management.init(allocator, &fixture.session, "$management", .{});
    defer mgmt.deinit();
    try openManagement(fixture, &mgmt);

    var request = Message.init(allocator);
    defer request.deinit();
    var first = Recorder{};
    var second = Recorder{};
    const id_a = try mgmt.executeOperation("READ", null, null, &request, Recorder.onComplete, &first);
    const id_b = try mgmt.executeOperation("READ", null, null, &request, Recorder.onComplete, &second);
    fixture.peer.clear();

    // Answering the second request first is allowed, and must not complete
    // the first one.
    try fixture.conn.onBytesReceived(try responseBytes(fixture, id_b, 200, "OK", "payload"));
    try testing.expectEqual(@as(usize, 0), first.calls);
    try testing.expectEqual(@as(usize, 1), second.calls);
    try testing.expectEqual(id_b, second.message_id);
    try testing.expectEqual(ManagementOperationResult.ok, second.result);
    try testing.expectEqual(@as(u32, 200), second.status_code);
    try testing.expectEqualStrings("OK", second.description().?);
    try testing.expectEqualStrings("payload", second.body().?);

    try fixture.conn.onBytesReceived(try responseBytes(fixture, id_a, 404, "Not Found", null));
    try testing.expectEqual(@as(usize, 1), first.calls);
    try testing.expectEqual(ManagementOperationResult.error_result, first.result);
    try testing.expectEqual(@as(u32, 404), first.status_code);
    try testing.expectEqualStrings("Not Found", first.description().?);
    try testing.expect(first.body() == null);

    try testing.expectEqual(@as(usize, 0), mgmt.pending_operations.items.len);
}

test "a request made without credit waits for a Flow" {
    const allocator = testing.allocator;
    const fixture = try Fixture.init(allocator);
    defer fixture.deinit();

    var mgmt = Management.init(allocator, &fixture.session, "$management", .{});
    defer mgmt.deinit();
    try mgmt.open();
    try fixture.respondAttach(&mgmt.sender.?, 100);
    try fixture.respondAttach(&mgmt.receiver.?, 101);
    fixture.peer.clear();

    var request = Message.init(allocator);
    defer request.deinit();
    var recorder = Recorder{};
    const message_id = try mgmt.executeOperation("READ", null, null, &request, Recorder.onComplete, &recorder);

    // Nothing goes out: the peer has not said it will accept anything yet.
    try testing.expectEqual(@as(usize, 0), fixture.peer.written().len);
    try testing.expect(!mgmt.pending_operations.items[0].sent);

    try fixture.grant(&mgmt.sender.?, 5);
    const sent = try fixture.peer.onlyPerformative();
    var decoded = try Message.decode(allocator, sent.payload);
    defer decoded.deinit();
    try testing.expectEqualStrings("READ", decoded.applicationProperty("operation").?.string);
    try testing.expect(mgmt.pending_operations.items[0].sent);

    fixture.peer.clear();
    try fixture.conn.onBytesReceived(try responseBytes(fixture, message_id, 202, null, null));
    try testing.expectEqual(ManagementOperationResult.ok, recorder.result);
}

test "an uncorrelated response is rejected, not silently dropped" {
    const allocator = testing.allocator;
    const fixture = try Fixture.init(allocator);
    defer fixture.deinit();

    var mgmt = Management.init(allocator, &fixture.session, "$management", .{});
    defer mgmt.deinit();
    try openManagement(fixture, &mgmt);
    fixture.peer.clear();

    try fixture.conn.onBytesReceived(try responseBytes(fixture, 77, 200, null, null));

    const disposition = try fixture.peer.onlyPerformative();
    try testing.expect(disposition.performative.disposition.delivery_state.? == .rejected);
}

test "a response with no status at all is a failure, not a success" {
    const allocator = testing.allocator;
    const fixture = try Fixture.init(allocator);
    defer fixture.deinit();

    var mgmt = Management.init(allocator, &fixture.session, "$management", .{});
    defer mgmt.deinit();
    try openManagement(fixture, &mgmt);

    var request = Message.init(allocator);
    defer request.deinit();
    var recorder = Recorder{};
    const message_id = try mgmt.executeOperation("READ", null, null, &request, Recorder.onComplete, &recorder);
    fixture.peer.clear();

    var response = Message.init(allocator);
    defer response.deinit();
    response.properties = .{ .correlation_id = .{ .ulong = message_id } };
    var buf = encoder.Buffer.initDynamic(allocator);
    defer buf.deinit();
    try response.encode(&buf);
    try fixture.conn.onBytesReceived(try fixture.peer.framePayload(1, .{ .transfer = .{
        .handle = 101,
        .delivery_id = 0,
        .delivery_tag = "t",
    } }, buf.written()));

    try testing.expectEqual(@as(usize, 1), recorder.calls);
    try testing.expectEqual(ManagementOperationResult.error_result, recorder.result);
    try testing.expectEqual(@as(u32, 0), recorder.status_code);
}

test "closing fails everything still outstanding" {
    const allocator = testing.allocator;
    const fixture = try Fixture.init(allocator);
    defer fixture.deinit();

    var mgmt = Management.init(allocator, &fixture.session, "$management", .{});
    defer mgmt.deinit();
    try openManagement(fixture, &mgmt);

    var request = Message.init(allocator);
    defer request.deinit();
    var recorder = Recorder{};
    _ = try mgmt.executeOperation("READ", null, null, &request, Recorder.onComplete, &recorder);

    mgmt.close();
    try testing.expectEqual(@as(usize, 1), recorder.calls);
    try testing.expectEqual(ManagementOperationResult.instance_closed, recorder.result);
    try testing.expectEqual(ManagementState.idle, mgmt.state);
}

test "operations are refused until the pair is open" {
    const allocator = testing.allocator;
    const fixture = try Fixture.init(allocator);
    defer fixture.deinit();

    var mgmt = Management.init(allocator, &fixture.session, "$management", .{});
    defer mgmt.deinit();

    var request = Message.init(allocator);
    defer request.deinit();
    var recorder = Recorder{};
    try testing.expectError(
        error.InvalidState,
        mgmt.executeOperation("READ", null, null, &request, Recorder.onComplete, &recorder),
    );

    try mgmt.open();
    try testing.expectError(error.InvalidState, mgmt.open());
    try testing.expectError(
        error.InvalidState,
        mgmt.executeOperation("READ", null, null, &request, Recorder.onComplete, &recorder),
    );
}

test "an integer of any width reads as a status code" {
    try testing.expectEqual(@as(?u64, 200), asUnsigned(.{ .int = 200 }));
    try testing.expectEqual(@as(?u64, 200), asUnsigned(.{ .uint = 200 }));
    try testing.expectEqual(@as(?u64, 200), asUnsigned(.{ .ubyte = 200 }));
    try testing.expectEqual(@as(?u64, 404), asUnsigned(.{ .ushort = 404 }));
    try testing.expectEqual(@as(?u64, 404), asUnsigned(.{ .long = 404 }));
    try testing.expectEqual(@as(?u64, null), asUnsigned(.{ .int = -1 }));
    try testing.expectEqual(@as(?u64, null), asUnsigned(.{ .string = "200" }));
}
