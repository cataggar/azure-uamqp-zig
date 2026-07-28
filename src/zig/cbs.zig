///! Claims-Based Security (CBS) for Azure AMQP
///!
///! CBS is the AMQP management protocol pointed at the `$cbs` node: a
///! `put-token` request carries a credential for an audience, and the service
///! answers with a status. Everything but the operation names and the status
///! keys is management's job.
const std = @import("std");
const Allocator = std.mem.Allocator;
const Session = @import("protocol/session.zig").Session;
const management = @import("management.zig");
const Management = management.Management;
const ManagementState = management.ManagementState;
const ManagementOperationResult = management.ManagementOperationResult;
const Message = @import("message.zig").Message;

const log = std.log.scoped(.amqp_cbs);

/// The node every CBS exchange goes through (§3.1 of the CBS spec).
pub const node_address = "$cbs";

pub const CbsOperationResult = enum {
    ok,
    cbs_error,
    instance_closed,
};

pub const CbsState = enum {
    closed,
    opening,
    open,
    closing,
    err,
};

pub const OnCbsOperationComplete = *const fn (
    context: ?*anyopaque,
    result: CbsOperationResult,
    status_code: u32,
    status_description: ?[]const u8,
) void;

pub const OnCbsStateChanged = *const fn (
    context: ?*anyopaque,
    new_state: CbsState,
    previous_state: CbsState,
) void;

/// CBS (Claims-Based Security) handle for Azure AMQP authentication.
///
/// Wraps a `Management` aimed at `$cbs`, so like it, a `Cbs` must not be
/// moved once `open` has been called.
pub const Cbs = struct {
    allocator: Allocator,
    management: Management,
    state: CbsState,

    on_state_changed: ?OnCbsStateChanged,
    on_state_changed_context: ?*anyopaque,

    /// Requests whose response has not arrived. Keyed by the correlation-id
    /// management assigned, which is what its callback reports back.
    pending_operations: std.ArrayList(PendingOp),

    const PendingOp = struct {
        message_id: u64,
        on_complete: OnCbsOperationComplete,
        context: ?*anyopaque,
    };

    pub fn init(allocator: Allocator, session: *Session) Cbs {
        return .{
            .allocator = allocator,
            // CBS renames both status keys, which is the whole of its
            // difference from a plain management node.
            .management = Management.init(allocator, session, node_address, .{
                .status_code_key = "status-code",
                .status_description_key = "status-description",
                .sender_link_name = "cbs-sender",
                .receiver_link_name = "cbs-receiver",
            }),
            .state = .closed,
            .on_state_changed = null,
            .on_state_changed_context = null,
            .pending_operations = .empty,
        };
    }

    pub fn deinit(self: *Cbs) void {
        self.pending_operations.deinit(self.allocator);
        self.management.deinit();
    }

    pub fn setOnStateChanged(self: *Cbs, cb: OnCbsStateChanged, context: ?*anyopaque) void {
        self.on_state_changed = cb;
        self.on_state_changed_context = context;
    }

    /// Attach the `$cbs` link pair. The state reaches `.open` only once the
    /// peer has attached both, which is some frames later.
    pub fn open(self: *Cbs) !void {
        if (self.state != .closed) return error.InvalidState;
        self.setState(.opening);
        errdefer self.setState(.err);

        self.management.setOnStateChanged(onManagementStateChanged, self);
        try self.management.open();
    }

    /// Submit a put-token operation for authentication.
    ///
    /// `token_type` is the credential's type — `jwt` for an AAD token,
    /// `servicebus.windows.net:sastoken` for SAS — and `audience` is the
    /// entity it is good for. None of the three slices is retained.
    pub fn putToken(
        self: *Cbs,
        token_type: []const u8,
        audience: []const u8,
        token: []const u8,
        on_complete: OnCbsOperationComplete,
        context: ?*anyopaque,
    ) !void {
        if (self.state != .open) return error.InvalidState;

        var request = Message.init(self.allocator);
        defer request.deinit();
        try request.setApplicationProperty("name", audience);
        // The credential itself is the body, not a property (§3.2).
        try request.setBodyValue(.{ .string = token });

        try self.submit("put-token", token_type, &request, on_complete, context);
    }

    /// Revoke a token previously put for `audience`.
    pub fn deleteToken(
        self: *Cbs,
        token_type: []const u8,
        audience: []const u8,
        on_complete: OnCbsOperationComplete,
        context: ?*anyopaque,
    ) !void {
        if (self.state != .open) return error.InvalidState;

        var request = Message.init(self.allocator);
        defer request.deinit();
        try request.setApplicationProperty("name", audience);
        try request.setBodyValue(.{ .string = "" });

        try self.submit("delete-token", token_type, &request, on_complete, context);
    }

    /// Close the CBS session.
    pub fn close(self: *Cbs) void {
        if (self.state == .closed or self.state == .closing) return;
        self.setState(.closing);
        // Management fails everything outstanding, which lands back here.
        self.management.close();
        self.setState(.closed);
    }

    // ── Internal ──────────────────────────────────────────────────────

    fn submit(
        self: *Cbs,
        operation: []const u8,
        token_type: []const u8,
        request: *Message,
        on_complete: OnCbsOperationComplete,
        context: ?*anyopaque,
    ) !void {
        // Reserved first: a pending entry that cannot be recorded would leave
        // a response with nothing to complete.
        try self.pending_operations.ensureUnusedCapacity(self.allocator, 1);
        const message_id = try self.management.executeOperation(
            operation,
            token_type,
            null,
            request,
            onOperationComplete,
            self,
        );
        self.pending_operations.appendAssumeCapacity(.{
            .message_id = message_id,
            .on_complete = on_complete,
            .context = context,
        });
    }

    fn onOperationComplete(
        context: ?*anyopaque,
        message_id: u64,
        result: ManagementOperationResult,
        status_code: u32,
        status_description: ?[]const u8,
        _: ?*const Message,
    ) void {
        const self: *Cbs = @ptrCast(@alignCast(context.?));
        const index = self.indexOf(message_id) orelse {
            log.warn("CBS completion for an unknown operation: {d}", .{message_id});
            return;
        };
        const op = self.pending_operations.orderedRemove(index);
        op.on_complete(op.context, switch (result) {
            .ok => .ok,
            .error_result => .cbs_error,
            .instance_closed => .instance_closed,
        }, status_code, status_description);
    }

    fn onManagementStateChanged(context: ?*anyopaque, new_state: ManagementState, _: ManagementState) void {
        const self: *Cbs = @ptrCast(@alignCast(context.?));
        switch (new_state) {
            .open => self.setState(.open),
            .err => self.setState(.err),
            else => {},
        }
    }

    fn indexOf(self: *Cbs, message_id: u64) ?usize {
        for (self.pending_operations.items, 0..) |op, i| {
            if (op.message_id == message_id) return i;
        }
        return null;
    }

    fn setState(self: *Cbs, new_state: CbsState) void {
        if (self.state == new_state) return;
        const prev = self.state;
        self.state = new_state;
        log.debug("CBS state: {s} -> {s}", .{ @tagName(prev), @tagName(new_state) });
        if (self.on_state_changed) |cb| {
            cb(self.on_state_changed_context, new_state, prev);
        }
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

const testing = std.testing;
const defs = @import("protocol/definitions.zig");
const encoder = @import("types/encoder.zig");
const Fixture = @import("protocol/test_peer.zig").Fixture;

const Recorder = struct {
    calls: usize = 0,
    result: CbsOperationResult = .instance_closed,
    status_code: u32 = 0,
    description_buf: [64]u8 = undefined,
    description_len: ?usize = null,

    fn onComplete(
        context: ?*anyopaque,
        result: CbsOperationResult,
        status_code: u32,
        status_description: ?[]const u8,
    ) void {
        const self: *Recorder = @ptrCast(@alignCast(context.?));
        self.calls += 1;
        self.result = result;
        self.status_code = status_code;
        self.description_len = null;
        if (status_description) |text| {
            @memcpy(self.description_buf[0..text.len], text);
            self.description_len = text.len;
        }
    }

    fn description(self: *const Recorder) ?[]const u8 {
        return self.description_buf[0 .. self.description_len orelse return null];
    }
};

fn openCbs(fixture: *Fixture, cbs: *Cbs) !void {
    try cbs.open();
    try fixture.respondAttach(&cbs.management.sender.?, 100);
    try fixture.respondAttach(&cbs.management.receiver.?, 101);
    try fixture.grant(&cbs.management.sender.?, 10);
    fixture.peer.clear();
}

/// The frame the service would send back to answer `message_id`.
fn responseBytes(fixture: *Fixture, message_id: u64, status_code: i32, status_description: ?[]const u8) ![]const u8 {
    var response = Message.init(fixture.allocator);
    defer response.deinit();
    response.properties = .{ .correlation_id = .{ .ulong = message_id } };
    try response.putApplicationProperty("status-code", .{ .int = status_code });
    if (status_description) |text| try response.setApplicationProperty("status-description", text);

    var buf = encoder.Buffer.initDynamic(fixture.allocator);
    defer buf.deinit();
    try response.encode(&buf);

    return fixture.peer.framePayload(1, .{ .transfer = .{
        .handle = 101,
        .delivery_id = @intCast(message_id),
        .delivery_tag = "t",
    } }, buf.written());
}

test "CBS init and open" {
    const allocator = testing.allocator;
    const fixture = try Fixture.init(allocator);
    defer fixture.deinit();

    var cbs = Cbs.init(allocator, &fixture.session);
    defer cbs.deinit();
    try testing.expectEqual(CbsState.closed, cbs.state);

    try cbs.open();
    // Opening is not open: the peer has not attached anything yet.
    try testing.expectEqual(CbsState.opening, cbs.state);

    const attaches = try fixture.peer.performatives();
    try testing.expectEqual(@as(usize, 2), attaches.len);
    try testing.expectEqualStrings("$cbs", attaches[0].performative.attach.target.?.address.?);
    try testing.expectEqualStrings("$cbs", attaches[1].performative.attach.source.?.address.?);

    try fixture.respondAttach(&cbs.management.sender.?, 100);
    try fixture.respondAttach(&cbs.management.receiver.?, 101);
    try testing.expectEqual(CbsState.open, cbs.state);
}

test "CBS close" {
    const allocator = testing.allocator;
    const fixture = try Fixture.init(allocator);
    defer fixture.deinit();

    var cbs = Cbs.init(allocator, &fixture.session);
    defer cbs.deinit();
    try openCbs(fixture, &cbs);

    cbs.close();
    try testing.expectEqual(CbsState.closed, cbs.state);
}

test "put-token sends the credential and reports the status" {
    const allocator = testing.allocator;
    const fixture = try Fixture.init(allocator);
    defer fixture.deinit();

    var cbs = Cbs.init(allocator, &fixture.session);
    defer cbs.deinit();
    try openCbs(fixture, &cbs);
    fixture.peer.clear();

    var recorder = Recorder{};
    try cbs.putToken(
        "servicebus.windows.net:sastoken",
        "sb://example.servicebus.windows.net/queue-1",
        "SharedAccessSignature sr=example&sig=abc&se=1&skn=key",
        Recorder.onComplete,
        &recorder,
    );

    const sent = try fixture.peer.onlyPerformative();
    var request = try Message.decode(allocator, sent.payload);
    defer request.deinit();
    try testing.expectEqualStrings("put-token", request.applicationProperty("operation").?.string);
    try testing.expectEqualStrings("servicebus.windows.net:sastoken", request.applicationProperty("type").?.string);
    try testing.expectEqualStrings("sb://example.servicebus.windows.net/queue-1", request.applicationProperty("name").?.string);
    // The credential rides in the body, not in a property.
    try testing.expectEqualStrings("SharedAccessSignature sr=example&sig=abc&se=1&skn=key", request.body_value.?.string);

    try testing.expectEqual(@as(usize, 0), recorder.calls);
    try fixture.conn.onBytesReceived(try responseBytes(fixture, request.properties.?.message_id.?.ulong, 202, "Accepted"));
    try testing.expectEqual(@as(usize, 1), recorder.calls);
    try testing.expectEqual(CbsOperationResult.ok, recorder.result);
    try testing.expectEqual(@as(u32, 202), recorder.status_code);
    try testing.expectEqualStrings("Accepted", recorder.description().?);
}

test "a rejected token is reported, not swallowed" {
    const allocator = testing.allocator;
    const fixture = try Fixture.init(allocator);
    defer fixture.deinit();

    var cbs = Cbs.init(allocator, &fixture.session);
    defer cbs.deinit();
    try openCbs(fixture, &cbs);
    fixture.peer.clear();

    var recorder = Recorder{};
    try cbs.putToken("jwt", "sb://example/entity", "expired", Recorder.onComplete, &recorder);
    const sent = try fixture.peer.onlyPerformative();
    var request = try Message.decode(allocator, sent.payload);
    defer request.deinit();

    try fixture.conn.onBytesReceived(try responseBytes(
        fixture,
        request.properties.?.message_id.?.ulong,
        401,
        "Unauthorized",
    ));
    try testing.expectEqual(CbsOperationResult.cbs_error, recorder.result);
    try testing.expectEqual(@as(u32, 401), recorder.status_code);
    try testing.expectEqualStrings("Unauthorized", recorder.description().?);
}

test "two tokens in flight complete independently" {
    const allocator = testing.allocator;
    const fixture = try Fixture.init(allocator);
    defer fixture.deinit();

    var cbs = Cbs.init(allocator, &fixture.session);
    defer cbs.deinit();
    try openCbs(fixture, &cbs);
    fixture.peer.clear();

    var first = Recorder{};
    var second = Recorder{};
    try cbs.putToken("jwt", "sb://example/a", "token-a", Recorder.onComplete, &first);
    try cbs.putToken("jwt", "sb://example/b", "token-b", Recorder.onComplete, &second);
    try testing.expectEqual(@as(usize, 2), cbs.pending_operations.items.len);

    const sent = try fixture.peer.performatives();
    try testing.expectEqual(@as(usize, 2), sent.len);
    var request_b = try Message.decode(allocator, sent[1].payload);
    defer request_b.deinit();
    try testing.expectEqualStrings("token-b", request_b.body_value.?.string);

    try fixture.conn.onBytesReceived(try responseBytes(fixture, request_b.properties.?.message_id.?.ulong, 202, null));
    try testing.expectEqual(@as(usize, 0), first.calls);
    try testing.expectEqual(@as(usize, 1), second.calls);
    try testing.expectEqual(@as(usize, 1), cbs.pending_operations.items.len);
}

test "delete-token asks for the same audience by name" {
    const allocator = testing.allocator;
    const fixture = try Fixture.init(allocator);
    defer fixture.deinit();

    var cbs = Cbs.init(allocator, &fixture.session);
    defer cbs.deinit();
    try openCbs(fixture, &cbs);
    fixture.peer.clear();

    var recorder = Recorder{};
    try cbs.deleteToken("jwt", "sb://example/entity", Recorder.onComplete, &recorder);

    const sent = try fixture.peer.onlyPerformative();
    var request = try Message.decode(allocator, sent.payload);
    defer request.deinit();
    try testing.expectEqualStrings("delete-token", request.applicationProperty("operation").?.string);
    try testing.expectEqualStrings("sb://example/entity", request.applicationProperty("name").?.string);
}

test "a token cannot be put before CBS is open" {
    const allocator = testing.allocator;
    const fixture = try Fixture.init(allocator);
    defer fixture.deinit();

    var cbs = Cbs.init(allocator, &fixture.session);
    defer cbs.deinit();

    var recorder = Recorder{};
    try testing.expectError(
        error.InvalidState,
        cbs.putToken("jwt", "sb://example/entity", "token", Recorder.onComplete, &recorder),
    );

    try cbs.open();
    // Still only half-way up.
    try testing.expectError(
        error.InvalidState,
        cbs.putToken("jwt", "sb://example/entity", "token", Recorder.onComplete, &recorder),
    );
    try testing.expectError(error.InvalidState, cbs.open());
}

test "closing tells a caller waiting on a token, rather than leaving it waiting" {
    const allocator = testing.allocator;
    const fixture = try Fixture.init(allocator);
    defer fixture.deinit();

    var cbs = Cbs.init(allocator, &fixture.session);
    defer cbs.deinit();
    try openCbs(fixture, &cbs);

    var recorder = Recorder{};
    try cbs.putToken("jwt", "sb://example/entity", "token", Recorder.onComplete, &recorder);
    cbs.close();

    try testing.expectEqual(@as(usize, 1), recorder.calls);
    try testing.expectEqual(CbsOperationResult.instance_closed, recorder.result);
    try testing.expectEqual(@as(usize, 0), cbs.pending_operations.items.len);
}
