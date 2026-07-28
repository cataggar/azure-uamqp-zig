//! AMQP 1.0 performative types (OASIS spec §2.7)
//!
//! Each performative, delivery state and composite is a plain Zig struct.
//! `described.zig` encodes and decodes them by reflecting over these
//! declarations, so the field order here is the field order on the wire and a
//! field without a default is a field the spec marks mandatory. This replaces
//! the C macro-generated amqp_definitions.c; `codegen/amqp_definitions.xml` is
//! the reference it is checked against.
//!
//! Where AMQP has three types and Zig has one `[]const u8`, the struct says
//! which with `amqp_symbols`, `amqp_binaries` and `amqp_timestamps`.
const std = @import("std");
const Allocator = std.mem.Allocator;
const AmqpValue = @import("../types/amqp_value.zig").AmqpValue;
const MapEntry = @import("../types/amqp_value.zig").MapEntry;

// ── AMQP Descriptor Codes (§2.7.1) ────────────────────────────────────

pub const descriptor = struct {
    pub const open: u64 = 0x0000000000000010;
    pub const begin: u64 = 0x0000000000000011;
    pub const attach: u64 = 0x0000000000000012;
    pub const flow: u64 = 0x0000000000000013;
    pub const transfer: u64 = 0x0000000000000014;
    pub const disposition: u64 = 0x0000000000000015;
    pub const detach: u64 = 0x0000000000000016;
    pub const end: u64 = 0x0000000000000017;
    pub const close: u64 = 0x0000000000000018;

    // SASL
    pub const sasl_mechanisms: u64 = 0x0000000000000040;
    pub const sasl_init: u64 = 0x0000000000000041;
    pub const sasl_challenge: u64 = 0x0000000000000042;
    pub const sasl_response: u64 = 0x0000000000000043;
    pub const sasl_outcome: u64 = 0x0000000000000044;

    // Messaging
    pub const header: u64 = 0x0000000000000070;
    pub const delivery_annotations: u64 = 0x0000000000000071;
    pub const message_annotations: u64 = 0x0000000000000072;
    pub const properties: u64 = 0x0000000000000073;
    pub const application_properties: u64 = 0x0000000000000074;
    pub const data: u64 = 0x0000000000000075;
    pub const amqp_sequence: u64 = 0x0000000000000076;
    pub const amqp_value: u64 = 0x0000000000000077;
    pub const footer: u64 = 0x0000000000000078;

    // Delivery state
    pub const received: u64 = 0x0000000000000023;
    pub const accepted: u64 = 0x0000000000000024;
    pub const rejected: u64 = 0x0000000000000025;
    pub const released: u64 = 0x0000000000000026;
    pub const modified: u64 = 0x0000000000000027;

    // Addressing
    pub const source: u64 = 0x0000000000000028;
    pub const target: u64 = 0x0000000000000029;

    pub const @"error": u64 = 0x000000000000001d;
};

// ── Role ──────────────────────────────────────────────────────────────

pub const Role = enum {
    sender,
    receiver,

    pub fn toBool(self: Role) bool {
        return self == .receiver;
    }

    pub fn fromBool(v: bool) Role {
        return if (v) .receiver else .sender;
    }
};

// ── Settle Modes ──────────────────────────────────────────────────────

pub const SenderSettleMode = enum(u8) {
    unsettled = 0,
    settled = 1,
    mixed = 2,
};

pub const ReceiverSettleMode = enum(u8) {
    first = 0,
    second = 1,
};

// ── SASL Code ─────────────────────────────────────────────────────────

pub const SaslCode = enum(u8) {
    ok = 0,
    auth = 1,
    sys = 2,
    sys_perm = 3,
    sys_temp = 4,
};

// ── Error ─────────────────────────────────────────────────────────────

pub const AmqpError = struct {
    condition: []const u8,
    description: ?[]const u8 = null,
    info: ?[]MapEntry = null,

    pub const amqp_descriptor = descriptor.@"error";
    pub const amqp_symbols: []const []const u8 = &.{"condition"};
};

// ── Open (§2.7.1) ─────────────────────────────────────────────────────

pub const Open = struct {
    container_id: []const u8,
    hostname: ?[]const u8 = null,
    max_frame_size: u32 = 4294967295,
    channel_max: u16 = 65535,
    idle_time_out: ?u32 = null,
    outgoing_locales: ?[]const []const u8 = null,
    incoming_locales: ?[]const []const u8 = null,
    offered_capabilities: ?[]const []const u8 = null,
    desired_capabilities: ?[]const []const u8 = null,
    properties: ?[]MapEntry = null,

    pub const amqp_descriptor = descriptor.open;
};

// ── Begin (§2.7.2) ────────────────────────────────────────────────────

pub const Begin = struct {
    remote_channel: ?u16 = null,
    next_outgoing_id: u32,
    incoming_window: u32,
    outgoing_window: u32,
    handle_max: u32 = 4294967295,
    offered_capabilities: ?[]const []const u8 = null,
    desired_capabilities: ?[]const []const u8 = null,
    properties: ?[]MapEntry = null,

    pub const amqp_descriptor = descriptor.begin;
};

// ── Attach (§2.7.3) ──────────────────────────────────────────────────

pub const Attach = struct {
    name: []const u8,
    handle: u32,
    role: Role,
    snd_settle_mode: SenderSettleMode = .mixed,
    rcv_settle_mode: ReceiverSettleMode = .first,
    source: ?Source = null,
    target: ?Target = null,
    unsettled: ?[]MapEntry = null,
    incomplete_unsettled: bool = false,
    initial_delivery_count: ?u32 = null,
    max_message_size: ?u64 = null,
    offered_capabilities: ?[]const []const u8 = null,
    desired_capabilities: ?[]const []const u8 = null,
    properties: ?[]MapEntry = null,

    pub const amqp_descriptor = descriptor.attach;
};

// ── Flow (§2.7.4) ────────────────────────────────────────────────────

pub const Flow = struct {
    next_incoming_id: ?u32 = null,
    incoming_window: u32,
    next_outgoing_id: u32,
    outgoing_window: u32,
    handle: ?u32 = null,
    delivery_count: ?u32 = null,
    link_credit: ?u32 = null,
    available: ?u32 = null,
    drain: bool = false,
    echo: bool = false,
    properties: ?[]MapEntry = null,

    pub const amqp_descriptor = descriptor.flow;
};

// ── Transfer (§2.7.5) ────────────────────────────────────────────────

pub const Transfer = struct {
    handle: u32,
    delivery_id: ?u32 = null,
    delivery_tag: ?[]const u8 = null,
    message_format: ?u32 = null,
    settled: ?bool = null,
    more: bool = false,
    rcv_settle_mode: ?ReceiverSettleMode = null,
    delivery_state: ?DeliveryState = null,
    is_resume: bool = false,
    aborted: bool = false,
    batchable: bool = false,

    pub const amqp_descriptor = descriptor.transfer;
    pub const amqp_binaries: []const []const u8 = &.{"delivery_tag"};
};

// ── Disposition (§2.7.6) ─────────────────────────────────────────────

pub const Disposition = struct {
    role: Role,
    first: u32,
    last: ?u32 = null,
    settled: bool = false,
    delivery_state: ?DeliveryState = null,
    batchable: bool = false,

    pub const amqp_descriptor = descriptor.disposition;
};

// ── Detach (§2.7.7) ─────────────────────────────────────────────────

pub const Detach = struct {
    handle: u32,
    closed: bool = false,
    err: ?AmqpError = null,

    pub const amqp_descriptor = descriptor.detach;
};

// ── End (§2.7.8) ────────────────────────────────────────────────────

pub const End = struct {
    err: ?AmqpError = null,

    pub const amqp_descriptor = descriptor.end;
};

// ── Close (§2.7.9) ──────────────────────────────────────────────────

pub const Close = struct {
    err: ?AmqpError = null,

    pub const amqp_descriptor = descriptor.close;
};

// ── Delivery State ──────────────────────────────────────────────────

pub const DeliveryState = union(enum) {
    received: Received,
    accepted,
    rejected: Rejected,
    released,
    modified: Modified,
};

pub const Received = struct {
    section_number: u32,
    section_offset: u64,

    pub const amqp_descriptor = descriptor.received;
};

pub const Rejected = struct {
    err: ?AmqpError = null,

    pub const amqp_descriptor = descriptor.rejected;
};

pub const Modified = struct {
    delivery_failed: ?bool = null,
    undeliverable_here: ?bool = null,
    message_annotations: ?[]MapEntry = null,

    pub const amqp_descriptor = descriptor.modified;
};

// ── Source (§3.5.3) ─────────────────────────────────────────────────

pub const TerminusDurability = enum(u32) {
    none = 0,
    configuration = 1,
    unsettled_state = 2,
};

pub const TerminusExpiryPolicy = enum {
    link_detach,
    session_end,
    connection_close,
    never,

    pub fn toSymbol(self: TerminusExpiryPolicy) []const u8 {
        return switch (self) {
            .link_detach => "link-detach",
            .session_end => "session-end",
            .connection_close => "connection-close",
            .never => "never",
        };
    }

    pub fn fromSymbol(symbol: []const u8) ?TerminusExpiryPolicy {
        inline for (@typeInfo(TerminusExpiryPolicy).@"enum".fields) |field| {
            const value: TerminusExpiryPolicy = @enumFromInt(field.value);
            if (std.mem.eql(u8, value.toSymbol(), symbol)) return value;
        }
        return null;
    }
};

pub const Source = struct {
    address: ?[]const u8 = null,
    durable: TerminusDurability = .none,
    expiry_policy: TerminusExpiryPolicy = .session_end,
    timeout: u32 = 0,
    dynamic: bool = false,
    dynamic_node_properties: ?[]MapEntry = null,
    distribution_mode: ?[]const u8 = null,
    filter: ?[]MapEntry = null,
    default_outcome: ?DeliveryState = null,
    outcomes: ?[]const []const u8 = null,
    capabilities: ?[]const []const u8 = null,

    pub const amqp_descriptor = descriptor.source;
    pub const amqp_symbols: []const []const u8 = &.{"distribution_mode"};
};

// ── Target (§3.5.4) ─────────────────────────────────────────────────

pub const Target = struct {
    address: ?[]const u8 = null,
    durable: TerminusDurability = .none,
    expiry_policy: TerminusExpiryPolicy = .session_end,
    timeout: u32 = 0,
    dynamic: bool = false,
    dynamic_node_properties: ?[]MapEntry = null,
    capabilities: ?[]const []const u8 = null,

    pub const amqp_descriptor = descriptor.target;
};

// ── SASL Performatives (§5.3) ───────────────────────────────────────

pub const SaslMechanisms = struct {
    sasl_server_mechanisms: []const []const u8,

    pub const amqp_descriptor = descriptor.sasl_mechanisms;
};

pub const SaslInit = struct {
    mechanism: []const u8,
    initial_response: ?[]const u8 = null,
    hostname: ?[]const u8 = null,

    pub const amqp_descriptor = descriptor.sasl_init;
    pub const amqp_symbols: []const []const u8 = &.{"mechanism"};
    pub const amqp_binaries: []const []const u8 = &.{"initial_response"};
};

pub const SaslChallenge = struct {
    challenge: []const u8,

    pub const amqp_descriptor = descriptor.sasl_challenge;
    pub const amqp_binaries: []const []const u8 = &.{"challenge"};
};

pub const SaslResponse = struct {
    response: []const u8,

    pub const amqp_descriptor = descriptor.sasl_response;
    pub const amqp_binaries: []const []const u8 = &.{"response"};
};

pub const SaslOutcome = struct {
    code: SaslCode,
    additional_data: ?[]const u8 = null,

    pub const amqp_descriptor = descriptor.sasl_outcome;
    pub const amqp_binaries: []const []const u8 = &.{"additional_data"};
};

// ── Performative Union ──────────────────────────────────────────────

pub const Performative = union(enum) {
    open: Open,
    begin: Begin,
    attach: Attach,
    flow: Flow,
    transfer: Transfer,
    disposition: Disposition,
    detach: Detach,
    end: End,
    close: Close,

    // SASL
    sasl_mechanisms: SaslMechanisms,
    sasl_init: SaslInit,
    sasl_challenge: SaslChallenge,
    sasl_response: SaslResponse,
    sasl_outcome: SaslOutcome,

    pub fn descriptorCode(self: Performative) u64 {
        return switch (self) {
            .open => descriptor.open,
            .begin => descriptor.begin,
            .attach => descriptor.attach,
            .flow => descriptor.flow,
            .transfer => descriptor.transfer,
            .disposition => descriptor.disposition,
            .detach => descriptor.detach,
            .end => descriptor.end,
            .close => descriptor.close,
            .sasl_mechanisms => descriptor.sasl_mechanisms,
            .sasl_init => descriptor.sasl_init,
            .sasl_challenge => descriptor.sasl_challenge,
            .sasl_response => descriptor.sasl_response,
            .sasl_outcome => descriptor.sasl_outcome,
        };
    }
};

// ── Message Sections (§3.2) ─────────────────────────────────────────

pub const Header = struct {
    durable: bool = false,
    priority: u8 = 4,
    ttl: ?u32 = null,
    first_acquirer: bool = false,
    delivery_count: u32 = 0,

    pub const amqp_descriptor = descriptor.header;
};

pub const Properties = struct {
    message_id: ?AmqpValue = null,
    user_id: ?[]const u8 = null,
    to: ?[]const u8 = null,
    subject: ?[]const u8 = null,
    reply_to: ?[]const u8 = null,
    correlation_id: ?AmqpValue = null,
    content_type: ?[]const u8 = null,
    content_encoding: ?[]const u8 = null,
    absolute_expiry_time: ?i64 = null,
    creation_time: ?i64 = null,
    group_id: ?[]const u8 = null,
    group_sequence: ?u32 = null,
    reply_to_group_id: ?[]const u8 = null,

    pub const amqp_descriptor = descriptor.properties;
    pub const amqp_symbols: []const []const u8 = &.{ "content_type", "content_encoding" };
    pub const amqp_binaries: []const []const u8 = &.{"user_id"};
    pub const amqp_timestamps: []const []const u8 = &.{ "absolute_expiry_time", "creation_time" };
};

// ── Tests ──────────────────────────────────────────────────────────────

test "descriptor codes" {
    try std.testing.expectEqual(@as(u64, 0x10), descriptor.open);
    try std.testing.expectEqual(@as(u64, 0x11), descriptor.begin);
    try std.testing.expectEqual(@as(u64, 0x12), descriptor.attach);
    try std.testing.expectEqual(@as(u64, 0x13), descriptor.flow);
    try std.testing.expectEqual(@as(u64, 0x14), descriptor.transfer);
    try std.testing.expectEqual(@as(u64, 0x18), descriptor.close);
}

test "Role conversion" {
    try std.testing.expect(Role.sender.toBool() == false);
    try std.testing.expect(Role.receiver.toBool() == true);
    try std.testing.expectEqual(Role.sender, Role.fromBool(false));
    try std.testing.expectEqual(Role.receiver, Role.fromBool(true));
}

test "Performative descriptor codes" {
    const open_perf = Performative{ .open = .{ .container_id = "test" } };
    try std.testing.expectEqual(@as(u64, 0x10), open_perf.descriptorCode());

    const close_perf = Performative{ .close = .{} };
    try std.testing.expectEqual(@as(u64, 0x18), close_perf.descriptorCode());
}

test "SaslCode values" {
    try std.testing.expectEqual(@as(u8, 0), @intFromEnum(SaslCode.ok));
    try std.testing.expectEqual(@as(u8, 1), @intFromEnum(SaslCode.auth));
}

test "TerminusExpiryPolicy symbol conversion" {
    try std.testing.expectEqualStrings("link-detach", TerminusExpiryPolicy.link_detach.toSymbol());
    try std.testing.expectEqualStrings("never", TerminusExpiryPolicy.never.toSymbol());
}
