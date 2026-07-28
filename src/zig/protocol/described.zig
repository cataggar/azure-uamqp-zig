///! Encoding and decoding of AMQP 1.0 described types (OASIS spec §1.3, §2.7).
///!
///! Every performative, every delivery state, and the source/target/error
///! composites are the same shape on the wire: a descriptor followed by a list
///! of fields, in declaration order, with trailing null fields omitted. That is
///! one rule, so it is written once here and driven by `@typeInfo` over the
///! structs in `definitions.zig` rather than repeated per type.
///!
///! What reflection gets right for free:
///!
///!   * **Field order** is declaration order, which is spec order.
///!   * **Mandatory fields** are the ones with no default. That matches the
///!     spec's `mandatory="true"` exactly, so a missing one is an error and a
///!     defaulted one is not, without a second list to keep in sync.
///!   * **Adding a performative** is adding a struct and an `amqp_descriptor`.
///!
///! What reflection cannot know, because Zig has one `[]const u8` where AMQP
///! has three types, is declared per struct:
///!
///!   * `amqp_symbols` — fields encoded as symbols rather than strings.
///!   * `amqp_binaries` — fields encoded as binary.
///!   * `amqp_timestamps` — `i64` fields encoded as timestamps rather than longs.
///!
///! `codegen/amqp_definitions.xml` is the reference these declarations are
///! checked against.
const std = @import("std");
const Allocator = std.mem.Allocator;

const amqp_value = @import("../types/amqp_value.zig");
const AmqpValue = amqp_value.AmqpValue;
const MapEntry = amqp_value.MapEntry;
const encoder = @import("../types/encoder.zig");
const decoder = @import("../types/decoder.zig");
const Buffer = encoder.Buffer;
const defs = @import("definitions.zig");

pub const Error = error{
    /// A field the spec marks mandatory was absent or null.
    MissingMandatoryField,
    /// A field held a type the spec does not allow there.
    UnexpectedFieldType,
    /// An integer field held a value too large for its declared width.
    FieldValueOutOfRange,
    /// The bytes are not a described type at all.
    NotDescribed,
    /// A described type whose descriptor this library does not know.
    UnknownDescriptor,
    /// A symbolic value outside the set the spec defines for that field.
    UnknownSymbolValue,
} || decoder.DecodeError || encoder.EncodeError;

/// How a field is written when the Zig type alone does not say.
const Kind = enum { auto, symbol, binary, timestamp };

// ── Encoding ───────────────────────────────────────────────────────────

/// Encode any described type — a performative, a source, an error — into `buf`.
///
/// The allocator is scratch: it holds the intermediate value tree and is
/// released before returning, so nothing it hands out escapes.
pub fn encode(allocator: Allocator, value: anytype, buf: *Buffer) Error!void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const tree = try toValue(arena.allocator(), @TypeOf(value), .auto, value);
    try encoder.encode(tree, buf);
}

/// Encode a performative, dispatching on which one it is.
pub fn encodePerformative(allocator: Allocator, perf: defs.Performative, buf: *Buffer) Error!void {
    switch (perf) {
        inline else => |body| try encode(allocator, body, buf),
    }
}

/// Build the described list for one struct: descriptor, then fields in
/// declaration order with the trailing defaulted ones dropped.
fn toDescribed(arena: Allocator, comptime T: type, value: T) Error!AmqpValue {
    const fields = @typeInfo(T).@"struct".fields;
    const items = try arena.alloc(AmqpValue, fields.len);

    var count: usize = 0;
    inline for (fields, 0..) |field, i| {
        const kind = comptime kindOf(T, field.name);
        const field_value = @field(value, field.name);
        items[i] = if (isDefault(field, field_value))
            .null
        else
            try toValue(arena, field.type, kind, field_value);
        // A field is only worth sending if something after it is, so remember
        // where the last meaningful one was.
        if (items[i] != .null) count = i + 1;
    }

    const descriptor_node = try arena.create(AmqpValue);
    descriptor_node.* = .{ .ulong = T.amqp_descriptor };
    const body = try arena.create(AmqpValue);
    body.* = .{ .list = items[0..count] };
    return .{ .described = .{ .descriptor = descriptor_node, .value = body } };
}

/// True when a field carries exactly the value the peer would assume anyway,
/// so it can be sent as null. Mandatory fields have no default and so are
/// never dropped.
fn isDefault(comptime field: std.builtin.Type.StructField, value: field.type) bool {
    const default = comptime field.defaultValue();
    if (default) |d| return std.meta.eql(value, d);
    return false;
}

fn toValue(arena: Allocator, comptime T: type, comptime kind: Kind, value: T) Error!AmqpValue {
    if (T == AmqpValue) return value;
    if (T == []MapEntry or T == []const MapEntry) return .{ .map = @constCast(value) };
    if (T == []const u8) return switch (kind) {
        .symbol => .{ .symbol = value },
        .binary => .{ .binary = value },
        else => .{ .string = value },
    };
    if (T == []const []const u8) {
        const items = try arena.alloc(AmqpValue, value.len);
        for (value, items) |symbol, *slot| slot.* = .{ .symbol = symbol };
        return .{ .array = items };
    }
    if (T == defs.DeliveryState) return deliveryStateToValue(arena, value);

    return switch (@typeInfo(T)) {
        .optional => if (value) |inner|
            try toValue(arena, @TypeOf(inner), kind, inner)
        else
            .null,
        .bool => .{ .boolean = value },
        .int => intToValue(T, kind, value),
        .@"enum" => enumToValue(T, value),
        .@"struct" => try toDescribed(arena, T, value),
        else => @compileError("no AMQP encoding for " ++ @typeName(T)),
    };
}

fn intToValue(comptime T: type, comptime kind: Kind, value: T) AmqpValue {
    return switch (T) {
        u8 => .{ .ubyte = value },
        u16 => .{ .ushort = value },
        u32 => .{ .uint = value },
        u64 => .{ .ulong = value },
        i64 => if (kind == .timestamp) .{ .timestamp = value } else .{ .long = value },
        else => @compileError("no AMQP encoding for " ++ @typeName(T)),
    };
}

/// Enums say how they cross the wire by which conversion they provide: a
/// `toBool` means the spec restricts it to a boolean, a `toSymbol` means a
/// symbol, and anything else rides on its integer tag.
fn enumToValue(comptime T: type, value: T) AmqpValue {
    if (@hasDecl(T, "toBool")) return .{ .boolean = value.toBool() };
    if (@hasDecl(T, "toSymbol")) return .{ .symbol = value.toSymbol() };
    return intToValue(@typeInfo(T).@"enum".tag_type, .auto, @intFromEnum(value));
}

fn deliveryStateToValue(arena: Allocator, state: defs.DeliveryState) Error!AmqpValue {
    return switch (state) {
        .received => |v| try toDescribed(arena, defs.Received, v),
        .rejected => |v| try toDescribed(arena, defs.Rejected, v),
        .modified => |v| try toDescribed(arena, defs.Modified, v),
        // Accepted and released carry no fields, but still need the empty list
        // a described type is defined to have.
        .accepted => try emptyDescribed(arena, defs.descriptor.accepted),
        .released => try emptyDescribed(arena, defs.descriptor.released),
    };
}

fn emptyDescribed(arena: Allocator, code: u64) Error!AmqpValue {
    const descriptor_node = try arena.create(AmqpValue);
    descriptor_node.* = .{ .ulong = code };
    const body = try arena.create(AmqpValue);
    body.* = .{ .list = &.{} };
    return .{ .described = .{ .descriptor = descriptor_node, .value = body } };
}

// ── Decoding ───────────────────────────────────────────────────────────

/// A decoded described type and the memory it points into.
///
/// Every slice the value holds — strings, symbol arrays, maps — lives in the
/// arena, so the whole thing is released in one call and the value is valid
/// until then.
pub fn Decoded(comptime T: type) type {
    return struct {
        value: T,
        bytes_consumed: usize,
        arena: std.heap.ArenaAllocator,

        pub fn deinit(self: *@This()) void {
            self.arena.deinit();
        }
    };
}

/// Decode one described type of a known shape.
pub fn decode(comptime T: type, allocator: Allocator, data: []const u8) Error!Decoded(T) {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();

    const result = try decoder.decode(arena.allocator(), data);
    if (result.value != .described) return error.NotDescribed;
    const code = try descriptorCode(result.value.described);
    if (code != T.amqp_descriptor) return error.UnknownDescriptor;

    return .{
        .value = try fromDescribed(arena.allocator(), T, result.value.described.value.*),
        .bytes_consumed = result.bytes_consumed,
        .arena = arena,
    };
}

/// Decode whichever performative the bytes describe.
pub fn decodePerformative(allocator: Allocator, data: []const u8) Error!Decoded(defs.Performative) {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();

    const result = try decoder.decode(arena.allocator(), data);
    if (result.value != .described) return error.NotDescribed;
    const code = try descriptorCode(result.value.described);

    inline for (@typeInfo(defs.Performative).@"union".fields) |field| {
        if (code == field.type.amqp_descriptor) {
            const body = try fromDescribed(arena.allocator(), field.type, result.value.described.value.*);
            return .{
                .value = @unionInit(defs.Performative, field.name, body),
                .bytes_consumed = result.bytes_consumed,
                .arena = arena,
            };
        }
    }
    return error.UnknownDescriptor;
}

fn descriptorCode(described: amqp_value.Described) Error!u64 {
    return switch (described.descriptor.*) {
        .ulong => |v| v,
        // Symbolic descriptors are legal but name the same types; this library
        // only writes and recognises the numeric form.
        else => error.UnknownDescriptor,
    };
}

fn fromDescribed(arena: Allocator, comptime T: type, body: AmqpValue) Error!T {
    const items: []const AmqpValue = switch (body) {
        .list => |l| l,
        // A described type with no fields may be written as a null body.
        .null => &.{},
        else => return error.UnexpectedFieldType,
    };

    var result: T = undefined;
    inline for (@typeInfo(T).@"struct".fields, 0..) |field, i| {
        const kind = comptime kindOf(T, field.name);
        // Absent, or present and null, both mean "the default" — and for a
        // field with no default, both mean the frame is malformed.
        const item: AmqpValue = if (i < items.len) items[i] else .null;
        @field(result, field.name) = if (item == .null) blk: {
            const default = comptime field.defaultValue();
            break :blk default orelse return error.MissingMandatoryField;
        } else try fromValue(arena, field.type, kind, item);
    }
    // Fields beyond the ones this version knows are ignored, which is how the
    // spec says to stay compatible with a later peer.
    return result;
}

fn fromValue(arena: Allocator, comptime T: type, comptime kind: Kind, value: AmqpValue) Error!T {
    if (T == AmqpValue) return value;
    if (T == []MapEntry or T == []const MapEntry) return switch (value) {
        .map => |m| m,
        else => error.UnexpectedFieldType,
    };
    if (T == []const u8) return switch (value) {
        // The three string-ish types are accepted interchangeably: a peer that
        // sends a string where the spec says symbol is understood, not dropped.
        .string, .symbol, .binary => |v| v,
        else => error.UnexpectedFieldType,
    };
    if (T == []const []const u8) return multipleFromValue(arena, value);
    if (T == defs.DeliveryState) return deliveryStateFromValue(arena, value);

    return switch (@typeInfo(T)) {
        .optional => |o| try fromValue(arena, o.child, kind, value),
        .bool => switch (value) {
            .boolean => |v| v,
            else => error.UnexpectedFieldType,
        },
        .int => try intFromValue(T, value),
        .@"enum" => try enumFromValue(T, value),
        .@"struct" => switch (value) {
            .described => |d| if (try descriptorCode(d) == T.amqp_descriptor)
                try fromDescribed(arena, T, d.value.*)
            else
                error.UnknownDescriptor,
            else => error.UnexpectedFieldType,
        },
        else => @compileError("no AMQP decoding for " ++ @typeName(T)),
    };
}

/// A `multiple` field is one symbol or an array of them; some peers send a
/// list instead of an array, so take either.
fn multipleFromValue(arena: Allocator, value: AmqpValue) Error![]const []const u8 {
    const items: []const AmqpValue = switch (value) {
        .array, .list => |items| items,
        .symbol, .string => |s| return try arena.dupe([]const u8, &.{s}),
        else => return error.UnexpectedFieldType,
    };
    const out = try arena.alloc([]const u8, items.len);
    for (items, out) |item, *slot| {
        slot.* = switch (item) {
            .symbol, .string => |s| s,
            else => return error.UnexpectedFieldType,
        };
    }
    return out;
}

/// Accept any integer encoding whose value fits the field, so a peer that
/// picks a narrower form than expected is still understood.
fn intFromValue(comptime T: type, value: AmqpValue) Error!T {
    const wide: i128 = switch (value) {
        .ubyte => |v| v,
        .ushort => |v| v,
        .uint => |v| v,
        .ulong => |v| v,
        .byte => |v| v,
        .short => |v| v,
        .int => |v| v,
        .long, .timestamp => |v| v,
        else => return error.UnexpectedFieldType,
    };
    if (wide < std.math.minInt(T) or wide > std.math.maxInt(T)) return error.FieldValueOutOfRange;
    return @intCast(wide);
}

fn enumFromValue(comptime T: type, value: AmqpValue) Error!T {
    if (@hasDecl(T, "fromBool")) return switch (value) {
        .boolean => |v| T.fromBool(v),
        else => error.UnexpectedFieldType,
    };
    if (@hasDecl(T, "fromSymbol")) return switch (value) {
        .symbol, .string => |v| T.fromSymbol(v) orelse error.UnknownSymbolValue,
        else => error.UnexpectedFieldType,
    };
    const tag = try intFromValue(@typeInfo(T).@"enum".tag_type, value);
    return std.enums.fromInt(T, tag) orelse error.UnknownSymbolValue;
}

fn deliveryStateFromValue(arena: Allocator, value: AmqpValue) Error!defs.DeliveryState {
    if (value != .described) return error.UnexpectedFieldType;
    const d = value.described;
    return switch (try descriptorCode(d)) {
        defs.descriptor.received => .{ .received = try fromDescribed(arena, defs.Received, d.value.*) },
        defs.descriptor.accepted => .accepted,
        defs.descriptor.rejected => .{ .rejected = try fromDescribed(arena, defs.Rejected, d.value.*) },
        defs.descriptor.released => .released,
        defs.descriptor.modified => .{ .modified = try fromDescribed(arena, defs.Modified, d.value.*) },
        else => error.UnknownDescriptor,
    };
}

// ── Field kinds ────────────────────────────────────────────────────────

fn kindOf(comptime T: type, comptime name: []const u8) Kind {
    if (@hasDecl(T, "amqp_symbols") and nameIn(T.amqp_symbols, name)) return .symbol;
    if (@hasDecl(T, "amqp_binaries") and nameIn(T.amqp_binaries, name)) return .binary;
    if (@hasDecl(T, "amqp_timestamps") and nameIn(T.amqp_timestamps, name)) return .timestamp;
    return .auto;
}

fn nameIn(comptime names: []const []const u8, comptime name: []const u8) bool {
    for (names) |candidate| {
        if (std.mem.eql(u8, candidate, name)) return true;
    }
    return false;
}

// ── Tests ──────────────────────────────────────────────────────────────

const testing = std.testing;

fn encodeToOwned(value: anytype) ![]u8 {
    var buf = Buffer.initDynamic(testing.allocator);
    defer buf.deinit();
    try encode(testing.allocator, value, &buf);
    return testing.allocator.dupe(u8, buf.written());
}

/// Encode, decode, and hand back both so a test can assert on either.
fn roundTrip(comptime T: type, value: T) !struct { bytes: []u8, decoded: Decoded(T) } {
    const bytes = try encodeToOwned(value);
    errdefer testing.allocator.free(bytes);
    var decoded = try decode(T, testing.allocator, bytes);
    errdefer decoded.deinit();
    try testing.expectEqual(bytes.len, decoded.bytes_consumed);
    return .{ .bytes = bytes, .decoded = decoded };
}

test "a minimal Open is a descriptor and one field" {
    const bytes = try encodeToOwned(defs.Open{ .container_id = "c1" });
    defer testing.allocator.free(bytes);

    try testing.expectEqualSlices(u8, &.{
        0x00, 0x53, 0x10, // described, smallulong descriptor 0x10
        0xc0, 0x05, 0x01, // list8, 5 bytes, 1 field
        0xa1, 0x02, 'c', '1', // str8 "c1"
    }, bytes);
}

test "fields that carry their default are not sent" {
    // max-frame-size, channel-max and the capability lists are all at their
    // spec defaults, so only container-id survives.
    const bytes = try encodeToOwned(defs.Open{
        .container_id = "c1",
        .max_frame_size = 4294967295,
        .channel_max = 65535,
    });
    defer testing.allocator.free(bytes);
    try testing.expectEqual(@as(usize, 10), bytes.len);

    // A field before a non-default one still has to be sent, as null.
    const with_gap = try encodeToOwned(defs.Open{ .container_id = "c1", .channel_max = 7 });
    defer testing.allocator.free(with_gap);
    try testing.expectEqualSlices(u8, &.{
        0x00, 0x53, 0x10,
        0xc0, 0x0a, 0x04,
        0xa1, 0x02, 'c',
        '1',
        0x40, // hostname: null
        0x40, // max-frame-size: null, meaning the default
        0x60, 0x00, 0x07, // channel-max: 7
    }, with_gap);
}

test "Open round trips with every field populated" {
    var info = [_]MapEntry{.{ .key = .{ .symbol = "product" }, .value = .{ .string = "zig" } }};
    const original = defs.Open{
        .container_id = "container",
        .hostname = "example.servicebus.windows.net",
        .max_frame_size = 65536,
        .channel_max = 256,
        .idle_time_out = 30000,
        .outgoing_locales = &.{"en-US"},
        .incoming_locales = &.{ "en-US", "fr-FR" },
        .offered_capabilities = &.{"sole-connection-for-container"},
        .desired_capabilities = &.{},
        .properties = &info,
    };

    var rt = try roundTrip(defs.Open, original);
    defer testing.allocator.free(rt.bytes);
    defer rt.decoded.deinit();
    const got = rt.decoded.value;

    try testing.expectEqualStrings(original.container_id, got.container_id);
    try testing.expectEqualStrings(original.hostname.?, got.hostname.?);
    try testing.expectEqual(original.max_frame_size, got.max_frame_size);
    try testing.expectEqual(original.channel_max, got.channel_max);
    try testing.expectEqual(original.idle_time_out, got.idle_time_out);
    try testing.expectEqualStrings("en-US", got.outgoing_locales.?[0]);
    try testing.expectEqual(@as(usize, 2), got.incoming_locales.?.len);
    try testing.expectEqualStrings("fr-FR", got.incoming_locales.?[1]);
    try testing.expectEqualStrings("sole-connection-for-container", got.offered_capabilities.?[0]);
    try testing.expectEqual(@as(usize, 0), got.desired_capabilities.?.len);
    try testing.expectEqualStrings("product", got.properties.?[0].key.symbol);
}

test "a string field is a string and a symbol field is a symbol" {
    // container-id is a string (0xa1); the capabilities are symbols (0xa3)
    // inside an array. Getting this wrong is invisible to a round trip
    // through this library and rejected by every other AMQP peer.
    const bytes = try encodeToOwned(defs.Open{
        .container_id = "c",
        .offered_capabilities = &.{"AB"},
    });
    defer testing.allocator.free(bytes);
    try testing.expectEqualSlices(u8, &.{
        0x00, 0x53, 0x10,
        0xc0, 0x11, 0x08,
        0xa1, 0x01, 'c', // container-id: str8
        0x40, 0x40, 0x40, 0x40, // hostname, max-frame-size, channel-max, idle-time-out
        0x40, 0x40, // outgoing-locales, incoming-locales
        0xe0, 0x05, 0x01, 0xa3, 0x02, 'A', 'B', // offered: array8 of sym8
    }, bytes);
}

test "a mandatory field that is absent is an error" {
    // An Open whose only field is null: container-id has no default, so there
    // is nothing to fall back to.
    const bytes = [_]u8{ 0x00, 0x53, 0x10, 0xc0, 0x02, 0x01, 0x40 };
    try testing.expectError(error.MissingMandatoryField, decode(defs.Open, testing.allocator, &bytes));

    // The same when the list simply stops short.
    const empty = [_]u8{ 0x00, 0x53, 0x10, 0x45 };
    try testing.expectError(error.MissingMandatoryField, decode(defs.Open, testing.allocator, &empty));

    // Flow's three windows are mandatory; handle is not.
    const flow_bytes = try encodeToOwned(defs.Flow{
        .incoming_window = 1,
        .next_outgoing_id = 2,
        .outgoing_window = 3,
    });
    defer testing.allocator.free(flow_bytes);
    var flow = try decode(defs.Flow, testing.allocator, flow_bytes);
    defer flow.deinit();
    try testing.expectEqual(@as(?u32, null), flow.value.handle);
    try testing.expectEqual(@as(u32, 3), flow.value.outgoing_window);
}

test "fields this version does not know are ignored" {
    // An Open with an eleventh field, as a later spec revision might send.
    // Everything up to it still decodes.
    var buf = Buffer.initDynamic(testing.allocator);
    defer buf.deinit();
    var items = [_]AmqpValue{
        .{ .string = "c1" }, .null, .null, .null, .null,
        .null,               .null, .null, .null, .null,
        .{ .uint = 99 },
    };
    var descriptor_node: AmqpValue = .{ .ulong = defs.descriptor.open };
    var body: AmqpValue = .{ .list = &items };
    try encoder.encode(.{ .described = .{ .descriptor = &descriptor_node, .value = &body } }, &buf);

    var decoded = try decode(defs.Open, testing.allocator, buf.written());
    defer decoded.deinit();
    try testing.expectEqualStrings("c1", decoded.value.container_id);
}

test "enums cross the wire in the form the spec names" {
    // role is a boolean, the settle modes are ubytes, expiry-policy a symbol.
    var rt = try roundTrip(defs.Attach, .{
        .name = "link",
        .handle = 3,
        .role = .receiver,
        .snd_settle_mode = .settled,
        .rcv_settle_mode = .second,
        .source = .{ .address = "queue", .expiry_policy = .connection_close },
        .target = .{ .address = "dest", .durable = .unsettled_state },
        .initial_delivery_count = 0,
        .max_message_size = 1048576,
    });
    defer testing.allocator.free(rt.bytes);
    defer rt.decoded.deinit();

    try testing.expect(std.mem.indexOf(u8, rt.bytes, "\x41") != null); // true, for receiver
    try testing.expect(std.mem.indexOf(u8, rt.bytes, "connection-close") != null);

    const got = rt.decoded.value;
    try testing.expectEqual(defs.Role.receiver, got.role);
    try testing.expectEqual(defs.SenderSettleMode.settled, got.snd_settle_mode);
    try testing.expectEqual(defs.ReceiverSettleMode.second, got.rcv_settle_mode);
    try testing.expectEqualStrings("queue", got.source.?.address.?);
    try testing.expectEqual(defs.TerminusExpiryPolicy.connection_close, got.source.?.expiry_policy);
    try testing.expectEqual(defs.TerminusDurability.unsettled_state, got.target.?.durable);
    try testing.expectEqual(@as(?u64, 1048576), got.max_message_size);
    // Not sent, so the spec default comes back.
    try testing.expectEqual(defs.TerminusExpiryPolicy.session_end, got.target.?.expiry_policy);
}

test "an unknown symbolic value is rejected, not silently mapped" {
    var rt_bytes = try encodeToOwned(defs.Source{ .expiry_policy = .never });
    defer testing.allocator.free(rt_bytes);
    // Replace "never" with a policy no version of the spec defines.
    const at = std.mem.indexOf(u8, rt_bytes, "never").?;
    @memcpy(rt_bytes[at..][0..5], "wrong");
    try testing.expectError(error.UnknownSymbolValue, decode(defs.Source, testing.allocator, rt_bytes));
}

test "Transfer carries a binary delivery tag and a delivery state" {
    var rt = try roundTrip(defs.Transfer, .{
        .handle = 1,
        .delivery_id = 42,
        .delivery_tag = "\x00\x01\x02",
        .message_format = 0,
        .settled = false,
        .more = true,
        .rcv_settle_mode = .first,
        .delivery_state = .{ .received = .{ .section_number = 1, .section_offset = 128 } },
        .batchable = true,
    });
    defer testing.allocator.free(rt.bytes);
    defer rt.decoded.deinit();

    // vbin8, not str8: a delivery tag is opaque bytes.
    try testing.expect(std.mem.indexOfScalar(u8, rt.bytes, 0xa0) != null);

    const got = rt.decoded.value;
    try testing.expectEqualSlices(u8, "\x00\x01\x02", got.delivery_tag.?);
    try testing.expectEqual(@as(?u32, 42), got.delivery_id);
    try testing.expect(got.more);
    try testing.expect(got.batchable);
    try testing.expect(!got.is_resume);
    try testing.expectEqual(@as(u32, 1), got.delivery_state.?.received.section_number);
    try testing.expectEqual(@as(u64, 128), got.delivery_state.?.received.section_offset);
}

test "every delivery state round trips, including the empty ones" {
    const states = [_]defs.DeliveryState{
        .accepted,
        .released,
        .{ .received = .{ .section_number = 2, .section_offset = 3 } },
        .{ .rejected = .{ .err = .{ .condition = "amqp:not-found", .description = "gone" } } },
        .{ .modified = .{ .delivery_failed = true, .undeliverable_here = false } },
    };
    for (states) |state| {
        var rt = try roundTrip(defs.Disposition, .{ .role = .sender, .first = 0, .delivery_state = state });
        defer testing.allocator.free(rt.bytes);
        defer rt.decoded.deinit();
        try testing.expectEqual(
            std.meta.activeTag(state),
            std.meta.activeTag(rt.decoded.value.delivery_state.?),
        );
    }
}

test "an error condition is a symbol and its description a string" {
    var rt = try roundTrip(defs.Close, .{
        .err = .{ .condition = "amqp:connection:forced", .description = "bye" },
    });
    defer testing.allocator.free(rt.bytes);
    defer rt.decoded.deinit();

    try testing.expect(std.mem.indexOf(u8, rt.bytes, "\xa3\x16amqp:connection:forced") != null);
    try testing.expect(std.mem.indexOf(u8, rt.bytes, "\xa1\x03bye") != null);
    try testing.expectEqualStrings("amqp:connection:forced", rt.decoded.value.err.?.condition);
    try testing.expectEqualStrings("bye", rt.decoded.value.err.?.description.?);
}

test "every performative decodes back to its own variant" {
    const performatives = [_]defs.Performative{
        .{ .open = .{ .container_id = "c" } },
        .{ .begin = .{ .next_outgoing_id = 0, .incoming_window = 1, .outgoing_window = 1 } },
        .{ .attach = .{ .name = "l", .handle = 0, .role = .sender } },
        .{ .flow = .{ .incoming_window = 1, .next_outgoing_id = 0, .outgoing_window = 1 } },
        .{ .transfer = .{ .handle = 0 } },
        .{ .disposition = .{ .role = .receiver, .first = 0 } },
        .{ .detach = .{ .handle = 0, .closed = true } },
        .{ .end = .{} },
        .{ .close = .{} },
        .{ .sasl_mechanisms = .{ .sasl_server_mechanisms = &.{ "PLAIN", "ANONYMOUS" } } },
        .{ .sasl_init = .{ .mechanism = "PLAIN", .initial_response = "\x00u\x00p" } },
        .{ .sasl_challenge = .{ .challenge = "\x01" } },
        .{ .sasl_response = .{ .response = "\x02" } },
        .{ .sasl_outcome = .{ .code = .auth, .additional_data = "no" } },
    };

    for (performatives) |perf| {
        var buf = Buffer.initDynamic(testing.allocator);
        defer buf.deinit();
        try encodePerformative(testing.allocator, perf, &buf);

        var decoded = try decodePerformative(testing.allocator, buf.written());
        defer decoded.deinit();
        try testing.expectEqual(std.meta.activeTag(perf), std.meta.activeTag(decoded.value));
        try testing.expectEqual(perf.descriptorCode(), decoded.value.descriptorCode());
        try testing.expectEqual(buf.written().len, decoded.bytes_consumed);
    }
}

test "SASL mechanisms decode whether sent as an array or a bare symbol" {
    // The spec's `multiple` allows a single value in place of an array, and
    // brokers use both.
    var single = Buffer.initDynamic(testing.allocator);
    defer single.deinit();
    var descriptor_node: AmqpValue = .{ .ulong = defs.descriptor.sasl_mechanisms };
    var items = [_]AmqpValue{.{ .symbol = "PLAIN" }};
    var body: AmqpValue = .{ .list = &items };
    try encoder.encode(.{ .described = .{ .descriptor = &descriptor_node, .value = &body } }, &single);

    var decoded = try decode(defs.SaslMechanisms, testing.allocator, single.written());
    defer decoded.deinit();
    try testing.expectEqual(@as(usize, 1), decoded.value.sasl_server_mechanisms.len);
    try testing.expectEqualStrings("PLAIN", decoded.value.sasl_server_mechanisms[0]);
}

test "message sections use the same machinery" {
    var rt = try roundTrip(defs.Header, .{ .durable = true, .priority = 9, .ttl = 60000 });
    defer testing.allocator.free(rt.bytes);
    defer rt.decoded.deinit();
    try testing.expect(rt.decoded.value.durable);
    try testing.expectEqual(@as(u8, 9), rt.decoded.value.priority);
    try testing.expectEqual(@as(?u32, 60000), rt.decoded.value.ttl);
    // Untouched, so it comes back as the spec default rather than zero.
    try testing.expectEqual(@as(u32, 0), rt.decoded.value.delivery_count);

    var props = try roundTrip(defs.Properties, .{
        .message_id = .{ .ulong = 7 },
        .user_id = "\xff\xfe",
        .to = "amqps://host/queue",
        .content_type = "application/json",
        .creation_time = 1700000000000,
        .group_sequence = 3,
    });
    defer testing.allocator.free(props.bytes);
    defer props.decoded.deinit();
    try testing.expectEqual(@as(u64, 7), props.decoded.value.message_id.?.ulong);
    try testing.expectEqualSlices(u8, "\xff\xfe", props.decoded.value.user_id.?);
    try testing.expectEqualStrings("application/json", props.decoded.value.content_type.?);
    try testing.expectEqual(@as(?i64, 1700000000000), props.decoded.value.creation_time);
    // A timestamp (0x83), not a long (0x81).
    try testing.expect(std.mem.indexOfScalar(u8, props.bytes, 0x83) != null);
}

test "a narrower integer encoding than expected is still understood" {
    // max-frame-size is a uint, but nothing stops a peer writing it small.
    var buf = Buffer.initDynamic(testing.allocator);
    defer buf.deinit();
    var descriptor_node: AmqpValue = .{ .ulong = defs.descriptor.open };
    var items = [_]AmqpValue{ .{ .string = "c" }, .null, .{ .ubyte = 200 } };
    var body: AmqpValue = .{ .list = &items };
    try encoder.encode(.{ .described = .{ .descriptor = &descriptor_node, .value = &body } }, &buf);

    var decoded = try decode(defs.Open, testing.allocator, buf.written());
    defer decoded.deinit();
    try testing.expectEqual(@as(u32, 200), decoded.value.max_frame_size);
}

test "an integer too large for its field is rejected" {
    var buf = Buffer.initDynamic(testing.allocator);
    defer buf.deinit();
    var descriptor_node: AmqpValue = .{ .ulong = defs.descriptor.open };
    // channel-max is a ushort; 70000 does not fit.
    var items = [_]AmqpValue{ .{ .string = "c" }, .null, .null, .{ .uint = 70000 } };
    var body: AmqpValue = .{ .list = &items };
    try encoder.encode(.{ .described = .{ .descriptor = &descriptor_node, .value = &body } }, &buf);

    try testing.expectError(error.FieldValueOutOfRange, decode(defs.Open, testing.allocator, buf.written()));
}

test "bytes that are not the expected described type are rejected" {
    const not_described = [_]u8{ 0xc0, 0x01, 0x00 }; // a bare empty list
    try testing.expectError(error.NotDescribed, decode(defs.Open, testing.allocator, &not_described));
    try testing.expectError(error.NotDescribed, decodePerformative(testing.allocator, &not_described));

    const begin_bytes = try encodeToOwned(defs.Begin{
        .next_outgoing_id = 0,
        .incoming_window = 1,
        .outgoing_window = 1,
    });
    defer testing.allocator.free(begin_bytes);
    try testing.expectError(error.UnknownDescriptor, decode(defs.Open, testing.allocator, begin_bytes));

    // A descriptor no performative claims.
    var buf = Buffer.initDynamic(testing.allocator);
    defer buf.deinit();
    var descriptor_node: AmqpValue = .{ .ulong = 0x7fff };
    var body: AmqpValue = .{ .list = &.{} };
    try encoder.encode(.{ .described = .{ .descriptor = &descriptor_node, .value = &body } }, &buf);
    try testing.expectError(error.UnknownDescriptor, decodePerformative(testing.allocator, buf.written()));
}

test "a field holding the wrong type is rejected" {
    var buf = Buffer.initDynamic(testing.allocator);
    defer buf.deinit();
    var descriptor_node: AmqpValue = .{ .ulong = defs.descriptor.begin };
    // remote-channel is a ushort, not a string.
    var items = [_]AmqpValue{.{ .string = "nope" }};
    var body: AmqpValue = .{ .list = &items };
    try encoder.encode(.{ .described = .{ .descriptor = &descriptor_node, .value = &body } }, &buf);

    try testing.expectError(error.UnexpectedFieldType, decode(defs.Begin, testing.allocator, buf.written()));
}

test "mandatory-ness comes from the absence of a default" {
    // The spec's mandatory fields and the fields with no Zig default are the
    // same set. If a struct gains a default the frame silently stops being
    // validated, so pin the ones that matter.
    inline for (.{
        .{ defs.Open, "container_id" },
        .{ defs.Attach, "name" },
        .{ defs.Attach, "handle" },
        .{ defs.Attach, "role" },
        .{ defs.Flow, "incoming_window" },
        .{ defs.Flow, "next_outgoing_id" },
        .{ defs.Flow, "outgoing_window" },
        .{ defs.Transfer, "handle" },
        .{ defs.Disposition, "role" },
        .{ defs.Disposition, "first" },
        .{ defs.Detach, "handle" },
        .{ defs.AmqpError, "condition" },
        .{ defs.Received, "section_number" },
        .{ defs.SaslMechanisms, "sasl_server_mechanisms" },
        .{ defs.SaslInit, "mechanism" },
        .{ defs.SaslOutcome, "code" },
    }) |entry| {
        const field = std.meta.fieldInfo(entry[0], @field(std.meta.FieldEnum(entry[0]), entry[1]));
        try testing.expect(field.defaultValue() == null);
    }
}
