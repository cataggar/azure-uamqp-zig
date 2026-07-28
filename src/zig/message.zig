///! AMQP 1.0 Message abstraction (OASIS spec §3.2)
///!
///! Combines header, delivery-annotations, message-annotations,
///! properties, application-properties, body, and footer into a
///! single message structure, and converts it to and from the sequence of
///! described sections that makes up a transfer's payload.
const std = @import("std");
const Allocator = std.mem.Allocator;
const defs = @import("protocol/definitions.zig");
const described = @import("protocol/described.zig");
const encoder = @import("types/encoder.zig");
const decoder = @import("types/decoder.zig");
const Buffer = encoder.Buffer;
const AmqpValue = @import("types/amqp_value.zig").AmqpValue;
const MapEntry = @import("types/amqp_value.zig").MapEntry;

pub const Error = error{
    BodyTypeMismatch,
    /// A section that is defined to appear at most once appeared twice.
    DuplicateSection,
    /// A section whose descriptor is not one of §3.2's.
    UnknownSection,
    /// A section whose body is not the type its descriptor calls for — a
    /// map for application-properties, binary for data, and so on.
    MalformedSection,
} || described.Error;

/// How the message body is encoded.
pub const BodyType = enum {
    none,
    data,
    sequence,
    value,
};

/// A single data section (binary payload).
pub const DataSection = struct {
    bytes: []const u8,
};

/// A complete AMQP message.
///
/// Everything the message allocates — data sections, the body value,
/// application properties added through the setters, and everything
/// `decode` produces — lives in its arena and is released by `deinit`.
/// Slices assigned straight to `header`, `properties` or the annotation
/// fields are borrowed and must outlive the message.
pub const Message = struct {
    arena: std.heap.ArenaAllocator,

    // §3.2.1 Header
    header: ?defs.Header = null,

    // §3.2.2 Delivery Annotations (map)
    delivery_annotations: ?[]MapEntry = null,

    // §3.2.3 Message Annotations (map)
    message_annotations: ?[]MapEntry = null,

    // §3.2.4 Properties
    properties: ?defs.Properties = null,

    // §3.2.5 Application Properties (map)
    application_properties: ?[]MapEntry = null,

    // Body — one of: data sections, sequence sections, or a single value
    body_type: BodyType = .none,
    body_data_sections: std.ArrayList(DataSection),
    body_sequence_sections: std.ArrayList([]AmqpValue),
    body_value: ?AmqpValue = null,

    // §3.2.9 Footer (map)
    footer: ?[]MapEntry = null,

    // Message format (default 0)
    message_format: u32 = 0,

    pub fn init(allocator: Allocator) Message {
        return .{
            .arena = std.heap.ArenaAllocator.init(allocator),
            .body_data_sections = .empty,
            .body_sequence_sections = .empty,
        };
    }

    pub fn deinit(self: *Message) void {
        self.arena.deinit();
    }

    /// The allocator everything the message owns comes from.
    ///
    /// Derived on each call rather than stored, because an `Allocator` points
    /// back at the arena it came from, and that address changes when the
    /// message is moved — `init` returning by value moves it once already.
    fn alloc(self: *Message) Allocator {
        return self.arena.allocator();
    }

    /// Add a binary data section to the message body.
    pub fn addBodyData(self: *Message, data: []const u8) !void {
        if (self.body_type != .none and self.body_type != .data) return error.BodyTypeMismatch;
        self.body_type = .data;
        const owned = try self.alloc().dupe(u8, data);
        try self.body_data_sections.append(self.alloc(), .{ .bytes = owned });
    }

    /// Add an amqp-sequence section to the message body.
    pub fn addBodySequence(self: *Message, values: []const AmqpValue) !void {
        if (self.body_type != .none and self.body_type != .sequence) return error.BodyTypeMismatch;
        self.body_type = .sequence;
        const allocator = self.alloc();
        const owned = try allocator.alloc(AmqpValue, values.len);
        for (values, owned) |value, *slot| slot.* = try value.clone(allocator);
        try self.body_sequence_sections.append(allocator, owned);
    }

    /// Set the message body to a single AMQP value.
    pub fn setBodyValue(self: *Message, value: AmqpValue) !void {
        if (self.body_type != .none) return error.BodyTypeMismatch;
        self.body_type = .value;
        self.body_value = try value.clone(self.alloc());
    }

    /// Set a string application property.
    pub fn setApplicationProperty(self: *Message, key: []const u8, value: []const u8) !void {
        return self.putApplicationProperty(key, .{ .string = value });
    }

    /// Set an application property of any type. A repeated key replaces the
    /// value already there, since a map may not carry the same key twice
    /// (§1.6.23) and the peer would be right to reject one that did.
    pub fn putApplicationProperty(self: *Message, key: []const u8, value: AmqpValue) !void {
        const allocator = self.alloc();
        const owned_value = try value.clone(allocator);
        const existing: []MapEntry = self.application_properties orelse &.{};

        for (existing) |*entry| {
            if (entry.key == .string and std.mem.eql(u8, entry.key.string, key)) {
                entry.value = owned_value;
                return;
            }
        }

        const grown = try allocator.alloc(MapEntry, existing.len + 1);
        @memcpy(grown[0..existing.len], existing);
        grown[existing.len] = .{
            .key = .{ .string = try allocator.dupe(u8, key) },
            .value = owned_value,
        };
        self.application_properties = grown;
    }

    /// The value of an application property, or null if the message carries
    /// none by that name.
    pub fn applicationProperty(self: *const Message, key: []const u8) ?AmqpValue {
        for (self.application_properties orelse &[_]MapEntry{}) |entry| {
            if (entry.key == .string and std.mem.eql(u8, entry.key.string, key)) return entry.value;
        }
        return null;
    }

    /// Get the total number of body data sections.
    pub fn bodyDataCount(self: *const Message) usize {
        return self.body_data_sections.items.len;
    }

    /// Clone the entire message.
    pub fn clone(self: *Message) !Message {
        var new_msg = Message.init(self.arena.child_allocator);
        errdefer new_msg.deinit();

        new_msg.header = self.header;
        new_msg.properties = self.properties;
        new_msg.message_format = self.message_format;

        for (self.body_data_sections.items) |section| try new_msg.addBodyData(section.bytes);
        for (self.body_sequence_sections.items) |seq| try new_msg.addBodySequence(seq);
        if (self.body_value) |v| try new_msg.setBodyValue(v);

        new_msg.delivery_annotations = try cloneMap(new_msg.alloc(), self.delivery_annotations);
        new_msg.message_annotations = try cloneMap(new_msg.alloc(), self.message_annotations);
        new_msg.application_properties = try cloneMap(new_msg.alloc(), self.application_properties);
        new_msg.footer = try cloneMap(new_msg.alloc(), self.footer);

        return new_msg;
    }

    // ── Wire format (§3.2) ────────────────────────────────────────────

    /// Write the message as the sequence of described sections that forms a
    /// transfer's payload, in the order §3.2 defines.
    pub fn encode(self: *const Message, buf: *Buffer) Error!void {
        var arena = std.heap.ArenaAllocator.init(self.arena.child_allocator);
        defer arena.deinit();

        if (self.header) |header| try described.encode(arena.allocator(), header, buf);
        if (self.delivery_annotations) |map| try encodeSection(buf, defs.descriptor.delivery_annotations, .{ .map = map });
        if (self.message_annotations) |map| try encodeSection(buf, defs.descriptor.message_annotations, .{ .map = map });
        if (self.properties) |properties| try described.encode(arena.allocator(), properties, buf);
        if (self.application_properties) |map| try encodeSection(buf, defs.descriptor.application_properties, .{ .map = map });

        switch (self.body_type) {
            .none => {},
            .data => for (self.body_data_sections.items) |section| {
                try encodeSection(buf, defs.descriptor.data, .{ .binary = section.bytes });
            },
            .sequence => for (self.body_sequence_sections.items) |seq| {
                try encodeSection(buf, defs.descriptor.amqp_sequence, .{ .list = seq });
            },
            .value => try encodeSection(buf, defs.descriptor.amqp_value, self.body_value orelse .null),
        }

        if (self.footer) |map| try encodeSection(buf, defs.descriptor.footer, .{ .map = map });
    }

    /// Read a transfer payload back into a message that owns every byte of
    /// it, so the caller is free to reuse the buffer it was handed — the one
    /// a link hands to `OnTransferReceived` is valid only during the call.
    ///
    /// Sections are taken in whatever order they arrive rather than the order
    /// §3.2 defines: insisting on the order gains a receiver nothing and
    /// rejects messages a lenient one accepts.
    pub fn decode(allocator: Allocator, bytes: []const u8) Error!Message {
        var msg = Message.init(allocator);
        errdefer msg.deinit();
        const arena = msg.alloc();

        var rest = bytes;
        while (rest.len > 0) {
            const result = try decoder.decode(arena, rest);
            rest = rest[result.bytes_consumed..];

            if (result.value != .described) return error.NotDescribed;
            const section = result.value.described;
            if (section.descriptor.* != .ulong) return error.MalformedSection;
            const body = section.value.*;

            switch (section.descriptor.ulong) {
                defs.descriptor.header => {
                    if (msg.header != null) return error.DuplicateSection;
                    msg.header = try described.fromBody(arena, defs.Header, body);
                },
                defs.descriptor.delivery_annotations => {
                    if (msg.delivery_annotations != null) return error.DuplicateSection;
                    msg.delivery_annotations = try asMap(body);
                },
                defs.descriptor.message_annotations => {
                    if (msg.message_annotations != null) return error.DuplicateSection;
                    msg.message_annotations = try asMap(body);
                },
                defs.descriptor.properties => {
                    if (msg.properties != null) return error.DuplicateSection;
                    msg.properties = try described.fromBody(arena, defs.Properties, body);
                },
                defs.descriptor.application_properties => {
                    if (msg.application_properties != null) return error.DuplicateSection;
                    msg.application_properties = try asMap(body);
                },
                defs.descriptor.data => {
                    if (msg.body_type != .none and msg.body_type != .data) return error.BodyTypeMismatch;
                    if (body != .binary) return error.MalformedSection;
                    msg.body_type = .data;
                    try msg.body_data_sections.append(arena, .{ .bytes = body.binary });
                },
                defs.descriptor.amqp_sequence => {
                    if (msg.body_type != .none and msg.body_type != .sequence) return error.BodyTypeMismatch;
                    if (body != .list) return error.MalformedSection;
                    msg.body_type = .sequence;
                    try msg.body_sequence_sections.append(arena, body.list);
                },
                defs.descriptor.amqp_value => {
                    if (msg.body_type != .none) return error.BodyTypeMismatch;
                    msg.body_type = .value;
                    msg.body_value = body;
                },
                defs.descriptor.footer => {
                    if (msg.footer != null) return error.DuplicateSection;
                    msg.footer = try asMap(body);
                },
                else => return error.UnknownSection,
            }
        }

        return msg;
    }
};

fn encodeSection(buf: *Buffer, code: u64, body: AmqpValue) Error!void {
    var descriptor_value: AmqpValue = .{ .ulong = code };
    var body_value = body;
    try encoder.encode(.{ .described = .{
        .descriptor = &descriptor_value,
        .value = &body_value,
    } }, buf);
}

fn asMap(value: AmqpValue) Error![]MapEntry {
    if (value != .map) return error.MalformedSection;
    return value.map;
}

fn cloneMap(allocator: Allocator, map: ?[]MapEntry) !?[]MapEntry {
    const entries = map orelse return null;
    const copy = try allocator.alloc(MapEntry, entries.len);
    for (entries, copy) |entry, *slot| slot.* = .{
        .key = try entry.key.clone(allocator),
        .value = try entry.value.clone(allocator),
    };
    return copy;
}

// ── Tests ──────────────────────────────────────────────────────────────

const testing = std.testing;

test "Message create and add body data" {
    const allocator = testing.allocator;

    var msg = Message.init(allocator);
    defer msg.deinit();

    try msg.addBodyData("hello world");
    try testing.expectEqual(@as(usize, 1), msg.bodyDataCount());
    try testing.expectEqual(BodyType.data, msg.body_type);

    try msg.addBodyData("second section");
    try testing.expectEqual(@as(usize, 2), msg.bodyDataCount());
}

test "Message set body value" {
    const allocator = testing.allocator;

    var msg = Message.init(allocator);
    defer msg.deinit();

    try msg.setBodyValue(.{ .string = "test value" });
    try testing.expectEqual(BodyType.value, msg.body_type);
    try testing.expect(msg.body_value != null);
}

test "Message body type mismatch" {
    const allocator = testing.allocator;

    var msg = Message.init(allocator);
    defer msg.deinit();

    try msg.addBodyData("data");
    try testing.expectError(error.BodyTypeMismatch, msg.setBodyValue(.null));
}

test "Message with header and properties" {
    const allocator = testing.allocator;

    var msg = Message.init(allocator);
    defer msg.deinit();

    msg.header = .{
        .durable = true,
        .priority = 7,
        .ttl = 60000,
    };
    msg.properties = .{
        .subject = "test-subject",
        .content_type = "application/json",
    };

    try testing.expect(msg.header.?.durable);
    try testing.expectEqual(@as(u8, 7), msg.header.?.priority);
    try testing.expectEqualStrings("test-subject", msg.properties.?.subject.?);
}

test "Message clone" {
    const allocator = testing.allocator;

    var original = Message.init(allocator);
    defer original.deinit();

    original.header = .{ .durable = true };
    try original.addBodyData("payload");
    try original.setApplicationProperty("operation", "put-token");

    var cloned = try original.clone();
    defer cloned.deinit();

    try testing.expect(cloned.header.?.durable);
    try testing.expectEqual(@as(usize, 1), cloned.bodyDataCount());
    try testing.expectEqualStrings("put-token", cloned.applicationProperty("operation").?.string);
}

test "an application property set twice keeps one entry" {
    const allocator = testing.allocator;
    var msg = Message.init(allocator);
    defer msg.deinit();

    try msg.setApplicationProperty("operation", "put-token");
    try msg.setApplicationProperty("operation", "read");
    try msg.putApplicationProperty("status-code", .{ .int = 200 });

    try testing.expectEqual(@as(usize, 2), msg.application_properties.?.len);
    try testing.expectEqualStrings("read", msg.applicationProperty("operation").?.string);
    try testing.expectEqual(@as(i32, 200), msg.applicationProperty("status-code").?.int);
    try testing.expect(msg.applicationProperty("absent") == null);
}

test "a message round-trips through its sections" {
    const allocator = testing.allocator;

    var msg = Message.init(allocator);
    defer msg.deinit();
    msg.header = .{ .durable = true, .priority = 7, .ttl = 60_000 };
    msg.properties = .{
        .message_id = .{ .ulong = 42 },
        .to = "$cbs",
        .subject = "put-token",
        .reply_to = "cbs-replies",
        .content_type = "application/octet-stream",
        .user_id = "who",
        .creation_time = 1_700_000_000_000,
    };
    try msg.setApplicationProperty("operation", "put-token");
    try msg.putApplicationProperty("status-code", .{ .int = 202 });
    try msg.addBodyData("first");
    try msg.addBodyData("second");

    var buf = Buffer.initDynamic(allocator);
    defer buf.deinit();
    try msg.encode(&buf);

    var back = try Message.decode(allocator, buf.written());
    defer back.deinit();

    try testing.expect(back.header.?.durable);
    try testing.expectEqual(@as(u8, 7), back.header.?.priority);
    try testing.expectEqual(@as(?u32, 60_000), back.header.?.ttl);
    try testing.expectEqual(@as(u64, 42), back.properties.?.message_id.?.ulong);
    try testing.expectEqualStrings("$cbs", back.properties.?.to.?);
    try testing.expectEqualStrings("put-token", back.properties.?.subject.?);
    try testing.expectEqualStrings("cbs-replies", back.properties.?.reply_to.?);
    try testing.expectEqualStrings("application/octet-stream", back.properties.?.content_type.?);
    try testing.expectEqualStrings("who", back.properties.?.user_id.?);
    try testing.expectEqual(@as(?i64, 1_700_000_000_000), back.properties.?.creation_time);
    try testing.expectEqualStrings("put-token", back.applicationProperty("operation").?.string);
    try testing.expectEqual(@as(i32, 202), back.applicationProperty("status-code").?.int);
    try testing.expectEqual(BodyType.data, back.body_type);
    try testing.expectEqual(@as(usize, 2), back.bodyDataCount());
    try testing.expectEqualStrings("first", back.body_data_sections.items[0].bytes);
    try testing.expectEqualStrings("second", back.body_data_sections.items[1].bytes);
}

test "a decoded message owns its bytes" {
    const allocator = testing.allocator;

    var buf = Buffer.initDynamic(allocator);
    defer buf.deinit();
    {
        var msg = Message.init(allocator);
        defer msg.deinit();
        try msg.setApplicationProperty("operation", "put-token");
        try msg.setBodyValue(.{ .string = "body" });
        try msg.encode(&buf);
    }

    const scratch = try allocator.dupe(u8, buf.written());
    var back = try Message.decode(allocator, scratch);
    defer back.deinit();

    // The payload a link hands over is valid only during the callback, so a
    // message pointing into it would be read after free.
    @memset(scratch, 0xaa);
    allocator.free(scratch);

    try testing.expectEqualStrings("put-token", back.applicationProperty("operation").?.string);
    try testing.expectEqualStrings("body", back.body_value.?.string);
}

test "an empty body encodes as no section at all" {
    const allocator = testing.allocator;
    var msg = Message.init(allocator);
    defer msg.deinit();
    msg.properties = .{ .subject = "empty" };

    var buf = Buffer.initDynamic(allocator);
    defer buf.deinit();
    try msg.encode(&buf);

    var back = try Message.decode(allocator, buf.written());
    defer back.deinit();
    try testing.expectEqual(BodyType.none, back.body_type);
    try testing.expectEqualStrings("empty", back.properties.?.subject.?);
}

test "annotations and a footer survive the wire" {
    const allocator = testing.allocator;
    var msg = Message.init(allocator);
    defer msg.deinit();

    var annotations = [_]MapEntry{.{ .key = .{ .symbol = "x-opt-partition-key" }, .value = .{ .string = "p1" } }};
    var footer = [_]MapEntry{.{ .key = .{ .symbol = "x-opt-crc" }, .value = .{ .uint = 7 } }};
    msg.message_annotations = &annotations;
    msg.delivery_annotations = &annotations;
    msg.footer = &footer;
    try msg.setBodyValue(.{ .string = "body" });

    var buf = Buffer.initDynamic(allocator);
    defer buf.deinit();
    try msg.encode(&buf);

    var back = try Message.decode(allocator, buf.written());
    defer back.deinit();
    try testing.expectEqualStrings("p1", back.message_annotations.?[0].value.string);
    try testing.expectEqualStrings("p1", back.delivery_annotations.?[0].value.string);
    try testing.expectEqual(@as(u32, 7), back.footer.?[0].value.uint);
    try testing.expectEqualStrings("body", back.body_value.?.string);
}

test "amqp-sequence sections round-trip" {
    const allocator = testing.allocator;
    var msg = Message.init(allocator);
    defer msg.deinit();

    try msg.addBodySequence(&.{ .{ .int = 1 }, .{ .string = "two" } });
    try msg.addBodySequence(&.{.{ .boolean = true }});

    var buf = Buffer.initDynamic(allocator);
    defer buf.deinit();
    try msg.encode(&buf);

    var back = try Message.decode(allocator, buf.written());
    defer back.deinit();
    try testing.expectEqual(BodyType.sequence, back.body_type);
    try testing.expectEqual(@as(usize, 2), back.body_sequence_sections.items.len);
    try testing.expectEqualStrings("two", back.body_sequence_sections.items[0][1].string);
    try testing.expect(back.body_sequence_sections.items[1][0].boolean);
}

test "malformed sections are rejected rather than guessed at" {
    const allocator = testing.allocator;

    // A descriptor §3.2 does not define.
    {
        var buf = Buffer.initDynamic(allocator);
        defer buf.deinit();
        try encodeSection(&buf, 0x7f, .{ .string = "?" });
        try testing.expectError(error.UnknownSection, Message.decode(allocator, buf.written()));
    }

    // Application-properties whose body is not a map.
    {
        var buf = Buffer.initDynamic(allocator);
        defer buf.deinit();
        try encodeSection(&buf, defs.descriptor.application_properties, .{ .string = "not a map" });
        try testing.expectError(error.MalformedSection, Message.decode(allocator, buf.written()));
    }

    // Body sections of kinds that cannot coexist.
    {
        var buf = Buffer.initDynamic(allocator);
        defer buf.deinit();
        try encodeSection(&buf, defs.descriptor.amqp_value, .{ .string = "v" });
        try encodeSection(&buf, defs.descriptor.data, .{ .binary = "d" });
        try testing.expectError(error.BodyTypeMismatch, Message.decode(allocator, buf.written()));
    }

    // The same at-most-once section twice.
    {
        var buf = Buffer.initDynamic(allocator);
        defer buf.deinit();
        try encodeSection(&buf, defs.descriptor.application_properties, .{ .map = &.{} });
        try encodeSection(&buf, defs.descriptor.application_properties, .{ .map = &.{} });
        try testing.expectError(error.DuplicateSection, Message.decode(allocator, buf.written()));
    }

    // Bytes that are not a described section at all.
    try testing.expectError(error.NotDescribed, Message.decode(allocator, &.{0x41}));
}
