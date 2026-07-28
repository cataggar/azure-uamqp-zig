//! Rendering an `AmqpValue` as text, for logs and diagnostics.
//!
//! Replaces amqpvalue_to_string.c. The syntax is meant to be read by a human
//! debugging a trace, and distinguishes the types a wire dump otherwise
//! blurs together: a symbol from a string, and — the distinction issue #2
//! was about — an array from a list.
//!
//! ```text
//! null            null
//! boolean         true / false
//! integers        decimal, unsigned and signed alike
//! float, double   decimal, `nan` / `inf` as the formatter renders them
//! char            'a', or U+00A0 when not printable
//! timestamp       timestamp(1571233434.152)
//! uuid            urn:uuid:00112233-4455-6677-8899-aabbccddeeff
//! binary          b"00ff1a"
//! string          "text"
//! symbol          :symbol
//! list            [1, "two"]
//! array           @[1, 2]
//! map             {"key": 1}
//! described       :amqp:accepted:list([])
//! ```
const std = @import("std");
const Allocator = std.mem.Allocator;
const Writer = std.Io.Writer;

const value_mod = @import("amqp_value.zig");
const AmqpValue = value_mod.AmqpValue;

/// Write `value` as text.
pub fn write(value: AmqpValue, w: *Writer) Writer.Error!void {
    switch (value) {
        .null => try w.writeAll("null"),
        .boolean => |b| try w.writeAll(if (b) "true" else "false"),
        .ubyte => |v| try w.print("{d}", .{v}),
        .ushort => |v| try w.print("{d}", .{v}),
        .uint => |v| try w.print("{d}", .{v}),
        .ulong => |v| try w.print("{d}", .{v}),
        .byte => |v| try w.print("{d}", .{v}),
        .short => |v| try w.print("{d}", .{v}),
        .int => |v| try w.print("{d}", .{v}),
        .long => |v| try w.print("{d}", .{v}),
        .float => |v| try w.print("{d}", .{v}),
        .double => |v| try w.print("{d}", .{v}),
        .char => |c| try writeChar(c, w),
        .timestamp => |ms| try w.print("timestamp({d})", .{ms}),
        .uuid => |u| try writeUuid(u, w),
        .binary => |bytes| {
            try w.writeAll("b\"");
            for (bytes) |byte| try w.print("{x:0>2}", .{byte});
            try w.writeAll("\"");
        },
        .string => |s| try writeQuoted(s, w),
        .symbol => |s| try w.print(":{s}", .{s}),
        .list => |items| try writeItems(items, "[", "]", w),
        .array => |items| try writeItems(items, "@[", "]", w),
        .map => |entries| {
            try w.writeAll("{");
            for (entries, 0..) |entry, i| {
                if (i > 0) try w.writeAll(", ");
                try write(entry.key, w);
                try w.writeAll(": ");
                try write(entry.value, w);
            }
            try w.writeAll("}");
        },
        .described => |d| {
            try write(d.descriptor.*, w);
            try w.writeAll("(");
            try write(d.value.*, w);
            try w.writeAll(")");
        },
    }
}

/// Render `value` into a string owned by the caller.
pub fn toOwned(allocator: Allocator, value: AmqpValue) Allocator.Error![]u8 {
    var aw = Writer.Allocating.init(allocator);
    errdefer aw.deinit();
    write(value, &aw.writer) catch |err| switch (err) {
        error.WriteFailed => return error.OutOfMemory,
    };
    return aw.toOwnedSlice();
}

fn writeItems(items: []const AmqpValue, open: []const u8, close: []const u8, w: *Writer) Writer.Error!void {
    try w.writeAll(open);
    for (items, 0..) |item, i| {
        if (i > 0) try w.writeAll(", ");
        try write(item, w);
    }
    try w.writeAll(close);
}

/// Quote and escape, so that a string containing a quote, a backslash or a
/// control character cannot be mistaken for structure.
fn writeQuoted(s: []const u8, w: *Writer) Writer.Error!void {
    try w.writeAll("\"");
    for (s) |c| switch (c) {
        '"' => try w.writeAll("\\\""),
        '\\' => try w.writeAll("\\\\"),
        '\n' => try w.writeAll("\\n"),
        '\r' => try w.writeAll("\\r"),
        '\t' => try w.writeAll("\\t"),
        else => {
            if (c < 0x20 or c == 0x7f) {
                try w.print("\\x{x:0>2}", .{c});
            } else {
                try w.writeByte(c);
            }
        },
    };
    try w.writeAll("\"");
}

fn writeChar(c: u21, w: *Writer) Writer.Error!void {
    if (c >= 0x20 and c < 0x7f) {
        try w.print("'{c}'", .{@as(u8, @intCast(c))});
    } else {
        try w.print("U+{X:0>4}", .{c});
    }
}

fn writeUuid(u: [16]u8, w: *Writer) Writer.Error!void {
    try w.writeAll("urn:uuid:");
    for (u, 0..) |byte, i| {
        if (i == 4 or i == 6 or i == 8 or i == 10) try w.writeAll("-");
        try w.print("{x:0>2}", .{byte});
    }
}

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

fn expectRenders(expected: []const u8, value: AmqpValue) !void {
    const rendered = try toOwned(testing.allocator, value);
    defer testing.allocator.free(rendered);
    try testing.expectEqualStrings(expected, rendered);
}

test "scalars render as themselves" {
    try expectRenders("null", .null);
    try expectRenders("true", .{ .boolean = true });
    try expectRenders("false", .{ .boolean = false });
    try expectRenders("42", .{ .ubyte = 42 });
    try expectRenders("-7", .{ .int = -7 });
    try expectRenders("18446744073709551615", .{ .ulong = std.math.maxInt(u64) });
    try expectRenders("1.5", .{ .double = 1.5 });
    try expectRenders("timestamp(1571233434152)", .{ .timestamp = 1571233434152 });
}

test "a char renders printably or as a code point" {
    try expectRenders("'a'", .{ .char = 'a' });
    try expectRenders("U+00A0", .{ .char = 0xa0 });
}

test "a uuid renders in canonical form" {
    try expectRenders(
        "urn:uuid:00112233-4455-6677-8899-aabbccddeeff",
        .{ .uuid = .{ 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff } },
    );
}

test "strings, symbols and binary are told apart" {
    try expectRenders("\"hello\"", .{ .string = "hello" });
    try expectRenders(":hello", .{ .symbol = "hello" });
    try expectRenders("b\"00ff1a\"", .{ .binary = &[_]u8{ 0x00, 0xff, 0x1a } });
}

test "a string that contains structure cannot be mistaken for it" {
    try expectRenders("\"a\\\"b\\\\c\\nd\\x01\"", .{ .string = "a\"b\\c\nd\x01" });
}

test "an array is distinguishable from a list" {
    var items = [_]AmqpValue{ .{ .uint = 1 }, .{ .uint = 2 } };
    try expectRenders("[1, 2]", .{ .list = &items });
    try expectRenders("@[1, 2]", .{ .array = &items });
    try expectRenders("[]", .{ .list = &.{} });
    try expectRenders("@[]", .{ .array = &.{} });
}

test "a map renders its entries in order" {
    var entries = [_]value_mod.MapEntry{
        .{ .key = .{ .symbol = "operation" }, .value = .{ .string = "READ" } },
        .{ .key = .{ .string = "count" }, .value = .{ .uint = 3 } },
    };
    try expectRenders("{:operation: \"READ\", \"count\": 3}", .{ .map = &entries });
}

test "a described value renders as descriptor and value" {
    var descriptor: AmqpValue = .{ .symbol = "amqp:accepted:list" };
    var body: AmqpValue = .{ .list = &.{} };
    try expectRenders(":amqp:accepted:list([])", .{ .described = .{
        .descriptor = &descriptor,
        .value = &body,
    } });
}

test "nesting renders all the way down" {
    var inner = [_]AmqpValue{ .{ .string = "a" }, .null };
    var entries = [_]value_mod.MapEntry{
        .{ .key = .{ .symbol = "body" }, .value = .{ .list = &inner } },
    };
    var outer = [_]AmqpValue{.{ .map = &entries }};
    try expectRenders("[{:body: [\"a\", null]}]", .{ .list = &outer });
}

test "a value formats through the standard interface" {
    var aw = Writer.Allocating.init(testing.allocator);
    defer aw.deinit();
    try aw.writer.print("received {f}", .{AmqpValue{ .symbol = "amqp:decode-error" }});
    try testing.expectEqualStrings("received :amqp:decode-error", aw.written());
}
