const std = @import("std");
const Allocator = std.mem.Allocator;
const amqp = @import("amqp_value.zig");
const AmqpValue = amqp.AmqpValue;
const MapEntry = amqp.MapEntry;
const Described = amqp.Described;
const FormatCode = amqp.FormatCode;

pub const DecodeError = error{
    InvalidFormatCode,
    InvalidData,
    UnexpectedEnd,
    /// The value nests deeper than `max_nesting_depth`.
    NestingTooDeep,
    OutOfMemory,
};

/// How deeply a value may nest before it is refused.
///
/// Decoding is recursive, and nesting is entirely under the peer's control:
/// a described type inside a described type inside a list costs a stack
/// frame each time, and 200 KB of `0x00` bytes — well inside any ordinary
/// max-frame-size — is enough to run the stack out and abort the process.
/// Real AMQP values nest a handful deep; this leaves a wide margin.
pub const max_nesting_depth: u8 = 64;

pub const DecodeResult = struct { value: AmqpValue, bytes_consumed: usize };

/// Decode a single AMQP 1.0 value from binary data.
/// Returns the decoded value and the number of bytes consumed.
pub fn decode(allocator: Allocator, data: []const u8) DecodeError!DecodeResult {
    return decodeAt(allocator, data, 0);
}

fn decodeAt(allocator: Allocator, data: []const u8, depth: u8) DecodeError!DecodeResult {
    if (depth > max_nesting_depth) return error.NestingTooDeep;
    if (data.len == 0) return error.UnexpectedEnd;

    const code_byte = data[0];

    // Described type constructor
    if (code_byte == 0x00) {
        if (data.len < 2) return error.UnexpectedEnd;
        const desc_result = try decodeAt(allocator, data[1..], depth + 1);
        const val_result = try decodeAt(allocator, data[1 + desc_result.bytes_consumed ..], depth + 1);
        const descriptor = try allocator.create(AmqpValue);
        descriptor.* = desc_result.value;
        const value = try allocator.create(AmqpValue);
        value.* = val_result.value;
        return .{
            .value = .{ .described = .{ .descriptor = descriptor, .value = value } },
            .bytes_consumed = 1 + desc_result.bytes_consumed + val_result.bytes_consumed,
        };
    }

    const code: FormatCode = @enumFromInt(code_byte);
    return switch (code) {
        .described => unreachable, // handled above via code_byte == 0x00 check

        // Fixed-width: 0 octets
        .null => .{ .value = .null, .bytes_consumed = 1 },
        .boolean_true => .{ .value = .{ .boolean = true }, .bytes_consumed = 1 },
        .boolean_false => .{ .value = .{ .boolean = false }, .bytes_consumed = 1 },
        .uint_0 => .{ .value = .{ .uint = 0 }, .bytes_consumed = 1 },
        .ulong_0 => .{ .value = .{ .ulong = 0 }, .bytes_consumed = 1 },
        .list_0 => .{ .value = .{ .list = try allocator.alloc(AmqpValue, 0) }, .bytes_consumed = 1 },

        // Fixed-width: 1 octet
        .boolean => blk: {
            if (data.len < 2) return error.UnexpectedEnd;
            break :blk .{ .value = .{ .boolean = data[1] != 0 }, .bytes_consumed = 2 };
        },
        .ubyte => blk: {
            if (data.len < 2) return error.UnexpectedEnd;
            break :blk .{ .value = .{ .ubyte = data[1] }, .bytes_consumed = 2 };
        },
        .byte => blk: {
            if (data.len < 2) return error.UnexpectedEnd;
            break :blk .{ .value = .{ .byte = @bitCast(data[1]) }, .bytes_consumed = 2 };
        },
        .smalluint => blk: {
            if (data.len < 2) return error.UnexpectedEnd;
            break :blk .{ .value = .{ .uint = data[1] }, .bytes_consumed = 2 };
        },
        .smallulong => blk: {
            if (data.len < 2) return error.UnexpectedEnd;
            break :blk .{ .value = .{ .ulong = data[1] }, .bytes_consumed = 2 };
        },
        .smallint => blk: {
            if (data.len < 2) return error.UnexpectedEnd;
            const v: i8 = @bitCast(data[1]);
            break :blk .{ .value = .{ .int = v }, .bytes_consumed = 2 };
        },
        .smalllong => blk: {
            if (data.len < 2) return error.UnexpectedEnd;
            const v: i8 = @bitCast(data[1]);
            break :blk .{ .value = .{ .long = v }, .bytes_consumed = 2 };
        },

        // Fixed-width: 2 octets
        .ushort => blk: {
            if (data.len < 3) return error.UnexpectedEnd;
            break :blk .{ .value = .{ .ushort = readU16(data[1..3]) }, .bytes_consumed = 3 };
        },
        .short => blk: {
            if (data.len < 3) return error.UnexpectedEnd;
            break :blk .{ .value = .{ .short = @bitCast(readU16(data[1..3])) }, .bytes_consumed = 3 };
        },

        // Fixed-width: 4 octets
        .uint => blk: {
            if (data.len < 5) return error.UnexpectedEnd;
            break :blk .{ .value = .{ .uint = readU32(data[1..5]) }, .bytes_consumed = 5 };
        },
        .int => blk: {
            if (data.len < 5) return error.UnexpectedEnd;
            break :blk .{ .value = .{ .int = @bitCast(readU32(data[1..5])) }, .bytes_consumed = 5 };
        },
        .float => blk: {
            if (data.len < 5) return error.UnexpectedEnd;
            break :blk .{ .value = .{ .float = @bitCast(readU32(data[1..5])) }, .bytes_consumed = 5 };
        },
        .char => blk: {
            if (data.len < 5) return error.UnexpectedEnd;
            const raw = readU32(data[1..5]);
            if (raw > 0x10FFFF) return error.InvalidData;
            break :blk .{ .value = .{ .char = @intCast(raw) }, .bytes_consumed = 5 };
        },

        // Fixed-width: 8 octets
        .ulong => blk: {
            if (data.len < 9) return error.UnexpectedEnd;
            break :blk .{ .value = .{ .ulong = readU64(data[1..9]) }, .bytes_consumed = 9 };
        },
        .long => blk: {
            if (data.len < 9) return error.UnexpectedEnd;
            break :blk .{ .value = .{ .long = @bitCast(readU64(data[1..9])) }, .bytes_consumed = 9 };
        },
        .double => blk: {
            if (data.len < 9) return error.UnexpectedEnd;
            break :blk .{ .value = .{ .double = @bitCast(readU64(data[1..9])) }, .bytes_consumed = 9 };
        },
        .timestamp => blk: {
            if (data.len < 9) return error.UnexpectedEnd;
            break :blk .{ .value = .{ .timestamp = @bitCast(readU64(data[1..9])) }, .bytes_consumed = 9 };
        },

        // Fixed-width: 16 octets
        .uuid => blk: {
            if (data.len < 17) return error.UnexpectedEnd;
            break :blk .{ .value = .{ .uuid = data[1..17].* }, .bytes_consumed = 17 };
        },

        // Variable-width: 1-octet size
        .binary_8 => try decodeVariable8(allocator, data, .binary),
        .string_8 => try decodeVariable8(allocator, data, .string),
        .symbol_8 => try decodeVariable8(allocator, data, .symbol),

        // Variable-width: 4-octet size
        .binary_32 => try decodeVariable32(allocator, data, .binary),
        .string_32 => try decodeVariable32(allocator, data, .string),
        .symbol_32 => try decodeVariable32(allocator, data, .symbol),

        // Compound: list
        .list_8 => try decodeList8(allocator, data, depth),
        .list_32 => try decodeList32(allocator, data, depth),

        // Compound: map
        .map_8 => try decodeMap8(allocator, data, depth),
        .map_32 => try decodeMap32(allocator, data, depth),

        // Array
        .array_8 => try decodeArray8(allocator, data, depth),
        .array_32 => try decodeArray32(allocator, data, depth),

        _ => error.InvalidFormatCode,
    };
}

const VariableTag = enum { binary, string, symbol };

fn decodeVariable8(allocator: Allocator, data: []const u8, tag: VariableTag) DecodeError!DecodeResult {
    if (data.len < 2) return error.UnexpectedEnd;
    const len: usize = data[1];
    if (data.len < 2 + len) return error.UnexpectedEnd;
    const bytes = try allocator.dupe(u8, data[2 .. 2 + len]);
    const value: AmqpValue = switch (tag) {
        .binary => .{ .binary = bytes },
        .string => .{ .string = bytes },
        .symbol => .{ .symbol = bytes },
    };
    return .{ .value = value, .bytes_consumed = 2 + len };
}

fn decodeVariable32(allocator: Allocator, data: []const u8, tag: VariableTag) DecodeError!DecodeResult {
    if (data.len < 5) return error.UnexpectedEnd;
    const len: usize = readU32(data[1..5]);
    if (data.len < 5 + len) return error.UnexpectedEnd;
    const bytes = try allocator.dupe(u8, data[5 .. 5 + len]);
    const value: AmqpValue = switch (tag) {
        .binary => .{ .binary = bytes },
        .string => .{ .string = bytes },
        .symbol => .{ .symbol = bytes },
    };
    return .{ .value = value, .bytes_consumed = 5 + len };
}

fn decodeList8(allocator: Allocator, data: []const u8, depth: u8) DecodeError!DecodeResult {
    if (data.len < 3) return error.UnexpectedEnd;
    const size: usize = data[1];
    const count: usize = data[2];
    _ = size;
    return try decodeListItems(allocator, data[3..], count, 3, depth);
}

fn decodeList32(allocator: Allocator, data: []const u8, depth: u8) DecodeError!DecodeResult {
    if (data.len < 9) return error.UnexpectedEnd;
    const size: usize = readU32(data[1..5]);
    const count: usize = readU32(data[5..9]);
    _ = size;
    return try decodeListItems(allocator, data[9..], count, 9, depth);
}

fn decodeListItems(allocator: Allocator, data: []const u8, count: usize, header_size: usize, depth: u8) DecodeError!DecodeResult {
    // The count comes off the wire. Every element needs at least one byte, so
    // reject a count the remaining data cannot possibly satisfy before
    // allocating for it.
    if (count > data.len) return error.UnexpectedEnd;

    const items = try allocator.alloc(AmqpValue, count);
    // Only the prefix written so far is initialized; deiniting past it would
    // free whatever the allocator happened to leave in the tail.
    var filled: usize = 0;
    errdefer {
        for (items[0..filled]) |*item| {
            @constCast(item).deinit(allocator);
        }
        allocator.free(items);
    }
    var offset: usize = 0;
    for (0..count) |i| {
        const result = try decodeAt(allocator, data[offset..], depth + 1);
        items[i] = result.value;
        filled = i + 1;
        offset += result.bytes_consumed;
    }
    return .{ .value = .{ .list = items }, .bytes_consumed = header_size + offset };
}

fn decodeMap8(allocator: Allocator, data: []const u8, depth: u8) DecodeError!DecodeResult {
    if (data.len < 3) return error.UnexpectedEnd;
    const size: usize = data[1];
    const count: usize = data[2];
    _ = size;
    if (count % 2 != 0) return error.InvalidData;
    return try decodeMapEntries(allocator, data[3..], count / 2, 3, depth);
}

fn decodeMap32(allocator: Allocator, data: []const u8, depth: u8) DecodeError!DecodeResult {
    if (data.len < 9) return error.UnexpectedEnd;
    const size: usize = readU32(data[1..5]);
    const count: usize = readU32(data[5..9]);
    _ = size;
    if (count % 2 != 0) return error.InvalidData;
    return try decodeMapEntries(allocator, data[9..], count / 2, 9, depth);
}

fn decodeMapEntries(allocator: Allocator, data: []const u8, pair_count: usize, header_size: usize, depth: u8) DecodeError!DecodeResult {
    // Each pair needs at least two bytes, so reject a count the remaining data
    // cannot possibly satisfy before allocating for it.
    if (pair_count > data.len / 2) return error.UnexpectedEnd;

    const entries = try allocator.alloc(MapEntry, pair_count);
    // Only the prefix written so far is initialized; deiniting past it would
    // free whatever the allocator happened to leave in the tail.
    var filled: usize = 0;
    errdefer {
        for (entries[0..filled]) |*entry| {
            @constCast(&entry.key).deinit(allocator);
            @constCast(&entry.value).deinit(allocator);
        }
        allocator.free(entries);
    }
    var offset: usize = 0;
    for (0..pair_count) |i| {
        const key_result = try decodeAt(allocator, data[offset..], depth + 1);
        offset += key_result.bytes_consumed;
        var key_value = key_result.value;
        errdefer key_value.deinit(allocator);

        const val_result = try decodeAt(allocator, data[offset..], depth + 1);
        offset += val_result.bytes_consumed;
        entries[i] = .{ .key = key_value, .value = val_result.value };
        filled = i + 1;
    }
    return .{ .value = .{ .map = entries }, .bytes_consumed = header_size + offset };
}

fn decodeArray8(allocator: Allocator, data: []const u8, depth: u8) DecodeError!DecodeResult {
    if (data.len < 3) return error.UnexpectedEnd;
    const size: usize = data[1];
    const count: usize = data[2];
    _ = size;
    return try decodeArrayItems(allocator, data[3..], count, 3, depth);
}

fn decodeArray32(allocator: Allocator, data: []const u8, depth: u8) DecodeError!DecodeResult {
    if (data.len < 9) return error.UnexpectedEnd;
    const size: usize = readU32(data[1..5]);
    const count: usize = readU32(data[5..9]);
    _ = size;
    return try decodeArrayItems(allocator, data[9..], count, 9, depth);
}

fn decodeArrayItems(allocator: Allocator, data: []const u8, count: usize, header_size: usize, depth: u8) DecodeError!DecodeResult {
    // First byte is the shared constructor (format code). An array carries one
    // even when it is empty, so it is read — and consumed — either way.
    if (data.len < 1) return error.UnexpectedEnd;
    const constructor_code = data[0];

    if (count == 0) {
        return .{
            .value = .{ .array = try allocator.alloc(AmqpValue, 0) },
            .bytes_consumed = header_size + 1,
        };
    }

    // The count comes off the wire; reject one the remaining data cannot
    // possibly satisfy before allocating for it.
    if (count > data.len) return error.UnexpectedEnd;

    const items = try allocator.alloc(AmqpValue, count);
    // Only the prefix written so far is initialized; deiniting past it would
    // free whatever the allocator happened to leave in the tail.
    var filled: usize = 0;
    errdefer {
        for (items[0..filled]) |*item| {
            @constCast(item).deinit(allocator);
        }
        allocator.free(items);
    }

    // Decode each item by prepending the constructor byte
    var offset: usize = 1; // skip constructor
    for (0..count) |i| {
        // Build a temporary buffer: constructor + remaining data
        // For efficiency, we decode by manually handling the format code
        var temp_buf: [256]u8 = undefined;
        const remaining = data[offset..];
        if (remaining.len + 1 > temp_buf.len) {
            // Fall back to allocated buffer for large items
            const temp = try allocator.alloc(u8, remaining.len + 1);
            defer allocator.free(temp);
            temp[0] = constructor_code;
            @memcpy(temp[1 .. 1 + remaining.len], remaining);
            const result = try decodeAt(allocator, temp, depth + 1);
            items[i] = result.value;
            filled = i + 1;
            offset += result.bytes_consumed - 1; // -1 for constructor we prepended
        } else {
            temp_buf[0] = constructor_code;
            @memcpy(temp_buf[1 .. 1 + remaining.len], remaining);
            const result = try decodeAt(allocator, temp_buf[0 .. 1 + remaining.len], depth + 1);
            items[i] = result.value;
            filled = i + 1;
            offset += result.bytes_consumed - 1;
        }
    }
    return .{ .value = .{ .array = items }, .bytes_consumed = header_size + offset };
}

// ── Helpers ────────────────────────────────────────────────────────────

fn readU16(data: []const u8) u16 {
    return std.mem.readInt(u16, data[0..2], .big);
}

fn readU32(data: []const u8) u32 {
    return std.mem.readInt(u32, data[0..4], .big);
}

fn readU64(data: []const u8) u64 {
    return std.mem.readInt(u64, data[0..8], .big);
}

// ── Tests ──────────────────────────────────────────────────────────────

test "decode null" {
    const allocator = std.testing.allocator;
    const result = try decode(allocator, &[_]u8{0x40});
    try std.testing.expect(result.value.eql(.null));
    try std.testing.expectEqual(@as(usize, 1), result.bytes_consumed);
}

test "decode boolean" {
    const allocator = std.testing.allocator;
    const r1 = try decode(allocator, &[_]u8{0x41});
    try std.testing.expect(r1.value.eql(.{ .boolean = true }));

    const r2 = try decode(allocator, &[_]u8{0x42});
    try std.testing.expect(r2.value.eql(.{ .boolean = false }));
}

test "decode uint forms" {
    const allocator = std.testing.allocator;

    // uint_0
    const r0 = try decode(allocator, &[_]u8{0x43});
    try std.testing.expect(r0.value.eql(.{ .uint = 0 }));

    // smalluint
    const r1 = try decode(allocator, &[_]u8{ 0x52, 42 });
    try std.testing.expect(r1.value.eql(.{ .uint = 42 }));

    // uint
    const r2 = try decode(allocator, &[_]u8{ 0x70, 0x12, 0x34, 0x56, 0x78 });
    try std.testing.expect(r2.value.eql(.{ .uint = 0x12345678 }));
}

test "decode string" {
    const allocator = std.testing.allocator;
    const data = [_]u8{ 0xa1, 5, 'h', 'e', 'l', 'l', 'o' };
    const result = try decode(allocator, &data);
    defer @constCast(&result.value).deinit(allocator);
    try std.testing.expectEqualStrings("hello", result.value.string);
}

test "roundtrip encode-decode" {
    const allocator = std.testing.allocator;
    const enc = @import("encoder.zig");

    const test_values = [_]AmqpValue{
        .null,
        .{ .boolean = true },
        .{ .boolean = false },
        .{ .ubyte = 255 },
        .{ .ushort = 1000 },
        .{ .uint = 0 },
        .{ .uint = 100 },
        .{ .uint = 0x12345678 },
        .{ .ulong = 0 },
        .{ .ulong = 50 },
        .{ .byte = -1 },
        .{ .short = -1000 },
        .{ .int = 0 },
        .{ .int = -42 },
        .{ .long = 0 },
        .{ .long = -100 },
        .{ .float = 3.14 },
        .{ .double = 2.71828 },
        .{ .timestamp = 1617235200000 },
    };

    for (test_values) |original| {
        var buf_arr: [64]u8 = undefined;
        var buf = enc.Buffer.initFixed(&buf_arr);
        try enc.encode(original, &buf);
        const written = buf.written();

        const result = try decode(allocator, written);
        defer {
            var v = result.value;
            v.deinit(allocator);
        }

        try std.testing.expect(original.eql(result.value));
        try std.testing.expectEqual(written.len, result.bytes_consumed);
    }
}

test "roundtrip string" {
    const allocator = std.testing.allocator;
    const enc = @import("encoder.zig");

    const original: AmqpValue = .{ .string = "hello world" };
    var buf_arr: [64]u8 = undefined;
    var buf = enc.Buffer.initFixed(&buf_arr);
    try enc.encode(original, &buf);

    const result = try decode(allocator, buf.written());
    defer @constCast(&result.value).deinit(allocator);
    try std.testing.expectEqualStrings("hello world", result.value.string);
}

test "roundtrip list" {
    const allocator = std.testing.allocator;
    const enc = @import("encoder.zig");

    var items = [_]AmqpValue{ .{ .uint = 1 }, .{ .boolean = true }, .null };
    const original: AmqpValue = .{ .list = &items };

    var buf_arr: [64]u8 = undefined;
    var buf = enc.Buffer.initFixed(&buf_arr);
    try enc.encode(original, &buf);

    const result = try decode(allocator, buf.written());
    defer {
        var v = result.value;
        v.deinit(allocator);
    }
    try std.testing.expect(original.eql(result.value));
}

test "a truncated map does not deinit uninitialized entries" {
    const allocator = std.testing.allocator;
    // map8 declaring one pair, with no entry bytes following it.
    try std.testing.expectError(error.UnexpectedEnd, decode(allocator, &.{ 0xc1, 0x02, 0x02 }));
}

test "a truncated list does not deinit uninitialized items" {
    const allocator = std.testing.allocator;
    // list8 declaring three items, with only one byte of body.
    try std.testing.expectError(error.UnexpectedEnd, decode(allocator, &.{ 0xc0, 0x02, 0x03, 0x40 }));
}

test "a truncated array does not deinit uninitialized items" {
    const allocator = std.testing.allocator;
    // array8 declaring four symbols after the shared constructor.
    try std.testing.expectError(error.UnexpectedEnd, decode(allocator, &.{ 0xe0, 0x02, 0x04, 0xa3 }));
}

test "an oversized count is rejected before allocating" {
    const allocator = std.testing.allocator;
    // list32 claiming 0xffffffff items with an empty body. Allocating for that
    // count would need 64 GiB.
    try std.testing.expectError(error.UnexpectedEnd, decode(
        allocator,
        &.{ 0xd0, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff, 0xff },
    ));
    // map32 with the same claim.
    try std.testing.expectError(error.UnexpectedEnd, decode(
        allocator,
        &.{ 0xd1, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff, 0xfe },
    ));
}

test "a partially decoded map frees only what it built" {
    const allocator = std.testing.allocator;
    // A well-formed first pair (str8 "k" -> str8 "v"), then a truncated key.
    // The first pair must be freed exactly once and the tail left alone.
    try std.testing.expectError(error.UnexpectedEnd, decode(allocator, &.{
        0xc1, 0x0c, 0x04,
        0xa1, 0x01, 'k',
        0xa1, 0x01, 'v',
        0xa1, 0x04, 'a',
    }));
}

test "a map whose value is truncated frees the already decoded key" {
    const allocator = std.testing.allocator;
    // str8 "k" decodes as a key, then the value claims four bytes but has one.
    try std.testing.expectError(error.UnexpectedEnd, decode(allocator, &.{
        0xc1, 0x08, 0x02,
        0xa1, 0x01, 'k',
        0xa1, 0x04, 'v',
    }));
}

test "decoding a nested truncated list is safe" {
    const allocator = std.testing.allocator;
    // Outer list of two: a valid null, then an inner list8 claiming two items
    // it does not have.
    try std.testing.expectError(error.UnexpectedEnd, decode(allocator, &.{
        0xc0, 0x06, 0x02,
        0x40, 0xc0, 0x02,
        0x02, 0x40,
    }));
}

test "nesting deeper than the limit is refused rather than crashing the process" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // A described type whose descriptor is another described type, and so on.
    // Every level is one byte and one stack frame; before the limit, 200 KB of
    // these — well inside an ordinary max-frame-size — aborted the process.
    const nested = try allocator.alloc(u8, 200_000);
    defer allocator.free(nested);
    @memset(nested, 0x00);
    try std.testing.expectError(error.NestingTooDeep, decode(arena.allocator(), nested));

    // Nesting the decoder is expected to handle still works: a chain of
    // described values, each `00 40 <inner>`, ending in a null.
    const levels = max_nesting_depth - 4;
    var shallow: [levels * 2 + 1]u8 = undefined;
    for (0..levels) |i| {
        shallow[i * 2] = 0x00;
        shallow[i * 2 + 1] = 0x40;
    }
    shallow[levels * 2] = 0x40;
    const result = try decode(arena.allocator(), &shallow);
    try std.testing.expectEqual(shallow.len, result.bytes_consumed);
}

test "fuzz: decoding arbitrary bytes never crashes" {
    try std.testing.fuzz({}, struct {
        fn one(_: void, smith: *std.testing.Smith) anyerror!void {
            var buf: [4096]u8 = undefined;
            const len = smith.slice(&buf);
            const data = buf[0..len];

            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();

            const result = decode(arena.allocator(), data) catch return;

            // A decoder that consumed nothing would spin its caller forever,
            // and one that consumed more than it was given read past the end.
            try std.testing.expect(result.bytes_consumed >= 1);
            try std.testing.expect(result.bytes_consumed <= data.len);
        }
    }.one, .{});
}
