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
        var desc_result = try decodeAt(allocator, data[1..], depth + 1);
        errdefer desc_result.value.deinit(allocator);
        var val_result = try decodeAt(allocator, data[1 + desc_result.bytes_consumed ..], depth + 1);
        errdefer val_result.value.deinit(allocator);

        const descriptor = try allocator.create(AmqpValue);
        errdefer allocator.destroy(descriptor);
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

/// A compound's `size` field counts every byte after itself, so it bounds the
/// whole body — the count field included. Reading it is what keeps a nested
/// element from running past its container: without it the only limit on a
/// malformed element is the end of the entire frame, and a truncated compound
/// is caught only if the truncation happens to land mid-element.
///
/// It also makes the container's declared width authoritative. The sum of what
/// the elements claimed to consume must agree with it, and if it does not, the
/// two disagree about where the next value starts — which is corruption, not
/// something to guess at.
const Compound = struct {
    /// Everything after the count field, clipped to the declared size.
    body: []const u8,
    count: usize,
    /// Constructor + size + declared size: what the compound occupies.
    total: usize,
};

fn compoundHeader(data: []const u8, comptime wide: bool) DecodeError!Compound {
    const width: usize = if (wide) 4 else 1;
    if (data.len < 1 + width * 2) return error.UnexpectedEnd;
    const size: usize = if (wide) readU32(data[1..5]) else data[1];
    // The size covers the count field, so it cannot be smaller than one.
    if (size < width) return error.InvalidData;
    const total = 1 + width + size;
    if (data.len < total) return error.UnexpectedEnd;
    return .{
        .body = data[1 + width * 2 .. total],
        .count = if (wide) readU32(data[5..9]) else data[2],
        .total = total,
    };
}

fn decodeList8(allocator: Allocator, data: []const u8, depth: u8) DecodeError!DecodeResult {
    const head = try compoundHeader(data, false);
    return try decodeListItems(allocator, head.body, head.count, head.total, depth);
}

fn decodeList32(allocator: Allocator, data: []const u8, depth: u8) DecodeError!DecodeResult {
    const head = try compoundHeader(data, true);
    return try decodeListItems(allocator, head.body, head.count, head.total, depth);
}

fn decodeListItems(allocator: Allocator, data: []const u8, count: usize, total: usize, depth: u8) DecodeError!DecodeResult {
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
    // The elements have to account for exactly the space the header said they
    // occupy; a shortfall means the two disagree on where the next value begins.
    if (offset != data.len) return error.InvalidData;
    return .{ .value = .{ .list = items }, .bytes_consumed = total };
}

fn decodeMap8(allocator: Allocator, data: []const u8, depth: u8) DecodeError!DecodeResult {
    const head = try compoundHeader(data, false);
    if (head.count % 2 != 0) return error.InvalidData;
    return try decodeMapEntries(allocator, head.body, head.count / 2, head.total, depth);
}

fn decodeMap32(allocator: Allocator, data: []const u8, depth: u8) DecodeError!DecodeResult {
    const head = try compoundHeader(data, true);
    if (head.count % 2 != 0) return error.InvalidData;
    return try decodeMapEntries(allocator, head.body, head.count / 2, head.total, depth);
}

fn decodeMapEntries(allocator: Allocator, data: []const u8, pair_count: usize, total: usize, depth: u8) DecodeError!DecodeResult {
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
    if (offset != data.len) return error.InvalidData;
    return .{ .value = .{ .map = entries }, .bytes_consumed = total };
}

fn decodeArray8(allocator: Allocator, data: []const u8, depth: u8) DecodeError!DecodeResult {
    const head = try compoundHeader(data, false);
    return try decodeArrayItems(allocator, head.body, head.count, head.total, depth);
}

fn decodeArray32(allocator: Allocator, data: []const u8, depth: u8) DecodeError!DecodeResult {
    const head = try compoundHeader(data, true);
    return try decodeArrayItems(allocator, head.body, head.count, head.total, depth);
}

fn decodeArrayItems(allocator: Allocator, data: []const u8, count: usize, total: usize, depth: u8) DecodeError!DecodeResult {
    // First byte is the shared constructor (format code). An array carries one
    // even when it is empty, so it is read — and consumed — either way.
    if (data.len < 1) return error.UnexpectedEnd;
    const constructor_code = data[0];

    if (count == 0) {
        // The constructor is still there and still counted; an empty array is
        // one byte of body, not zero.
        if (data.len != 1) return error.InvalidData;
        return .{
            .value = .{ .array = try allocator.alloc(AmqpValue, 0) },
            .bytes_consumed = total,
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

    // The elements share the one constructor at the head of the body, so
    // decoding element i means handing the decoder that constructor followed
    // by element i. Building that by copying the constructor in front of
    // everything still undecoded made the work quadratic in the element count,
    // and allocated a fresh buffer per element once the remainder outgrew 256
    // bytes: a 4096-element array cost 4034 allocations and 2.3ms.
    //
    // Take one mutable copy of the body instead, and write the constructor
    // into the byte immediately before each element as it is reached. That
    // byte is the last byte of the element just decoded, which is safe to
    // overwrite because decoded values own their bytes rather than borrow
    // them. The first element needs nothing done: the constructor is already
    // in front of it.
    var stack_buf: [256]u8 = undefined;
    const heap: ?[]u8 = if (data.len <= stack_buf.len) null else try allocator.alloc(u8, data.len);
    defer if (heap) |h| allocator.free(h);
    const scratch = heap orelse stack_buf[0..data.len];
    @memcpy(scratch, data);

    var offset: usize = 1; // past the shared constructor
    for (0..count) |i| {
        scratch[offset - 1] = constructor_code;
        const result = try decodeAt(allocator, scratch[offset - 1 ..], depth + 1);
        items[i] = result.value;
        filled = i + 1;
        offset += result.bytes_consumed - 1;
    }
    if (offset != data.len) return error.InvalidData;
    return .{ .value = .{ .array = items }, .bytes_consumed = total };
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

test "a decode interrupted by allocation failure frees what it built" {
    // A described value whose body is a list holding a string, a binary, a
    // map and an array: every compound path that allocates, nested, so a
    // failure part-way through has something half-built to leak.
    const bytes = [_]u8{
        0x00, 0x53, 0x75, // described, descriptor smallulong 117
        0xc0, 0x1b, 0x04, // list8 of four
        0xa1, 0x03, 'a',
        'b',  'c',
        0xa0, 0x02, 0x01, 0x02, // vbin8
        0xc1, 0x07, 0x02, 0xa1, 0x01, 'k', 0xa1, 0x01, 'v', // map8
        0xe0, 0x06, 0x02, 0xa1, 0x01, 'x', 0x01, 'y', // array8 of str8
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn one(allocator: Allocator, data: []const u8) !void {
            var result = try decode(allocator, data);
            result.value.deinit(allocator);
        }
    }.one, .{&bytes});
}

test "a compound is bounded by its declared size, not by the end of the buffer" {
    const allocator = std.testing.allocator;

    // A list8 declaring five bytes of body: the count, then a nested list8
    // that wants five bytes of its own but is only given four. Before the
    // outer size was honoured the inner list read straight through it and
    // swallowed the 0x42 that belongs to whatever comes after the outer list.
    const nested_overrun = [_]u8{
        0xc0, 0x05, 0x01, // outer list8, five bytes of body, one element
        0xc0, 0x03, 0x01, 0x41, // inner list8 wanting 0x41 plus one more byte
        0x42, // outside the outer list entirely
    };
    try std.testing.expectError(error.UnexpectedEnd, decode(allocator, &nested_overrun));
}

test "a compound whose elements do not fill it is rejected" {
    const allocator = std.testing.allocator;

    // Body is two bytes, but the single element accounts for one of them.
    // The header and the elements disagree about where the next value starts,
    // and there is no way to tell which of the two is right.
    const short = [_]u8{ 0xc0, 0x03, 0x01, 0x41, 0x42 };
    try std.testing.expectError(error.InvalidData, decode(allocator, &short));

    // Same for a map...
    const map = [_]u8{ 0xc1, 0x04, 0x02, 0x41, 0x42, 0x43 };
    try std.testing.expectError(error.InvalidData, decode(allocator, &map));

    // ...and for an array, including the empty one, where the body is exactly
    // the shared constructor and nothing else.
    const array = [_]u8{ 0xe0, 0x03, 0x00, 0x40, 0x41 };
    try std.testing.expectError(error.InvalidData, decode(allocator, &array));
}

test "a compound truncated mid-element is caught by its own header" {
    const allocator = std.testing.allocator;

    // Declares nine bytes of body and supplies three.
    const truncated = [_]u8{ 0xc0, 0x09, 0x02, 0x41, 0x41 };
    try std.testing.expectError(error.UnexpectedEnd, decode(allocator, &truncated));

    // The size has to cover the count field it precedes.
    const too_small = [_]u8{ 0xc0, 0x00, 0x00 };
    try std.testing.expectError(error.InvalidData, decode(allocator, &too_small));

    const too_small_wide = [_]u8{ 0xd0, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00 };
    try std.testing.expectError(error.InvalidData, decode(allocator, &too_small_wide));
}

test "a compound consumes exactly what it declared" {
    const allocator = std.testing.allocator;

    // An empty list followed by a byte that is none of its business.
    const data = [_]u8{ 0xc0, 0x01, 0x00, 0xff };
    var result = try decode(allocator, &data);
    defer result.value.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), result.value.list.len);
    try std.testing.expectEqual(@as(usize, 3), result.bytes_consumed);
}

test "decoding an array does not allocate per element" {
    // The constructor used to be copied in front of everything still
    // undecoded, once per element: quadratic work, and a fresh buffer every
    // time the remainder was larger than 256 bytes. The count of allocations
    // is the part worth pinning, because it is what made a large array cost
    // so much more than the bytes on the wire suggest.
    const Counting = struct {
        parent: Allocator,
        count: usize = 0,

        fn alloc(ctx: *anyopaque, len: usize, a: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.count += 1;
            return self.parent.rawAlloc(len, a, ra);
        }
        fn resize(ctx: *anyopaque, buf: []u8, a: std.mem.Alignment, new_len: usize, ra: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.parent.rawResize(buf, a, new_len, ra);
        }
        fn remap(ctx: *anyopaque, buf: []u8, a: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.parent.rawRemap(buf, a, new_len, ra);
        }
        fn free(ctx: *anyopaque, buf: []u8, a: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.parent.rawFree(buf, a, ra);
        }
        fn allocator(self: *@This()) Allocator {
            return .{ .ptr = self, .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .remap = remap,
                .free = free,
            } };
        }
    };

    const enc = @import("encoder.zig");
    var counts: [2]usize = undefined;
    // Two sizes an order of magnitude apart, both well past the 256-byte
    // buffer that used to decide whether an element allocated.
    for ([_]usize{ 128, 2048 }, 0..) |n, slot| {
        const items = try std.testing.allocator.alloc(AmqpValue, n);
        defer std.testing.allocator.free(items);
        for (items, 0..) |*it, i| it.* = .{ .uint = @intCast(i) };

        var buf = enc.Buffer.initDynamic(std.testing.allocator);
        defer buf.deinit();
        try enc.encode(.{ .array = items }, &buf);

        var counting = Counting{ .parent = std.testing.allocator };
        const ca = counting.allocator();
        var result = try decode(ca, buf.written());
        try std.testing.expectEqual(n, result.value.array.len);
        result.value.deinit(ca);
        counts[slot] = counting.count;
    }

    // Sixteen times the elements, the same number of allocations.
    try std.testing.expectEqual(counts[0], counts[1]);
}
