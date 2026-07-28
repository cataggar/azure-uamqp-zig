const std = @import("std");
const amqp = @import("amqp_value.zig");
const AmqpValue = amqp.AmqpValue;
const MapEntry = amqp.MapEntry;
const FormatCode = amqp.FormatCode;
const Allocator = std.mem.Allocator;

/// A simple growable byte buffer for encoding.
pub const Buffer = struct {
    data: []u8,
    pos: usize,
    allocator: ?Allocator,
    is_fixed: bool,

    pub fn initFixed(buf: []u8) Buffer {
        return .{ .data = buf, .pos = 0, .allocator = null, .is_fixed = true };
    }

    pub fn initDynamic(allocator: Allocator) Buffer {
        return .{ .data = &.{}, .pos = 0, .allocator = allocator, .is_fixed = false };
    }

    pub fn deinit(self: *Buffer) void {
        if (!self.is_fixed) {
            if (self.allocator) |a| {
                if (self.data.len > 0) a.free(self.data);
            }
        }
    }

    pub fn written(self: *const Buffer) []const u8 {
        return self.data[0..self.pos];
    }

    /// The written bytes, writable — for a caller that has to go back and fill
    /// in a length it could not know until the rest had been encoded.
    pub fn mutable(self: *Buffer) []u8 {
        return self.data[0..self.pos];
    }

    pub fn reset(self: *Buffer) void {
        self.pos = 0;
    }

    /// Take the written bytes out of the buffer, trimmed to exactly their
    /// length. The buffer owns nothing afterwards and is safe to `deinit`, so
    /// a caller can hand the bytes on without the buffer and the new owner
    /// both believing they hold them.
    pub fn toOwnedSlice(self: *Buffer) Allocator.Error![]u8 {
        const a = self.allocator orelse return error.OutOfMemory;
        if (self.is_fixed) return error.OutOfMemory;
        const out = try a.realloc(self.data, self.pos);
        self.data = &.{};
        self.pos = 0;
        return out;
    }

    fn ensureCapacity(self: *Buffer, additional: usize) !void {
        const needed = self.pos + additional;
        if (needed <= self.data.len) return;
        if (self.is_fixed) return error.OutOfMemory;
        const a = self.allocator orelse return error.OutOfMemory;
        const new_cap = @max(self.data.len * 2, needed, 64);
        if (self.data.len > 0) {
            self.data = try a.realloc(self.data, new_cap);
        } else {
            self.data = try a.alloc(u8, new_cap);
        }
    }

    pub fn writeByte(self: *Buffer, byte: u8) !void {
        try self.ensureCapacity(1);
        self.data[self.pos] = byte;
        self.pos += 1;
    }

    pub fn writeAll(self: *Buffer, bytes: []const u8) !void {
        try self.ensureCapacity(bytes.len);
        @memcpy(self.data[self.pos .. self.pos + bytes.len], bytes);
        self.pos += bytes.len;
    }
};

pub const EncodeError = error{
    OutOfMemory,
    /// An array's elements do not all share one AMQP type, so there is no
    /// single constructor that describes them.
    MixedArrayElements,
    /// Arrays of described values need a shared descriptor in the constructor,
    /// which this encoder does not emit.
    UnsupportedArrayElement,
    /// The encoded value does not fit in an AMQP size or count field.
    ValueTooLarge,
};

const max_field: usize = std.math.maxInt(u32);

/// The chosen width of a compound (list/map/array) encoding.
///
/// `body` is everything after the count field: the elements, plus the shared
/// constructor for an array. The size field covers the count field as well,
/// which is why it is `body + 1` or `body + 4` rather than `body`.
const Compound = struct {
    count: usize,
    body: usize,
    small: bool,

    fn init(count: usize, body: usize) EncodeError!Compound {
        // The guard belongs on the size field, not the body: a 255-byte body
        // needs a size field of 256, which does not fit in a byte.
        const small = body + 1 <= 0xFF and count <= 0xFF;
        if (!small and (body + 4 > max_field or count > max_field)) return error.ValueTooLarge;
        return .{ .count = count, .body = body, .small = small };
    }

    fn headerSize(self: Compound) usize {
        return if (self.small) 3 else 9;
    }

    fn write(self: Compound, buf: *Buffer, small_code: FormatCode, large_code: FormatCode) EncodeError!void {
        if (self.small) {
            try buf.writeByte(@intFromEnum(small_code));
            try buf.writeByte(@intCast(self.body + 1));
            try buf.writeByte(@intCast(self.count));
        } else {
            try buf.writeByte(@intFromEnum(large_code));
            try writeU32(buf, @intCast(self.body + 4));
            try writeU32(buf, @intCast(self.count));
        }
    }
};

fn writeU32(buf: *Buffer, v: u32) EncodeError!void {
    try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u32, v, .big)));
}

/// Encode an AmqpValue into AMQP 1.0 binary wire format.
pub fn encode(value: AmqpValue, buf: *Buffer) EncodeError!void {
    switch (value) {
        .null => try buf.writeByte(@intFromEnum(FormatCode.null)),
        .boolean => |v| {
            try buf.writeByte(@intFromEnum(if (v) FormatCode.boolean_true else FormatCode.boolean_false));
        },
        .ubyte => |v| {
            try buf.writeByte(@intFromEnum(FormatCode.ubyte));
            try buf.writeByte(v);
        },
        .ushort => |v| {
            try buf.writeByte(@intFromEnum(FormatCode.ushort));
            try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u16, v, .big)));
        },
        .uint => |v| {
            if (v == 0) {
                try buf.writeByte(@intFromEnum(FormatCode.uint_0));
            } else if (v <= 0xFF) {
                try buf.writeByte(@intFromEnum(FormatCode.smalluint));
                try buf.writeByte(@truncate(v));
            } else {
                try buf.writeByte(@intFromEnum(FormatCode.uint));
                try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u32, v, .big)));
            }
        },
        .ulong => |v| {
            if (v == 0) {
                try buf.writeByte(@intFromEnum(FormatCode.ulong_0));
            } else if (v <= 0xFF) {
                try buf.writeByte(@intFromEnum(FormatCode.smallulong));
                try buf.writeByte(@truncate(v));
            } else {
                try buf.writeByte(@intFromEnum(FormatCode.ulong));
                try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u64, v, .big)));
            }
        },
        .byte => |v| {
            try buf.writeByte(@intFromEnum(FormatCode.byte));
            try buf.writeByte(@bitCast(v));
        },
        .short => |v| {
            try buf.writeByte(@intFromEnum(FormatCode.short));
            try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u16, @as(u16, @bitCast(v)), .big)));
        },
        .int => |v| {
            if (v >= -128 and v <= 127) {
                try buf.writeByte(@intFromEnum(FormatCode.smallint));
                try buf.writeByte(@bitCast(@as(i8, @intCast(v))));
            } else {
                try buf.writeByte(@intFromEnum(FormatCode.int));
                try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u32, @as(u32, @bitCast(v)), .big)));
            }
        },
        .long => |v| {
            if (v >= -128 and v <= 127) {
                try buf.writeByte(@intFromEnum(FormatCode.smalllong));
                try buf.writeByte(@bitCast(@as(i8, @intCast(v))));
            } else {
                try buf.writeByte(@intFromEnum(FormatCode.long));
                try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u64, @as(u64, @bitCast(v)), .big)));
            }
        },
        .float => |v| {
            try buf.writeByte(@intFromEnum(FormatCode.float));
            try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u32, @as(u32, @bitCast(v)), .big)));
        },
        .double => |v| {
            try buf.writeByte(@intFromEnum(FormatCode.double));
            try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u64, @as(u64, @bitCast(v)), .big)));
        },
        .char => |v| {
            try buf.writeByte(@intFromEnum(FormatCode.char));
            try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u32, @as(u32, v), .big)));
        },
        .timestamp => |v| {
            try buf.writeByte(@intFromEnum(FormatCode.timestamp));
            try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u64, @as(u64, @bitCast(v)), .big)));
        },
        .uuid => |v| {
            try buf.writeByte(@intFromEnum(FormatCode.uuid));
            try buf.writeAll(&v);
        },
        .binary => |v| try writeVariable(buf, v, FormatCode.binary_8, FormatCode.binary_32),
        .string => |v| try writeVariable(buf, v, FormatCode.string_8, FormatCode.string_32),
        .symbol => |v| try writeVariable(buf, v, FormatCode.symbol_8, FormatCode.symbol_32),
        .list => |items| {
            if (items.len == 0) {
                try buf.writeByte(@intFromEnum(FormatCode.list_0));
                return;
            }

            const layout = try Compound.init(items.len, try listBodySize(items));
            try layout.write(buf, .list_8, .list_32);
            for (items) |item| {
                try encode(item, buf);
            }
        },
        .map => |entries| {
            const layout = try Compound.init(entries.len * 2, try mapBodySize(entries));
            try layout.write(buf, .map_8, .map_32);
            for (entries) |entry| {
                try encode(entry.key, buf);
                try encode(entry.value, buf);
            }
        },
        .array => |items| {
            // Every element shares one constructor, so it has to describe all
            // of them, not just the first.
            const element_code = try arrayConstructor(items);
            const layout = try Compound.init(items.len, try arrayBodySize(items, element_code));
            try layout.write(buf, .array_8, .array_32);
            try buf.writeByte(@intFromEnum(element_code));
            for (items) |item| {
                try encodeElement(item, element_code, buf);
            }
        },
        .described => |d| {
            try buf.writeByte(0x00); // described type constructor
            try encode(d.descriptor.*, buf);
            try encode(d.value.*, buf);
        },
    }
}

/// Compute the encoded size of a value without writing it.
///
/// Exact: `encode` writes precisely this many bytes. The compound arms rely on
/// it to size their headers, so the two must agree — `encodedSize agrees with
/// encode` in the tests below pins that down.
pub fn encodedSize(value: AmqpValue) EncodeError!usize {
    return switch (value) {
        .null, .boolean => 1,
        .ubyte, .byte => 2,
        .ushort, .short => 3,
        .uint => |v| if (v == 0) @as(usize, 1) else if (v <= 0xFF) 2 else 5,
        .ulong => |v| if (v == 0) @as(usize, 1) else if (v <= 0xFF) 2 else 9,
        .int => |v| if (v >= -128 and v <= 127) @as(usize, 2) else 5,
        .long => |v| if (v >= -128 and v <= 127) @as(usize, 2) else 9,
        .float, .char => 5,
        .double, .timestamp => 9,
        .uuid => 17,
        .binary, .string, .symbol => |v| 1 + variableSize(v.len),
        .list => |items| blk: {
            if (items.len == 0) break :blk 1;
            const layout = try Compound.init(items.len, try listBodySize(items));
            break :blk layout.headerSize() + layout.body;
        },
        .map => |entries| blk: {
            const layout = try Compound.init(entries.len * 2, try mapBodySize(entries));
            break :blk layout.headerSize() + layout.body;
        },
        .array => |items| blk: {
            const element_code = try arrayConstructor(items);
            const layout = try Compound.init(items.len, try arrayBodySize(items, element_code));
            break :blk layout.headerSize() + layout.body;
        },
        .described => |d| 1 + try encodedSize(d.descriptor.*) + try encodedSize(d.value.*),
    };
}

fn variableSize(len: usize) usize {
    return if (len <= 0xFF) 1 + len else 4 + len;
}

fn listBodySize(items: []const AmqpValue) EncodeError!usize {
    var total: usize = 0;
    for (items) |item| total += try encodedSize(item);
    return total;
}

fn mapBodySize(entries: []const MapEntry) EncodeError!usize {
    var total: usize = 0;
    for (entries) |entry| {
        total += try encodedSize(entry.key);
        total += try encodedSize(entry.value);
    }
    return total;
}

/// Size of an array body: the shared constructor byte plus every element
/// written without its own constructor.
fn arrayBodySize(items: []const AmqpValue, element_code: FormatCode) EncodeError!usize {
    var total: usize = 1;
    for (items) |item| total += try elementSize(item, element_code);
    return total;
}

fn elementSize(value: AmqpValue, element_code: FormatCode) EncodeError!usize {
    return switch (value) {
        .null => 0,
        .boolean => 1,
        .ubyte, .byte => 1,
        .ushort, .short => 2,
        .uint, .int, .float, .char => 4,
        .ulong, .long, .double, .timestamp => 8,
        .uuid => 16,
        .binary, .string, .symbol => |v| blk: {
            const width = try elementWidth(v.len, element_code);
            break :blk width + v.len;
        },
        .list => |items| 8 + try listBodySize(items),
        .map => |entries| 8 + try mapBodySize(entries),
        .array => |items| blk: {
            const nested_code = try arrayConstructor(items);
            break :blk 8 + try arrayBodySize(items, nested_code);
        },
        .described => error.UnsupportedArrayElement,
    };
}

/// Bytes the shared constructor spends on each element's length prefix.
fn elementWidth(len: usize, element_code: FormatCode) EncodeError!usize {
    return switch (element_code) {
        .binary_8, .string_8, .symbol_8 => if (len > 0xFF) error.ValueTooLarge else 1,
        else => if (len > max_field) error.ValueTooLarge else 4,
    };
}

/// Write one array element: its data, but not its constructor, which the array
/// wrote once up front. Variable-width elements still carry their own length —
/// the shared constructor names the type and the width of that length field,
/// not the length itself.
fn encodeElement(value: AmqpValue, element_code: FormatCode, buf: *Buffer) EncodeError!void {
    switch (value) {
        .null => {},
        .boolean => |v| try buf.writeByte(if (v) 1 else 0),
        .ubyte => |v| try buf.writeByte(v),
        .byte => |v| try buf.writeByte(@bitCast(v)),
        .ushort => |v| try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u16, v, .big))),
        .short => |v| try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u16, @as(u16, @bitCast(v)), .big))),
        .uint => |v| try writeU32(buf, v),
        .int => |v| try writeU32(buf, @bitCast(v)),
        .float => |v| try writeU32(buf, @bitCast(v)),
        .char => |v| try writeU32(buf, @as(u32, v)),
        .ulong => |v| try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u64, v, .big))),
        .long => |v| try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u64, @as(u64, @bitCast(v)), .big))),
        .double => |v| try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u64, @as(u64, @bitCast(v)), .big))),
        .timestamp => |v| try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u64, @as(u64, @bitCast(v)), .big))),
        .uuid => |v| try buf.writeAll(&v),
        .binary, .string, .symbol => |v| {
            if (try elementWidth(v.len, element_code) == 1) {
                try buf.writeByte(@intCast(v.len));
            } else {
                try writeU32(buf, @intCast(v.len));
            }
            try buf.writeAll(v);
        },
        .list => |items| {
            const body = try listBodySize(items);
            try writeWideCompound(buf, body, items.len);
            for (items) |item| try encode(item, buf);
        },
        .map => |entries| {
            const body = try mapBodySize(entries);
            try writeWideCompound(buf, body, entries.len * 2);
            for (entries) |entry| {
                try encode(entry.key, buf);
                try encode(entry.value, buf);
            }
        },
        .array => |items| {
            const nested_code = try arrayConstructor(items);
            const body = try arrayBodySize(items, nested_code);
            try writeWideCompound(buf, body, items.len);
            try buf.writeByte(@intFromEnum(nested_code));
            for (items) |item| try encodeElement(item, nested_code, buf);
        },
        .described => return error.UnsupportedArrayElement,
    }
}

/// Compound elements of an array always take the 32-bit form, so that one
/// shared constructor fits every element regardless of size.
fn writeWideCompound(buf: *Buffer, body: usize, count: usize) EncodeError!void {
    if (body + 4 > max_field or count > max_field) return error.ValueTooLarge;
    try writeU32(buf, @intCast(body + 4));
    try writeU32(buf, @intCast(count));
}

/// Pick the one constructor that describes every element of an array.
///
/// Variable-width elements share a width, so it has to fit the longest one;
/// compound elements always take the 32-bit form for the same reason.
fn arrayConstructor(items: []const AmqpValue) EncodeError!FormatCode {
    // An empty array still needs an element constructor, and there is no
    // element to take it from. Null describes nothing, which is accurate.
    if (items.len == 0) return .null;

    const Tag = std.meta.Tag(AmqpValue);
    const tag: Tag = items[0];
    for (items[1..]) |item| {
        if (@as(Tag, item) != tag) return error.MixedArrayElements;
    }

    return switch (items[0]) {
        // A null element occupies no bytes, so a count of four billion of them
        // encodes in four bytes. The decoder cannot bound that allocation from
        // the input, so refuse to produce it — a null array carries no
        // information anyway.
        .null => error.UnsupportedArrayElement,
        // Not boolean_true/boolean_false: those are zero-width, so they could
        // not distinguish the elements from each other.
        .boolean => .boolean,
        .ubyte => .ubyte,
        .byte => .byte,
        .ushort => .ushort,
        .short => .short,
        // The compact integer forms cannot be shared either: each element gets
        // the full width so that every value in the array fits.
        .uint => .uint,
        .int => .int,
        .ulong => .ulong,
        .long => .long,
        .float => .float,
        .double => .double,
        .char => .char,
        .timestamp => .timestamp,
        .uuid => .uuid,
        .binary => if (allShort(items)) .binary_8 else .binary_32,
        .string => if (allShort(items)) .string_8 else .string_32,
        .symbol => if (allShort(items)) .symbol_8 else .symbol_32,
        .list => .list_32,
        .map => .map_32,
        .array => .array_32,
        .described => error.UnsupportedArrayElement,
    };
}

fn allShort(items: []const AmqpValue) bool {
    for (items) |item| {
        const len = switch (item) {
            .binary, .string, .symbol => |v| v.len,
            else => return false,
        };
        if (len > 0xFF) return false;
    }
    return true;
}

fn writeVariable(buf: *Buffer, data: []const u8, small_code: FormatCode, large_code: FormatCode) !void {
    if (data.len <= 0xFF) {
        try buf.writeByte(@intFromEnum(small_code));
        try buf.writeByte(@intCast(data.len));
    } else {
        try buf.writeByte(@intFromEnum(large_code));
        try buf.writeAll(&std.mem.toBytes(std.mem.nativeTo(u32, @intCast(data.len), .big)));
    }
    try buf.writeAll(data);
}

// ── Tests ──────────────────────────────────────────────────────────────

test "encode null" {
    var buf_arr: [1]u8 = undefined;
    var buf = Buffer.initFixed(&buf_arr);
    try encode(.null, &buf);
    try std.testing.expectEqual(@as(u8, 0x40), buf.written()[0]);
}

test "encode boolean" {
    var buf_arr: [1]u8 = undefined;
    var buf = Buffer.initFixed(&buf_arr);
    try encode(.{ .boolean = true }, &buf);
    try std.testing.expectEqual(@as(u8, 0x41), buf.written()[0]);

    buf.reset();
    try encode(.{ .boolean = false }, &buf);
    try std.testing.expectEqual(@as(u8, 0x42), buf.written()[0]);
}

test "encode uint compact forms" {
    // uint_0
    {
        var buf_arr: [1]u8 = undefined;
        var buf = Buffer.initFixed(&buf_arr);
        try encode(.{ .uint = 0 }, &buf);
        try std.testing.expectEqual(@as(u8, 0x43), buf.written()[0]);
    }
    // smalluint
    {
        var buf_arr: [2]u8 = undefined;
        var buf = Buffer.initFixed(&buf_arr);
        try encode(.{ .uint = 200 }, &buf);
        try std.testing.expectEqual(@as(u8, 0x52), buf.written()[0]);
        try std.testing.expectEqual(@as(u8, 200), buf.written()[1]);
    }
    // uint
    {
        var buf_arr: [5]u8 = undefined;
        var buf = Buffer.initFixed(&buf_arr);
        try encode(.{ .uint = 0x12345678 }, &buf);
        try std.testing.expectEqual(@as(u8, 0x70), buf.written()[0]);
        try std.testing.expectEqualSlices(u8, &.{ 0x12, 0x34, 0x56, 0x78 }, buf.written()[1..5]);
    }
}

test "encode string" {
    var buf_arr: [32]u8 = undefined;
    var buf = Buffer.initFixed(&buf_arr);
    try encode(.{ .string = "hello" }, &buf);
    const w = buf.written();
    try std.testing.expectEqual(@as(u8, 0xa1), w[0]); // string_8
    try std.testing.expectEqual(@as(u8, 5), w[1]); // length
    try std.testing.expectEqualStrings("hello", w[2..7]);
}

test "encode list" {
    var buf_arr: [64]u8 = undefined;
    var buf = Buffer.initFixed(&buf_arr);
    var items = [_]AmqpValue{ .{ .uint = 1 }, .{ .boolean = true }, .null };
    try encode(.{ .list = &items }, &buf);
    try std.testing.expectEqual(@as(u8, 0xc0), buf.written()[0]); // list_8
}

test "encode empty list" {
    var buf_arr: [1]u8 = undefined;
    var buf = Buffer.initFixed(&buf_arr);
    const empty: []AmqpValue = &.{};
    try encode(.{ .list = empty }, &buf);
    try std.testing.expectEqual(@as(u8, 0x45), buf.written()[0]); // list_0
}

// ── Array round trips ──────────────────────────────────────────────────
//
// An array shares one constructor across its elements, so encoding it wrong
// produces bytes no decoder can walk. Every element type gets a round trip.

const decoder = @import("decoder.zig");

fn roundTrip(allocator: Allocator, value: AmqpValue) !AmqpValue {
    var buf = Buffer.initDynamic(allocator);
    defer buf.deinit();
    try encode(value, &buf);

    // encode and encodedSize must agree, or the compound headers are wrong.
    try std.testing.expectEqual(try encodedSize(value), buf.pos);

    const result = try decoder.decode(allocator, buf.written());
    // Nothing may be left over: a decoder that stops short would hide a
    // length field that does not describe the bytes that follow.
    try std.testing.expectEqual(buf.pos, result.bytes_consumed);
    return result.value;
}

fn expectRoundTrip(value: AmqpValue) !void {
    const allocator = std.testing.allocator;
    var decoded = try roundTrip(allocator, value);
    defer decoded.deinit(allocator);
    try std.testing.expect(value.eql(decoded));
}

test "arrays of every element type round trip" {
    var symbols = [_]AmqpValue{ .{ .symbol = "ABC" }, .{ .symbol = "DE" } };
    var strings = [_]AmqpValue{ .{ .string = "first" }, .{ .string = "second" } };
    var binaries = [_]AmqpValue{ .{ .binary = "\x01\x02" }, .{ .binary = "\x03" } };
    var booleans = [_]AmqpValue{ .{ .boolean = true }, .{ .boolean = false } };
    var ubytes = [_]AmqpValue{ .{ .ubyte = 1 }, .{ .ubyte = 2 }, .{ .ubyte = 3 } };
    var bytes = [_]AmqpValue{ .{ .byte = -1 }, .{ .byte = 2 } };
    var ushorts = [_]AmqpValue{ .{ .ushort = 1 }, .{ .ushort = 65535 } };
    var shorts = [_]AmqpValue{ .{ .short = -1 }, .{ .short = 300 } };
    // Values that would take the compact form on their own: inside an array
    // they must still be written at the shared constructor's full width.
    var uints = [_]AmqpValue{ .{ .uint = 0 }, .{ .uint = 7 }, .{ .uint = 0x12345678 } };
    var ulongs = [_]AmqpValue{ .{ .ulong = 0 }, .{ .ulong = 0xDEADBEEF } };
    var ints = [_]AmqpValue{ .{ .int = -1 }, .{ .int = 100000 } };
    var longs = [_]AmqpValue{ .{ .long = -2 }, .{ .long = 1 << 40 } };
    var floats = [_]AmqpValue{ .{ .float = 1.5 }, .{ .float = -2.5 } };
    var doubles = [_]AmqpValue{ .{ .double = 1.5 }, .{ .double = -0.25 } };
    var chars = [_]AmqpValue{ .{ .char = 'a' }, .{ .char = 0x1F600 } };
    var timestamps = [_]AmqpValue{ .{ .timestamp = 1234567890 }, .{ .timestamp = -1 } };
    var uuids = [_]AmqpValue{ .{ .uuid = [_]u8{0xAB} ** 16 }, .{ .uuid = [_]u8{0xCD} ** 16 } };

    for ([_][]AmqpValue{
        &symbols, &strings, &binaries, &booleans,   &ubytes, &bytes,
        &ushorts, &shorts,  &uints,    &ulongs,     &ints,   &longs,
        &floats,  &doubles, &chars,    &timestamps, &uuids,
    }) |items| {
        try expectRoundTrip(.{ .array = items });
    }
}

test "an array of symbols keeps its element boundaries" {
    const allocator = std.testing.allocator;
    var items = [_]AmqpValue{ .{ .symbol = "ABC" }, .{ .symbol = "DE" } };

    var buf = Buffer.initDynamic(allocator);
    defer buf.deinit();
    try encode(.{ .array = &items }, &buf);

    // array8, size, count, shared symbol8 constructor, then each element with
    // its own length. Without the lengths this decodes as one 4-byte size.
    try std.testing.expectEqualSlices(u8, &.{
        0xe0, 0x09, 0x02, 0xa3, 0x03, 'A', 'B', 'C', 0x02, 'D', 'E',
    }, buf.written());

    var decoded = try roundTrip(allocator, .{ .array = &items });
    defer decoded.deinit(allocator);
    try std.testing.expectEqualStrings("ABC", decoded.array[0].symbol);
    try std.testing.expectEqualStrings("DE", decoded.array[1].symbol);
}

test "an array constructor fits the longest element" {
    const long_symbol = "s" ** 300;
    var items = [_]AmqpValue{ .{ .symbol = "short" }, .{ .symbol = long_symbol } };

    const allocator = std.testing.allocator;
    var buf = Buffer.initDynamic(allocator);
    defer buf.deinit();
    try encode(.{ .array = &items }, &buf);
    // symbol32, because the 8-bit form cannot express the second element.
    try std.testing.expectEqual(@as(u8, 0xb3), buf.written()[9]);

    try expectRoundTrip(.{ .array = &items });
}

test "arrays of compound elements round trip" {
    var inner_a = [_]AmqpValue{.{ .ubyte = 1 }};
    var inner_b = [_]AmqpValue{.{ .ubyte = 2 }};
    var lists = [_]AmqpValue{ .{ .list = &inner_a }, .{ .list = &inner_b } };
    try expectRoundTrip(.{ .array = &lists });

    var entries_a = [_]MapEntry{.{ .key = .{ .symbol = "k" }, .value = .{ .uint = 1 } }};
    var entries_b = [_]MapEntry{.{ .key = .{ .symbol = "j" }, .value = .{ .uint = 2 } }};
    var maps = [_]AmqpValue{ .{ .map = &entries_a }, .{ .map = &entries_b } };
    try expectRoundTrip(.{ .array = &maps });

    var nested = [_]AmqpValue{ .{ .array = &inner_a }, .{ .array = &inner_b } };
    try expectRoundTrip(.{ .array = &nested });
}

test "an empty array stays an array" {
    const empty: []AmqpValue = &.{};
    const allocator = std.testing.allocator;

    var buf = Buffer.initDynamic(allocator);
    defer buf.deinit();
    try encode(.{ .array = empty }, &buf);
    // array8, size 2 (count byte + constructor), count 0, null constructor —
    // not 0x45, which is an empty *list* and decodes back as one.
    try std.testing.expectEqualSlices(u8, &.{ 0xe0, 0x02, 0x00, 0x40 }, buf.written());

    var decoded = try roundTrip(allocator, .{ .array = empty });
    defer decoded.deinit(allocator);
    try std.testing.expect(decoded == .array);
    try std.testing.expectEqual(@as(usize, 0), decoded.array.len);
}

test "a mixed array is rejected rather than silently mis-encoded" {
    const allocator = std.testing.allocator;
    var buf = Buffer.initDynamic(allocator);
    defer buf.deinit();

    var mixed = [_]AmqpValue{ .{ .symbol = "a" }, .{ .uint = 1 } };
    try std.testing.expectError(error.MixedArrayElements, encode(.{ .array = &mixed }, &buf));
    try std.testing.expectError(error.MixedArrayElements, encodedSize(.{ .array = &mixed }));

    var descriptor: AmqpValue = .{ .ulong = 0x13 };
    var described_value: AmqpValue = .null;
    var described = [_]AmqpValue{.{ .described = .{ .descriptor = &descriptor, .value = &described_value } }};
    try std.testing.expectError(error.UnsupportedArrayElement, encode(.{ .array = &described }, &buf));

    // Null elements are zero-width, so the count would be the only bound on
    // what a decoder allocates. Refuse to write bytes we would not accept.
    var nulls = [_]AmqpValue{ .null, .null };
    try std.testing.expectError(error.UnsupportedArrayElement, encode(.{ .array = &nulls }, &buf));
    try std.testing.expectError(error.UnsupportedArrayElement, encodedSize(.{ .array = &nulls }));
}

// ── Compound size fields ───────────────────────────────────────────────

test "a 255-byte body does not panic" {
    // 127 ubytes at 2 bytes each plus a 1-byte boolean is exactly 255 bytes,
    // which needs a size field of 256 — one past what a byte holds.
    var items: [128]AmqpValue = undefined;
    for (items[0..127]) |*slot| slot.* = .{ .ubyte = 7 };
    items[127] = .{ .boolean = true };

    const allocator = std.testing.allocator;
    var buf = Buffer.initDynamic(allocator);
    defer buf.deinit();
    try encode(.{ .list = &items }, &buf);
    try std.testing.expectEqual(@as(u8, 0xd0), buf.written()[0]); // list_32

    try expectRoundTrip(.{ .list = &items });
}

test "compound bodies switch width at the size field, not the body" {
    const allocator = std.testing.allocator;

    // Walk a list body across 254, 255, and 256 bytes.
    var items: [129]AmqpValue = undefined;
    for (&items) |*slot| slot.* = .{ .ubyte = 0 };
    for ([_]struct { count: usize, code: u8 }{
        .{ .count = 127, .code = 0xc0 }, // body 254, size 255: still fits
        .{ .count = 128, .code = 0xd0 }, // body 256, size 257: does not
    }) |case| {
        var buf = Buffer.initDynamic(allocator);
        defer buf.deinit();
        try encode(.{ .list = items[0..case.count] }, &buf);
        try std.testing.expectEqual(case.code, buf.written()[0]);
        try expectRoundTrip(.{ .list = items[0..case.count] });
    }

    // Maps and arrays share the guard.
    var entries: [86]MapEntry = undefined;
    for (&entries) |*slot| slot.* = .{ .key = .{ .ubyte = 1 }, .value = .{ .ubyte = 2 } };
    try expectRoundTrip(.{ .map = &entries });

    var array_items: [254]AmqpValue = undefined;
    for (&array_items) |*slot| slot.* = .{ .ubyte = 9 };
    try expectRoundTrip(.{ .array = &array_items });
}

test "encodedSize agrees with encode for every type" {
    var list_items = [_]AmqpValue{ .{ .uint = 1 }, .{ .string = "two" } };
    var map_entries = [_]MapEntry{.{ .key = .{ .symbol = "k" }, .value = .{ .boolean = false } }};
    var array_items = [_]AmqpValue{ .{ .symbol = "x" }, .{ .symbol = "yz" } };
    var descriptor: AmqpValue = .{ .ulong = 0x73 };
    var described_body: AmqpValue = .{ .list = &list_items };

    for ([_]AmqpValue{
        .null,
        .{ .boolean = true },
        .{ .ubyte = 5 },
        .{ .byte = -5 },
        .{ .ushort = 500 },
        .{ .short = -500 },
        .{ .uint = 0 },
        .{ .uint = 42 },
        .{ .uint = 0x12345678 },
        .{ .int = -1 },
        .{ .int = 70000 },
        .{ .ulong = 0 },
        .{ .ulong = 42 },
        .{ .ulong = 1 << 40 },
        .{ .long = -1 },
        .{ .long = -(1 << 40) },
        .{ .float = 1.25 },
        .{ .double = 1.25 },
        .{ .char = 'A' },
        .{ .timestamp = 999 },
        .{ .uuid = [_]u8{7} ** 16 },
        .{ .binary = "bytes" },
        .{ .string = "hello" },
        .{ .symbol = "amqp:accepted:list" },
        .{ .string = "l" ** 300 },
        .{ .list = &.{} },
        .{ .list = &list_items },
        .{ .map = &map_entries },
        .{ .array = &array_items },
        .{ .described = .{ .descriptor = &descriptor, .value = &described_body } },
    }) |value| {
        try expectRoundTrip(value);
    }
}
