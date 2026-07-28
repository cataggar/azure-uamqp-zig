//! The parts of an AMQP client that are the same whichever direction the
//! messages go: a socket, a clock, and a loop that feeds received bytes to
//! the library and waits for something to become true.
//!
//! The library does no I/O and reads no clock of its own -- both are handed
//! to it -- so this file is what a real application has to supply. It is the
//! uninteresting half of both examples, kept here so the interesting half
//! stays readable.

const std = @import("std");
const uamqp = @import("uamqp");

pub const Connection = uamqp.connection.Connection;
pub const Session = uamqp.session.Session;
pub const Link = uamqp.link.Link;
pub const Message = uamqp.message.Message;
pub const SaslClientIo = uamqp.sasl.client_io.SaslClientIo;

/// Where to connect and as whom. Read from the environment so the examples
/// can be pointed at a local broker or a cloud one without recompiling.
///
///   AMQP_HOST       default 127.0.0.1
///   AMQP_PORT       default 5672
///   AMQP_USER       SASL PLAIN when set, ANONYMOUS when not
///   AMQP_PASSWORD   default empty
///   AMQP_ADDRESS    default examples
///   AMQP_TIMEOUT_MS overall deadline, default 30000
pub const Config = struct {
    host: []const u8,
    port: u16,
    user: ?[]const u8,
    password: []const u8,
    address: []const u8,
    timeout_ms: i64,

    pub fn fromEnv(env: *const std.process.Environ.Map) !Config {
        return .{
            .host = env.get("AMQP_HOST") orelse "127.0.0.1",
            .port = try std.fmt.parseInt(u16, env.get("AMQP_PORT") orelse "5672", 10),
            .user = env.get("AMQP_USER"),
            .password = env.get("AMQP_PASSWORD") orelse "",
            .address = env.get("AMQP_ADDRESS") orelse "examples",
            .timeout_ms = try std.fmt.parseInt(i64, env.get("AMQP_TIMEOUT_MS") orelse "30000", 10),
        };
    }
};

/// The socket, and the callback shape the library sends through.
pub const Transport = struct {
    stream: std.Io.net.Stream,
    reader: std.Io.net.Stream.Reader,
    writer: std.Io.net.Stream.Writer,

    pub fn send(context: ?*anyopaque, data: []const u8) anyerror!void {
        const self: *Transport = @ptrCast(@alignCast(context.?));
        try self.writer.interface.writeAll(data);
        try self.writer.interface.flush();
    }

    /// Zero means "nothing has arrived yet", not end of stream. The caller's
    /// deadline is what decides when that has gone on too long.
    pub fn receive(self: *Transport, buf: []u8) !usize {
        var vec: [1][]u8 = .{buf};
        return self.reader.interface.readVec(&vec) catch |err| switch (err) {
            error.EndOfStream => return error.ConnectionClosedByBroker,
            else => return err,
        };
    }
};

/// A monotonic clock, so idle-timeout keep-alives are driven by real time.
pub const Clock = struct {
    io: std.Io,
    start: std.Io.Timestamp,

    pub fn init(io: std.Io) Clock {
        return .{ .io = io, .start = std.Io.Timestamp.now(io, .awake) };
    }

    pub fn readMs(context: ?*anyopaque) i64 {
        const self: *Clock = @ptrCast(@alignCast(context.?));
        const now = std.Io.Timestamp.now(self.io, .awake);
        return @intCast(@divFloor(now.nanoseconds - self.start.nanoseconds, std.time.ns_per_ms));
    }

    pub fn clock(self: *Clock) uamqp.connection.Clock {
        return .{ .context = self, .read_ms = readMs };
    }
};

/// Owns the connection and drives it. `until` is the whole event loop: the
/// library is not threaded and does not block, so something has to keep
/// handing it bytes and asking whether the thing you wanted has happened.
pub const Client = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    cfg: Config,
    transport: *Transport,
    clock: *Clock,
    sasl: *SaslClientIo,
    conn: *Connection,
    sasl_done: bool = false,
    sasl_ok: bool = false,

    /// Read once and hand the bytes to whichever layer owns them. SASL keeps
    /// ownership even after the outcome, because it is what knows where the
    /// AMQP bytes start.
    fn pump(self: *Client) !void {
        var buf: [16 * 1024]u8 = undefined;
        const n = try self.transport.receive(&buf);
        if (n == 0) return;
        return self.sasl.onBytesReceived(buf[0..n]);
    }

    /// Pump until `ready` says so, or the deadline passes.
    pub fn until(
        self: *Client,
        what: []const u8,
        context: *anyopaque,
        ready: *const fn (context: *anyopaque) bool,
    ) !void {
        while (!ready(context)) {
            if (Clock.readMs(self.clock) > self.cfg.timeout_ms) {
                std.debug.print("timed out waiting for {s}\n", .{what});
                return error.Timeout;
            }
            self.conn.doWork() catch |err| {
                std.debug.print("doWork failed: {t}\n", .{err});
            };
            try self.pump();
        }
    }

    pub fn linkAttached(ctx: *anyopaque) bool {
        const l: *Link = @ptrCast(@alignCast(ctx));
        return l.state == .attached;
    }

    fn onSaslComplete(context: ?*anyopaque, success: bool) void {
        const self: *Client = @ptrCast(@alignCast(context.?));
        self.sasl_done = true;
        self.sasl_ok = success;
    }

    fn onAmqpBytes(context: ?*anyopaque, data: []const u8) anyerror!void {
        const self: *Client = @ptrCast(@alignCast(context.?));
        return self.conn.onBytesReceived(data);
    }

    /// Connect, authenticate, open the connection and begin a session --
    /// everything that has to happen before a link can be attached.
    pub fn connect(self: *Client) !void {
        try self.sasl.open();
        try self.until("SASL negotiation", self, struct {
            fn ready(ctx: *anyopaque) bool {
                const c: *Client = @ptrCast(@alignCast(ctx));
                return c.sasl_done;
            }
        }.ready);
        if (!self.sasl_ok) return error.SaslRejected;

        try self.conn.open();
        try self.until("connection open", self.conn, struct {
            fn ready(ctx: *anyopaque) bool {
                const c: *Connection = @ptrCast(@alignCast(ctx));
                return c.state == .opened;
            }
        }.ready);
    }

    pub fn beginSession(self: *Client, session: *Session) !void {
        try session.begin();
        try self.until("session begin", session, struct {
            fn ready(ctx: *anyopaque) bool {
                const s: *Session = @ptrCast(@alignCast(ctx));
                return s.state == .mapped;
            }
        }.ready);
    }

    /// Close in the order the spec asks for, and wait for the peer to agree.
    pub fn close(self: *Client) !void {
        try self.conn.close(null, null);
        try self.until("connection close", self.conn, struct {
            fn ready(ctx: *anyopaque) bool {
                const c: *Connection = @ptrCast(@alignCast(ctx));
                return c.state == .end;
            }
        }.ready);
    }
};

/// Everything a `Client` borrows has to outlive it, which in Zig means the
/// caller owns it. `Setup` is that storage; keep it on the stack of `main`.
pub const Setup = struct {
    transport: Transport,
    clock: Clock,
    conn: Connection,
    sasl_io: SaslClientIo,
    plain: uamqp.sasl.plain.Plain,
    anonymous: uamqp.sasl.anonymous.Anonymous,
    read_buf: [64 * 1024]u8,
    write_buf: [64 * 1024]u8,
    client: Client,

    pub fn deinit(self: *Setup, io: std.Io) void {
        self.sasl_io.deinit();
        self.conn.deinit();
        self.transport.stream.close(io);
    }
};

/// Connect a socket and wire the library to it. Returns a `Client` pointing
/// into `setup`, so `setup` must not move afterwards.
pub fn start(
    setup: *Setup,
    gpa: std.mem.Allocator,
    io: std.Io,
    cfg: Config,
    container_id: []const u8,
) !*Client {
    const addr = try std.Io.net.IpAddress.parse(cfg.host, cfg.port);
    const stream = try addr.connect(io, .{ .mode = .stream });

    setup.transport = .{
        .stream = stream,
        .reader = stream.reader(io, &setup.read_buf),
        .writer = stream.writer(io, &setup.write_buf),
    };
    setup.clock = Clock.init(io);

    setup.conn = Connection.init(gpa, container_id, cfg.host, .{
        .max_frame_size = 65536,
        .idle_timeout_ms = 60_000,
        .clock = setup.clock.clock(),
    });
    setup.conn.setIo(Transport.send, &setup.transport);

    setup.sasl_io = if (cfg.user) |user| blk: {
        setup.plain = uamqp.sasl.plain.Plain.init(gpa, user, cfg.password, null);
        break :blk SaslClientIo.init(gpa, setup.plain.mechanism(), cfg.host);
    } else blk: {
        setup.anonymous = .{};
        break :blk SaslClientIo.init(gpa, setup.anonymous.mechanism(), cfg.host);
    };

    setup.client = .{
        .gpa = gpa,
        .io = io,
        .cfg = cfg,
        .transport = &setup.transport,
        .clock = &setup.clock,
        .sasl = &setup.sasl_io,
        .conn = &setup.conn,
    };

    setup.sasl_io.setIo(Transport.send, &setup.transport);
    setup.sasl_io.setOnAmqpBytes(Client.onAmqpBytes, &setup.client);
    setup.sasl_io.on_open_complete = Client.onSaslComplete;
    setup.sasl_io.on_open_complete_context = &setup.client;

    return &setup.client;
}
