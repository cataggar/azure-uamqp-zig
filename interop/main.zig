//! Interop check against a real AMQP 1.0 broker.
//!
//! Everything else in this repository is tested against a scripted peer
//! written from the same reading of the spec as the code it is checking, so
//! the two can agree and both be wrong. This connects a socket to a broker
//! nobody here wrote, sends a message and reads it back.
//!
//! Configured by environment, so nothing has to be quoted through a build
//! step:
//!
//!   AMQP_HOST      default 127.0.0.1
//!   AMQP_PORT      default 5672
//!   AMQP_USER      SASL PLAIN when set, ANONYMOUS when not
//!   AMQP_PASSWORD  default empty
//!   AMQP_ADDRESS   default interop-test
//!   AMQP_TIMEOUT_MS overall deadline, default 30000
const std = @import("std");
const uamqp = @import("uamqp");

const Connection = uamqp.connection.Connection;
const Session = uamqp.session.Session;
const Link = uamqp.link.Link;
const Message = uamqp.message.Message;
const MessageSender = uamqp.message_sender.MessageSender;
const MessageReceiver = uamqp.message_receiver.MessageReceiver;
const SaslClientIo = uamqp.sasl.client_io.SaslClientIo;

const log = std.log.scoped(.interop);

pub const std_options: std.Options = .{ .log_level = .debug };

/// The socket, and the two callbacks the library reaches it through.
const Transport = struct {
    io: std.Io,
    stream: std.Io.net.Stream,
    reader: std.Io.net.Stream.Reader,
    writer: std.Io.net.Stream.Writer,
    bytes_sent: usize = 0,
    bytes_received: usize = 0,

    fn send(context: ?*anyopaque, data: []const u8) anyerror!void {
        const self: *Transport = @ptrCast(@alignCast(context.?));
        try self.writer.interface.writeAll(data);
        try self.writer.interface.flush();
        self.bytes_sent += data.len;
    }

    /// Read whatever has arrived. Zero is not end of stream, only "nothing
    /// yet"; the caller's deadline decides when that has gone on too long.
    fn receive(self: *Transport, buf: []u8) !usize {
        var vec: [1][]u8 = .{buf};
        const n = self.reader.interface.readVec(&vec) catch |err| switch (err) {
            error.EndOfStream => return error.ConnectionClosedByBroker,
            else => return err,
        };
        self.bytes_received += n;
        return n;
    }
};

/// A monotonic clock for the connection, so idle-timeout keep-alives are
/// driven by real time rather than a test's manual ticks.
const Clock = struct {
    io: std.Io,
    start: std.Io.Timestamp,

    fn init(io: std.Io) Clock {
        return .{ .io = io, .start = std.Io.Timestamp.now(io, .awake) };
    }

    fn readMs(context: ?*anyopaque) i64 {
        const self: *Clock = @ptrCast(@alignCast(context.?));
        const now = std.Io.Timestamp.now(self.io, .awake);
        return @intCast(@divFloor(now.nanoseconds - self.start.nanoseconds, std.time.ns_per_ms));
    }

    fn clock(self: *Clock) uamqp.connection.Clock {
        return .{ .context = self, .read_ms = readMs };
    }
};

const Config = struct {
    host: []const u8,
    port: u16,
    user: ?[]const u8,
    password: []const u8,
    address: []const u8,
    timeout_ms: i64,

    fn fromEnv(env: *const std.process.Environ.Map) !Config {
        const port_text = env.get("AMQP_PORT") orelse "5672";
        const timeout_text = env.get("AMQP_TIMEOUT_MS") orelse "30000";
        return .{
            .host = env.get("AMQP_HOST") orelse "127.0.0.1",
            .port = try std.fmt.parseInt(u16, port_text, 10),
            .user = env.get("AMQP_USER"),
            .password = env.get("AMQP_PASSWORD") orelse "",
            .address = env.get("AMQP_ADDRESS") orelse "interop-test",
            .timeout_ms = try std.fmt.parseInt(i64, timeout_text, 10),
        };
    }
};

/// What the receiver saw, filled in by the message handler.
const Received = struct {
    body: [256]u8 = undefined,
    len: usize = 0,
    subject: [64]u8 = undefined,
    subject_len: usize = 0,
    count: usize = 0,

    fn onMessage(context: ?*anyopaque, msg: *const Message) ?uamqp.definitions.DeliveryState {
        const self: *Received = @ptrCast(@alignCast(context.?));
        self.count += 1;
        if (msg.body_data_sections.items.len > 0) {
            const body = msg.body_data_sections.items[0].bytes;
            const n = @min(body.len, self.body.len);
            @memcpy(self.body[0..n], body[0..n]);
            self.len = n;
        }
        if (msg.properties) |props| if (props.subject) |subject| {
            const n = @min(subject.len, self.subject.len);
            @memcpy(self.subject[0..n], subject[0..n]);
            self.subject_len = n;
        };
        return .accepted;
    }
};

const SendOutcome = struct {
    done: bool = false,
    result: ?uamqp.message_sender.MessageSendResult = null,

    fn onComplete(
        context: ?*anyopaque,
        result: uamqp.message_sender.MessageSendResult,
        _: ?uamqp.definitions.DeliveryState,
    ) void {
        const self: *SendOutcome = @ptrCast(@alignCast(context.?));
        self.done = true;
        self.result = result;
    }
};

/// Everything the run needs to keep alive, and the pump that feeds bytes in.
const Client = struct {
    allocator: std.mem.Allocator,
    transport: *Transport,
    clock: *Clock,
    sasl: ?*SaslClientIo,
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
        if (self.sasl) |s| return s.onBytesReceived(buf[0..n]);
        return self.conn.onBytesReceived(buf[0..n]);
    }

    /// Pump until `ready` says so, or the deadline passes.
    fn until(
        self: *Client,
        what: []const u8,
        deadline_ms: i64,
        context: *anyopaque,
        ready: *const fn (context: *anyopaque) bool,
    ) !void {
        while (!ready(context)) {
            if (Clock.readMs(self.clock) > deadline_ms) {
                log.err("timed out waiting for {s}", .{what});
                return error.Timeout;
            }
            self.conn.doWork() catch |err| {
                log.warn("doWork failed: {t}", .{err});
            };
            try self.pump();
        }
        log.info("{s}: ok", .{what});
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
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const cfg = try Config.fromEnv(init.environ_map);

    log.info("connecting to {s}:{d} as {s}, address {s}", .{
        cfg.host,
        cfg.port,
        cfg.user orelse "(anonymous)",
        cfg.address,
    });

    const addr = try std.Io.net.IpAddress.parse(cfg.host, cfg.port);
    const stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);

    var read_buf: [64 * 1024]u8 = undefined;
    var write_buf: [64 * 1024]u8 = undefined;
    var transport: Transport = .{
        .io = io,
        .stream = stream,
        .reader = stream.reader(io, &read_buf),
        .writer = stream.writer(io, &write_buf),
    };

    var clock = Clock.init(io);
    const deadline = cfg.timeout_ms;

    var conn = Connection.init(gpa, "azure-uamqp-zig-interop", cfg.host, .{
        .max_frame_size = 65536,
        .idle_timeout_ms = 60_000,
        .clock = clock.clock(),
    });
    defer conn.deinit();
    conn.setIo(Transport.send, &transport);

    var plain: uamqp.sasl.plain.Plain = undefined;
    var anonymous: uamqp.sasl.anonymous.Anonymous = undefined;
    var sasl_io: SaslClientIo = if (cfg.user) |user| blk: {
        plain = uamqp.sasl.plain.Plain.init(gpa, user, cfg.password, null);
        break :blk SaslClientIo.init(gpa, plain.mechanism(), cfg.host);
    } else blk: {
        anonymous = .{};
        break :blk SaslClientIo.init(gpa, anonymous.mechanism(), cfg.host);
    };
    defer sasl_io.deinit();

    var client: Client = .{
        .allocator = gpa,
        .transport = &transport,
        .clock = &clock,
        .sasl = &sasl_io,
        .conn = &conn,
    };

    sasl_io.setIo(Transport.send, &transport);
    sasl_io.setOnAmqpBytes(Client.onAmqpBytes, &client);
    sasl_io.on_open_complete = Client.onSaslComplete;
    sasl_io.on_open_complete_context = &client;

    try sasl_io.open();
    try client.until("SASL negotiation", deadline, &client, struct {
        fn ready(ctx: *anyopaque) bool {
            const c: *Client = @ptrCast(@alignCast(ctx));
            return c.sasl_done;
        }
    }.ready);
    if (!client.sasl_ok) return error.SaslRejected;

    try conn.open();
    try client.until("connection open", deadline, &conn, struct {
        fn ready(ctx: *anyopaque) bool {
            const c: *Connection = @ptrCast(@alignCast(ctx));
            return c.state == .opened;
        }
    }.ready);

    var session = Session.init(gpa, &conn, .{});
    defer session.deinit();
    try session.begin();
    try client.until("session begin", deadline, &session, struct {
        fn ready(ctx: *anyopaque) bool {
            const s: *Session = @ptrCast(@alignCast(ctx));
            return s.state == .mapped;
        }
    }.ready);

    // The receiver attaches first: on a broker that treats an unknown address
    // as a topic, a message sent before anything subscribes goes nowhere.
    var receiver_link = try Link.init(
        gpa,
        &session,
        "interop-receiver",
        .receiver,
        .{ .address = cfg.address },
        .{ .address = "interop-client" },
    );
    defer receiver_link.deinit();

    var received: Received = .{};
    var receiver = MessageReceiver.init(gpa, &receiver_link, .{ .credit = 10 });
    defer receiver.deinit();
    try receiver.open(Received.onMessage, &received);
    try client.until("receiver attach", deadline, &receiver_link, linkAttached);

    var sender_link = try Link.init(
        gpa,
        &session,
        "interop-sender",
        .sender,
        .{ .address = "interop-client" },
        .{ .address = cfg.address },
    );
    defer sender_link.deinit();

    var sender = MessageSender.init(gpa, &sender_link);
    defer sender.deinit();
    try sender.open();
    try client.until("sender attach", deadline, &sender_link, linkAttached);

    var msg = Message.init(gpa);
    defer msg.deinit();
    msg.properties = .{ .subject = "interop", .content_type = "text/plain" };
    try msg.setApplicationProperty("sent-by", "azure-uamqp-zig");
    try msg.addBodyData("round trip");

    var outcome: SendOutcome = .{};
    _ = try sender.send(&msg, .{ .on_complete = SendOutcome.onComplete, .context = &outcome });
    try client.until("message settled by the broker", deadline, &outcome, struct {
        fn ready(ctx: *anyopaque) bool {
            const o: *SendOutcome = @ptrCast(@alignCast(ctx));
            return o.done;
        }
    }.ready);
    if (outcome.result != .ok) {
        log.err("the broker did not accept the message: {?}", .{outcome.result});
        return error.MessageRejected;
    }

    try client.until("message delivered back", deadline, &received, struct {
        fn ready(ctx: *anyopaque) bool {
            const r: *Received = @ptrCast(@alignCast(ctx));
            return r.count > 0;
        }
    }.ready);

    const body = received.body[0..received.len];
    const subject = received.subject[0..received.subject_len];
    log.info("received {d} byte(s): '{s}', subject '{s}'", .{ body.len, body, subject });
    if (!std.mem.eql(u8, body, "round trip")) return error.BodyMismatch;
    if (!std.mem.eql(u8, subject, "interop")) return error.SubjectMismatch;

    // Shut down in the order the spec asks for, and wait for the peer to
    // agree: a broker that rejects the teardown is as much a failure as one
    // that rejects the message.
    receiver.close();
    try sender_link.detach(true, null);
    try session.end(null);
    try conn.close(null, null);
    try client.until("connection close", deadline, &conn, struct {
        fn ready(ctx: *anyopaque) bool {
            const c: *Connection = @ptrCast(@alignCast(ctx));
            return c.state == .end;
        }
    }.ready);

    log.info("interop ok: {d} bytes out, {d} bytes in", .{
        transport.bytes_sent,
        transport.bytes_received,
    });
}

fn linkAttached(ctx: *anyopaque) bool {
    const l: *Link = @ptrCast(@alignCast(ctx));
    return l.state == .attached;
}
