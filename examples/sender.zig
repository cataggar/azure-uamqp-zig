//! Send one message to a broker.
//!
//!     zig build && ./zig-out/bin/sender
//!
//! Talks to 127.0.0.1:5672 anonymously by default; see `amqp_client.zig` for
//! the environment variables that change that. `interop/broker.py` is a
//! broker to try it against.

const std = @import("std");
const uamqp = @import("uamqp");
const client = @import("amqp_client.zig");

/// Filled in by the callback the send completes through. The library never
/// blocks, so the result arrives later, when the broker has settled it.
const Outcome = struct {
    done: bool = false,
    result: ?uamqp.message_sender.MessageSendResult = null,

    fn onComplete(
        context: ?*anyopaque,
        result: uamqp.message_sender.MessageSendResult,
        _: ?uamqp.definitions.DeliveryState,
    ) void {
        const self: *Outcome = @ptrCast(@alignCast(context.?));
        self.done = true;
        self.result = result;
    }

    fn ready(ctx: *anyopaque) bool {
        const self: *Outcome = @ptrCast(@alignCast(ctx));
        return self.done;
    }
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const cfg = try client.Config.fromEnv(init.environ_map);

    std.debug.print("uamqp {s}: sending to {s}:{d}, address '{s}'\n", .{
        uamqp.version, cfg.host, cfg.port, cfg.address,
    });

    // `Setup` is the storage the client borrows, so it lives here and must
    // not move for as long as the client is in use.
    var setup: client.Setup = undefined;
    const c = try client.start(&setup, gpa, init.io, cfg, "azure-uamqp-zig-sender");
    defer setup.deinit(init.io);

    try c.connect();

    var session = client.Session.init(gpa, &setup.conn, .{});
    defer session.deinit();
    try c.beginSession(&session);

    // A sender's target is where the messages go; its source names this end.
    var link = try client.Link.init(
        gpa,
        &session,
        "example-sender",
        .sender,
        .{ .address = "example-client" },
        .{ .address = cfg.address },
    );
    defer link.deinit();

    var sender = uamqp.message_sender.MessageSender.init(gpa, &link);
    defer sender.deinit();
    try sender.open();
    try c.until("sender attach", &link, client.Client.linkAttached);

    var msg = client.Message.init(gpa);
    defer msg.deinit();
    msg.header = .{ .durable = true };
    msg.properties = .{ .subject = "example", .content_type = "text/plain" };
    try msg.setApplicationProperty("sent-by", "azure-uamqp-zig");
    try msg.addBodyData("hello from Zig");

    // Nothing is on the wire yet: if the broker has not granted credit the
    // send is queued until it does.
    var outcome: Outcome = .{};
    _ = try sender.send(&msg, .{ .on_complete = Outcome.onComplete, .context = &outcome });
    try c.until("the broker to settle the message", &outcome, Outcome.ready);

    if (outcome.result != .ok) {
        std.debug.print("the broker did not accept it: {?}\n", .{outcome.result});
        return error.MessageRejected;
    }
    std.debug.print("sent and accepted\n", .{});

    try link.detach(true, null);
    try session.end(null);
    try c.close();
}
