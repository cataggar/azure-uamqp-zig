//! Receive one message from a broker.
//!
//!     zig build && ./zig-out/bin/receiver
//!
//! Talks to 127.0.0.1:5672 anonymously by default; see `amqp_client.zig` for
//! the environment variables that change that. Run `sender` against the same
//! broker and address to give this something to read.

const std = @import("std");
const uamqp = @import("uamqp");
const client = @import("amqp_client.zig");

/// What the handler saw. The message it is given does not outlive the call,
/// so anything worth keeping has to be copied out of it now.
const Inbox = struct {
    body: [512]u8 = undefined,
    len: usize = 0,
    count: usize = 0,

    fn onMessage(context: ?*anyopaque, msg: *const client.Message) ?uamqp.definitions.DeliveryState {
        const self: *Inbox = @ptrCast(@alignCast(context.?));
        self.count += 1;

        if (msg.body_data_sections.items.len > 0) {
            const body = msg.body_data_sections.items[0].bytes;
            self.len = @min(body.len, self.body.len);
            @memcpy(self.body[0..self.len], body[0..self.len]);
        }

        const subject = if (msg.properties) |p| p.subject else null;
        std.debug.print("received '{s}' (subject {?s})\n", .{ self.body[0..self.len], subject });

        // Returning the delivery state is how the message is settled. Return
        // `.rejected` here and the broker is told this one could not be
        // handled; return null to leave it unsettled and decide later.
        return .accepted;
    }

    fn gotOne(ctx: *anyopaque) bool {
        const self: *Inbox = @ptrCast(@alignCast(ctx));
        return self.count > 0;
    }
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const cfg = try client.Config.fromEnv(init.environ_map);

    std.debug.print("uamqp {s}: receiving from {s}:{d}, address '{s}'\n", .{
        uamqp.version, cfg.host, cfg.port, cfg.address,
    });

    var setup: client.Setup = undefined;
    const c = try client.start(&setup, gpa, init.io, cfg, "azure-uamqp-zig-receiver");
    defer setup.deinit(init.io);

    try c.connect();

    var session = client.Session.init(gpa, &setup.conn, .{});
    defer session.deinit();
    try c.beginSession(&session);

    // A receiver's source is where the messages come from; its target names
    // this end. The reverse of the sender.
    var link = try client.Link.init(
        gpa,
        &session,
        "example-receiver",
        .receiver,
        .{ .address = cfg.address },
        .{ .address = "example-client" },
    );
    defer link.deinit();

    // Credit is the flow control: this says the broker may send ten messages
    // before waiting to be told it can send more.
    var inbox: Inbox = .{};
    var receiver = uamqp.message_receiver.MessageReceiver.init(gpa, &link, .{ .credit = 10 });
    defer receiver.deinit();
    try receiver.open(Inbox.onMessage, &inbox);
    try c.until("receiver attach", &link, client.Client.linkAttached);

    try c.until("a message", &inbox, Inbox.gotOne);

    receiver.close();
    try session.end(null);
    try c.close();
}
