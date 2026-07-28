//! SASL MSSBCBS mechanism.
//!
//! The mechanism Azure Service Bus offers for claims-based security: the
//! peer names it in its mechanisms list, the client selects it with an empty
//! initial response, and authorization then happens over the `$cbs` node
//! (see `cbs.zig`) rather than in the SASL exchange itself.
const std = @import("std");
const Mechanism = @import("mechanism.zig").Mechanism;

pub const MsSbCbs = struct {
    pub fn mechanism(self: *MsSbCbs) Mechanism {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Mechanism.VTable{
        .get_mechanism_name = getMechanismName,
        .get_init_bytes = getInitBytes,
        .on_challenge = onChallenge,
    };

    fn getMechanismName(_: *anyopaque) []const u8 {
        return "MSSBCBS";
    }

    fn getInitBytes(_: *anyopaque) ?[]const u8 {
        return null;
    }

    fn onChallenge(_: *anyopaque, _: []const u8) ?[]const u8 {
        return null;
    }
};

test "MSSBCBS mechanism carries no credential of its own" {
    var mssbcbs = MsSbCbs{};
    const mech = mssbcbs.mechanism();
    try std.testing.expectEqualStrings("MSSBCBS", mech.getMechanismName());
    // The token goes to $cbs, not into the SASL exchange.
    try std.testing.expect(mech.getInitBytes() == null);
    try std.testing.expect(mech.onChallenge("anything") == null);
}
