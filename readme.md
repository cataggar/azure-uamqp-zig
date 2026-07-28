# uamqp

An AMQP 1.0 client library for Zig, ported from
[Azure/azure-uamqp-c](https://github.com/Azure/azure-uamqp-c) v1.2.12.

Everything is Zig — the C sources this began as have been removed. It targets
Zig 0.16.0.

## Design

The library is **sans-I/O**: it owns no socket, no TLS and no event loop. You
give it a function that writes bytes, hand it the bytes that arrive, and it
tells you what to do next through callbacks. That keeps it usable with any
transport — plain TCP, TLS, WebSockets, a test double — and makes every layer
testable without a network.

For the same reason it does not read a clock: idle timeouts and keep-alives
run off a `Clock` you supply (`Connection.setClock`), so tests wind time by
hand instead of sleeping.

Memory is explicit: every type that allocates takes an `std.mem.Allocator`,
and callback arguments never outlive the call — copy what you keep.

## Using it

```sh
zig fetch --save git+https://github.com/cataggar/azure-uamqp-zig
```

```zig
// build.zig
const uamqp = b.dependency("uamqp", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("uamqp", uamqp.module("uamqp"));
```

```zig
const uamqp = @import("uamqp");

var connection = uamqp.connection.Connection.init(allocator, "my-container", null, .{});
defer connection.deinit();
connection.setIo(writeToYourSocket, socket_context);
try connection.open();

var session = uamqp.session.Session.init(allocator, &connection, .{});
defer session.deinit();
try session.begin();

var link = try uamqp.link.Link.init(
    allocator,
    &session,
    "my-sender",
    .sender,
    uamqp.messaging.createSource("my-queue"),
    uamqp.messaging.createTarget("my-queue"),
);
defer link.deinit();

var sender = uamqp.message_sender.MessageSender.init(allocator, &link);
defer sender.deinit();
try sender.open(); // attaches the link; open once the peer answers

// Whenever the socket has bytes, which is what drives every state machine:
try connection.onBytesReceived(bytes);

// Once the sender is open — `setOnStateChanged` says when:
var message = uamqp.message.Message.init(allocator);
defer message.deinit();
try message.addBodyData("hello");
_ = try sender.send(&message, .{});
```

`examples/sender.zig` and `examples/receiver.zig` are built by `zig build`.

## What is in it

| Module | |
| --- | --- |
| `types`, `encoder`, `decoder`, `to_string` | the AMQP type system, on and off the wire |
| `frame`, `frame_codec` | frame headers and the streaming frame parser |
| `definitions`, `described` | the performatives, encoded by reflection over their structs |
| `connection`, `session`, `link` | the state machines of AMQP 1.0 §2 |
| `message`, `messaging` | the message sections of §3 |
| `message_sender`, `message_receiver` | send and receive, with credit and settlement handled |
| `sasl.*` | SASL negotiation: PLAIN, ANONYMOUS, MSSBCBS |
| `cbs`, `management` | the `$cbs` node and the AMQP management protocol |

## Building

```sh
zig build                    # static library + examples
zig build test --summary all # the test suite
zig fmt src examples build.zig
```

CI runs the same steps on Linux, Windows and macOS, plus the suite under
`-Doptimize=ReleaseSafe`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Every change goes through a pull
request, and CI must be green before it merges.

## License

MIT — see [LICENSE](LICENSE). The original C library is
copyright Microsoft Corporation and licensed under the same terms; this port
keeps that notice.
