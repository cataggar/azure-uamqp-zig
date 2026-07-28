# Changelog

## Unreleased

A hardening and performance pass. No API breaks.

### Fixed

- **`Connection.open` failed against any peer that pipelines its AMQP header
  with the SASL outcome** (#27). The header was answered from inside
  `onBytesReceived`, so by the time the application called `open` the state
  had already advanced and it returned `error.InvalidState`. Apache ActiveMQ
  Artemis pipelines; Apache Qpid Proton does not, which is why this survived
  until a second broker was added to CI. `open` now means "I want this
  connection open" and is a no-op once it is being opened.
- **Unbounded memory on the receive path** (#25). A peer could send transfer
  fragments forever; `max-message-size` is now enforced while reassembling a
  delivery, and a link that exceeds it is detached.
- **A compound's declared `size` was parsed and discarded** (#28). A list, map
  or array is now sliced to the size it declared and its elements must fill it
  exactly.
- Three defects found by running every allocating path against an allocator
  that fails (#26), including an `ArenaAllocator.reset` whose `false` return
  was discarded, silently swallowing an out-of-memory.
- A `max-message-size` of zero is treated as unset on both sides per §3.5.3,
  rather than as a limit that refuses every message (#25).

### Known limitations

- The decoder rejects a list or array of zero-width elements — five nulls as
  `e0 02 05 40` — because the element count is bounded by the remaining byte
  count, and that bound is currently the only thing preventing a small frame
  from driving a large allocation. Nothing this library encodes hits it.

### Performance

Measured with `zig build bench`, ReleaseFast, allocations counted:

- **Array decode is no longer quadratic** (#29). It rebuilt
  `constructor ++ remaining-bytes` for every element. 4096 elements: 561 to
  89 microseconds, 4034 allocations to 2.
- **A frame is built once instead of three times** (#30). 1 KiB transfer:
  477 to 415 ns on send, 7 allocations to 1; 461 to 400 ns on receive, 4 to 1.

### Changed

- Connection, session, link, CBS and management state changes log at `debug`
  rather than `info` (#31). Nothing above `debug` is written on a healthy
  connection.

### Added

- `zig build interop`, a client that runs the whole stack against a real
  broker over a real socket (#27). CI runs it against Apache Qpid Proton and
  Apache ActiveMQ Artemis, and it is a required check.
- `zig build bench` (#29) and `zig build docs` (#32).
- Hostile-input tests over every entry point reachable from the wire (#37):
  ~500k seeded inputs through the decoder, the performative decoder, the
  message decoder and the frame codec, the last fed in random-sized chunks.
  One is a property rather than a smoke test: anything the decoder accepts
  re-encodes and decodes back equal.
- `examples/sender.zig` and `examples/receiver.zig` are complete programs
  that open a socket and move a message (#39). They previously claimed to
  open a connection and did not. CI now runs them against a broker.

### Packaging

- `LICENSE`, `readme.md` and `CHANGELOG.md` are included in the published
  package (#38). `.paths` listed only the sources, so a fetched copy -- which
  is a redistribution of MIT code, most of it Microsoft's -- arrived without
  its notice.
- `uamqp.version` is read from `build.zig.zon` instead of being a second copy
  of the version that nothing checked (#38).
- Module documentation used `///!`, which is not module documentation: Zig
  attached it to the next declaration, usually a private `@import` (#36).
  `zig build docs` would have published a reference with no module
  documentation in it.

## 0.2.0

The release that makes the library speak AMQP rather than only encode it, and
the first one a consumer should pin.

### Breaking

- `encoder.encodedSize` returns `EncodeError!usize`. It used to swallow the
  error and report 0 bytes for a value that could not be encoded (#2).
- `DecodeError` gains `NestingTooDeep` (#19).
- Array encoding changed on the wire: elements now carry the width field their
  shared constructor implies, so an encoded array is parseable at all. An
  empty array encodes as `e0 02 00 40`. Consumers working around this by
  substituting a list where an array is required can stop (#2).

### Fixed

- **Remote stack overflow in `decoder.decode`** (#19). The described-type
  constructor recursed one stack frame per byte, so ~200 KB of zeros — inside
  an ordinary max-frame-size — aborted the process. Nesting is now bounded at
  64.
- **Remote 4 GiB allocation from a malformed frame header** (#3). `doff` was
  never checked against `size`, so `bodySize()` underflowed.
- **Dangling endpoints** (#4). `Session` and `Connection` handed out interior
  pointers into an `ArrayList`; endpoints are found by handle now.
- A 255-byte list, map or array panicked on an `@intCast` (#2).
- Idle timeouts and keep-alives could never fire, and the two were inverted.

### Added

- The wire protocol (#5): `Open`/`Close`, `Begin`/`End`, `Attach`/`Detach`,
  `Transfer` with credit, settlement, fragmentation and disposition, and SASL
  negotiation. Performatives are encoded by reflection over their structs.
- `MessageSender` and `MessageReceiver`: queue-until-credit, per-send
  timeouts, credit replenishment, settlement.
- `Message.encode`/`decode` for the §3.2 sections, and real `$cbs` and
  management operations on top of them.
- `Clock`, injected, so idle timeouts are testable without sleeping.
- `to_string`: an `AmqpValue` renders through `{f}`, keeping arrays, lists,
  symbols and strings distinguishable.
- CI on Linux, Windows and macOS, plus the suite under ReleaseSafe.
- Fuzz targets for `decoder.decode` and `FrameCodec.receiveBytes`.

### Removed

- The C tree. Every module in it was ported, or dropped as server-side or
  superseded; it remains in git history.

## 0.1.0

The initial Zig port: the AMQP type system, encoder and decoder, frame codec,
and the skeletons of the connection, session and link state machines.
