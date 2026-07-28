# Changelog

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
