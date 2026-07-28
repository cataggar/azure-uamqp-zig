# azure-uamqp-zig

An AMQP 1.0 client library, ported from `Azure/azure-uamqp-c` v1.2.12 to idiomatic Zig
(`zig 0.16.0`). Everything here is Zig: the C tree it was ported from has been deleted.

## Layout

- `build.zig`, `build.zig.zon`, `src/zig/**`, `examples/*.zig` — all of the code.
- `codegen/amqp_definitions.xml` (with its `.xsd`/`.dtd`) is the machine-readable AMQP 1.0
  definition that generated the C `amqp_definitions.c`. It is kept as the source of truth
  for `src/zig/protocol/definitions.zig`.
- The C sources, headers, unit tests, `devdoc/*_requirements.md` specs and samples are in
  git history up to the commit that removed them; `git log -- <path>` still finds them
  when a ported module's original behavior needs checking.

## Build, test, format

```sh
zig build                 # static lib "uamqp" + sender/receiver example binaries
zig build test            # whole suite
zig build test --summary all           # show per-step test counts
zig test src/zig/types/decoder.zig     # one module (+ everything it imports)
zig test src/zig/uamqp.zig --test-filter "map"   # one test / subset, by name substring
zig fmt src examples build.zig
```

`build.zig` exposes no test-filter option, so filter with `zig test ... --test-filter`
directly. Filters match the fully qualified name, e.g. `types.decoder.test.decode string`.

Tests only run if the file is reachable from `src/zig/uamqp.zig` (its `test { refAllDecls }`
block pulls in the module graph). **A new `.zig` file must be re-exported from
`src/zig/uamqp.zig` or its tests silently never execute.** `build.zig.zon` also only
packages `src/zig`, so anything outside it is invisible to downstream consumers.

## Architecture

Strict bottom-up layering, all re-exported from the single entry point `src/zig/uamqp.zig`
(consumers write `@import("uamqp")`):

1. `types/` — `AmqpValue` is a tagged union covering all 23 AMQP primitive/composite types
   and is the currency of the whole library. `encoder.zig` writes into a `Buffer` (fixed or
   dynamic); `decoder.zig` allocates and returns `DecodeResult{ value, bytes_consumed }`.
2. `protocol/` — `frame.zig` (header parse/serialize), `frame_codec.zig` (byte-stream state
   machine: `reading_header` → `reading_body`, with a subscription list keyed by
   `FrameType`), `definitions.zig` (every performative as a plain struct plus its descriptor
   code), then the three state machines `connection.zig` → `session.zig` → `link.zig`.
3. `message.zig` / `messaging.zig` — message sections and source/target helpers.
4. `sasl/` and the high-level `cbs.zig` / `management.zig`.

Dispatch chain: `FrameCodec` emits complete frames → `Connection` routes performatives to
registered `Endpoint`s by channel → `Session` routes to `LinkEndpoint`s by handle → `Link`
applies credit/delivery logic.

**There is no networking or TLS.** I/O is injected as
`io_send: *const fn (context: ?*anyopaque, data: []const u8) anyerror!void` plus an opaque
context, set via `setIo`. The clock is still a placeholder (`TODO` in `connection.zig`), so
idle-timeout logic is not wired to a real time source.

## Conventions

- **C constructs were mapped deliberately** — keep using the replacements rather than
  reintroducing C-isms: `gballoc` → explicit `std.mem.Allocator`; `xlogging` →
  `std.log.scoped`; `singlylinkedlist` → `std.ArrayList`; `MU_DEFINE_ENUM` → native enums;
  `MU_FAILURE` → error unions; `refcount` → explicit `clone`/`deinit`.
- **File headers** start with a `///!` doc block naming the OASIS AMQP 1.0 spec section
  implemented (and the C file it replaces). Inline comments cite sections as `§2.6.4`.
  Sections within a file are separated by `// ── Name ──────` banners.
- **Two memory-ownership styles.** Value types take the allocator per call
  (`AmqpValue.clone(allocator)`, `deinit(allocator)`); long-lived structs store `allocator`
  and expose `init(allocator, ...)` / `deinit(self)`.
- **Unmanaged containers (Zig 0.15+ style):** `std.ArrayList` fields are initialized with
  `.empty` and the allocator is passed to `append`/`deinit`.
- **Optional settings** are an anonymous struct parameter with defaults, e.g.
  `Connection.init(allocator, container_id, hostname, .{ .channel_max = 8 })`.
- **Callbacks follow the C event model:** a function pointer plus a `?*anyopaque` context
  field (`on_state_changed` / `on_state_changed_context`). Multi-method interfaces use an
  explicit `ptr` + `vtable` struct instead (`sasl.Mechanism`).
- **Explicit error sets** for the public codec surface (`DecodeError`); wire problems are
  `error.UnexpectedEnd` / `error.InvalidData`.
- **Logging:** one `const log = std.log.scoped(.amqp_connection);` per module, scopes named
  `amqp_*` / `sasl_*`.
- **Tests live at the bottom of the file they cover**, named as prose sentences
  (`test "a truncated map does not deinit uninitialized entries"`), and use
  `std.testing.allocator` so leaks fail the test.

## Decoder safety invariants

Everything `types/decoder.zig` parses is attacker-controlled (it comes straight off the
wire). These invariants were added in commit `5933106` and must be preserved:

- Never size an allocation from a wire-supplied element count without cross-checking it
  against the remaining bytes (≥1 byte per list/array element, ≥2 per map pair) — otherwise
  a 9-byte frame can request 64 GiB.
- Track how many elements have actually been written; `errdefer` cleanup must free only that
  initialized prefix, never walk the uninitialized tail.
- Free partially decoded sub-values on failure (e.g. the map key when its value fails).

Add a truncation/oversize test alongside any new composite-type decoding path.
