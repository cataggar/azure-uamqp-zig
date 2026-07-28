# Contributing

## Pull requests

Every change lands through a pull request against `main`, which is protected:
no direct pushes, and CI must be green. Keep a pull request to one concern —
one fix, one module — so it can be reviewed and reverted on its own.

## What a change should come with

- **A test that fails without it.** For a bug, write the test that reproduces
  it first; for a feature, cover the behavior a caller would rely on.
- **`zig fmt`**, or CI will reject it: `zig fmt src examples build.zig`.
- A commit message that says what changed and *why* it was wrong before.

## Running things

```sh
zig build                                        # library + examples
zig build test --summary all                     # the whole suite
zig build test -Doptimize=ReleaseSafe            # again, with release semantics
zig test src/zig/uamqp.zig --test-filter "map"   # one test by name substring
zig fmt --check src examples build.zig
```

A test only runs if its file is reachable from `src/zig/uamqp.zig`, so a new
`.zig` file must be re-exported there or its tests silently never execute.

## Conventions

- Take an `std.mem.Allocator`; never reach for a global one.
- Nothing handed to a callback outlives the call — copy what you keep.
- No I/O, and no clock, inside the library: both are injected.

## Reporting bugs

Open an issue with the bytes or the sequence that triggers it where you can.
For anything with security impact, see [SECURITY.MD](SECURITY.MD) instead.
