# Security

## Reporting a vulnerability

**Do not report security vulnerabilities through public issues.**

Report them privately through GitHub:
[open a security advisory](https://github.com/cataggar/azure-uamqp-zig/security/advisories/new).

Please include what you have: the affected version or commit, the bytes or the
sequence that triggers it, and what an attacker gets out of it.

## Scope

This library parses input from a remote peer, so the decoder
(`src/zig/types/decoder.zig`) and the frame codec
(`src/zig/protocol/frame_codec.zig`) are the parts most worth attacking —
anything reachable from bytes on the wire is in scope.

This is a port of [Azure/azure-uamqp-c](https://github.com/Azure/azure-uamqp-c)
and is not maintained by Microsoft. A vulnerability in the upstream C library
should go to the
[Microsoft Security Response Center](https://msrc.microsoft.com/create-report).
