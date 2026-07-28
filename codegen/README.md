# AMQP 1.0 definitions

`amqp_definitions.xml` is the machine-readable AMQP 1.0 type and performative definition
that upstream `Azure/azure-uamqp-c` used to generate `amqp_definitions.c`/`.h`. The C#
generator that consumed it is gone; the XML is kept because it is still the authoritative
list of the 59 defined types, their descriptors, field names, types, and mandatory flags.

It is the reference for `src/zig/protocol/definitions.zig`, and the input a Zig generator
would read if that file is ever generated rather than hand-written.

`amqp_definitions.xsd` and `amqp_definitions.dtd` describe the XML's own schema.
