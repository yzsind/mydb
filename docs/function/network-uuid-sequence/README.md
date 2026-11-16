# Network, UUID, and Sequence Functions

| Function | Description |
| --- | --- |
| [`BIN_TO_UUID`](bin_to_uuid.md) | Converts a 16-byte UUID to the standard text format. |
| [`CURRVAL`](currval.md) | Returns the most recent value obtained from the specified sequence in the current session. |
| [`INET6_ATON`](inet6_aton.md) | Converts an IPv6 or IPv4 text address to binary format. |
| [`INET6_NTOA`](inet6_ntoa.md) | Converts a binary IPv6 or IPv4 address to text. |
| [`INET_ATON`](inet_aton.md) | Converts an IPv4 text address to an integer. |
| [`INET_NTOA`](inet_ntoa.md) | Converts an integer IPv4 address to text. |
| [`IS_IPV4`](is_ipv4.md) | Determines whether a string is a valid IPv4 address. |
| [`IS_IPV4_COMPAT`](is_ipv4_compat.md) | Determines whether a binary IPv6 address is an IPv4-compatible address. |
| [`IS_IPV4_MAPPED`](is_ipv4_mapped.md) | Determines whether a binary IPv6 address is an IPv4-mapped address. |
| [`IS_IPV6`](is_ipv6.md) | Determines whether a string is a valid IPv6 address. |
| [`IS_UUID`](is_uuid.md) | Determines whether a string is a valid UUID. |
| [`LASTVAL`](lastval.md) | Returns the most recent value obtained from any sequence in the current session. |
| [`NEXTVAL`](nextval.md) | Obtains and advances the next value of the specified sequence. |
| [`SETVAL`](setval.md) | Sets the current value of the specified sequence. |
| [`UUID`](uuid.md) | Generates a UUID in the standard text format. |
| [`UUID_SHORT`](uuid_short.md) | Generates a 64-bit counter value increasing across sessions of the current process; cross-instance/restart uniqueness is not guaranteed. |
| [`UUID_TO_BIN`](uuid_to_bin.md) | Converts a standard text UUID to 16-byte binary format. |


## Address and UUID formats

INET_ATON/INET_NTOA convert IPv4 numbers; INET6_ATON/INET6_NTOA use 4/16-byte
network-order payloads. IS_IPV4 is stricter than legacy INET_ATON abbreviated IPv4
conversion. IPv6 literal parsing does not accept appended ports or interface scopes.
Use HEX to inspect binary output. IPv4-compatible and IPv4-mapped tests expect
16 binary bytes, not the printable IPv6 address.

UUID accepts the generated version-1 layout. UUID text parsing also accepts its
supported compact/braced representation; IS_UUID checks syntax, not provenance.
UUID_TO_BIN and BIN_TO_UUID must use the same swap_flag. Swapping is a time-field
layout transformation, not a change to identity. UUID_SHORT has a separate
process-local counter contract and does not guarantee cross-instance/restart
uniqueness; see its page.


## Sequence state and transactions

NEXTVAL and SETVAL are nontransactional: statement/transaction rollback does not
undo counter allocation/reset. Gaps are expected from failed work, cached values,
connection closure and crash recovery. Sequence names can be database-qualified;
these are mydb extensions, not MySQL AUTO_INCREMENT aliases.

NEXTVAL records CURRVAL and the LASTVAL target in the calling session. SETVAL(...,
true) updates that sequence's CURRVAL but does not select a new LASTVAL target;
if it modifies the existing target, LASTVAL reads the updated session value.
SETVAL(...,false) arranges for the next allocation to start at the supplied value.
Other sessions may retain already reserved caches. An undefined CURRVAL/LASTVAL
raises an error rather than returning another session's last allocation.

Object permissions are checked for sequence reads/allocation/reset. See
[Sequences](../../sequences.md) for the exact privilege matrix, CACHE/CYCLE,
min/max bounds, durability and primary/replica reservation policies.
