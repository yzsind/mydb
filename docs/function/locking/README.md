# Locking Functions

| Function | Description |
| --- | --- |
| [`GET_LOCK`](get_lock.md) | Acquires a user-level named lock within the given timeout. |
| [`IS_FREE_LOCK`](is_free_lock.md) | Determines whether a named lock is currently free. |
| [`IS_USED_LOCK`](is_used_lock.md) | Returns the connection ID holding a named lock. |
| [`RELEASE_ALL_LOCKS`](release_all_locks.md) | Releases all named locks held by the current connection. |
| [`RELEASE_LOCK`](release_lock.md) | Releases the named lock held by the current connection. |


## Scope waiting and reentry

Names are case-insensitive in this implementation. GET_LOCK requires a nonempty
name of at most 64 characters; SQL NULL names are errors in all named-lock calls.
Locks belong to connections and are local to one server process. COMMIT/ROLLBACK
does not release them. Connection exit releases them, but they are not a distributed
mutex across independent instances or a durable lock after a restart.

Acquiring the same name again in one connection increments a reentry count.
RELEASE_LOCK releases one acquisition; RELEASE_ALL_LOCKS counts all acquisitions
released. Another connection cannot acquire until the count reaches zero.
IS_FREE_LOCK/IS_USED_LOCK are observations and can be stale immediately afterward.

For a two-connection test, acquire GET_LOCK('example',0) in A, then call the same
name in B with timeout 0 to observe 0. Release in A, retry in B, and release in B.
Use distinct test names and clean up every successful acquisition. Infinite or
network-length waits inside transactions extend database lock lifetimes; statement
cancellation still applies.
