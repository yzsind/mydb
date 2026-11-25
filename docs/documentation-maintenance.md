# Documentation Scope and Versions

Product guides in this checkout describe its current source behavior. Use the
documentation from the matching Git tag when operating a downloaded release.
[Release notes](README.md#getting-started-and-deployment) and dated benchmark
reports keep their identified versions, settings and evaluation boundaries.

The current package version is `1.0.2`, but development after the release has
changed physical formats and added features. Read [current source changes](current-source.md)
and [changing versions](changing-versions.md) before reusing a data directory,
restoring a physical backup, or connecting native replicas. A product version
string alone does not establish physical compatibility.

The [feature overview](feature-overview.md) states preview maturity, and the
[MySQL compatibility reference](mysql80-compatibility.md) describes supported
behavior and intentional differences. A listed MySQL-shaped metadata object
does not imply that its corresponding subsystem is implemented. Unsupported
SQL and protocols return explicit errors within the documented boundaries.

Machine-readable [startup options](configuration-registry.md),
[storage formats](storage-format-versions.md), [extension errors](error-codes.md),
[metadata](mysql80-metadata-compat-matrix.json),
[protocol](mysql80-protocol-compat-matrix.json) and [REST contracts](rest-openapi.json)
accompany the usage guides. When prose and behavior disagree, source code and
tests are authoritative; include the exact build and a reproducible example
when [reporting a documentation issue](reporting-issues.md).
