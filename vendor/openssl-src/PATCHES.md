# Vendored OpenSSL 3.5 LTS

The build wrapper comes from the crates.io `openssl-src 300.5.5+3.5.5`
release. mydb uses its existing platform configuration and compiler selection
with the official OpenSSL 3.5.9 sources. The wrapper extracts the local archive
into Cargo's build directory using the host's `tar` command. This keeps the
vendored build independent of the network and system OpenSSL installation.

`UPSTREAM.json` records the wrapper and source checksums. To refresh the
archive, verify the official release tarball against its published checksum.
Copy its tar members in order, dropping the top-level release directory
prefix and omitting the contents of `test/`, `doc/`, and `demos/` except
their `build.info` files. Keep the remaining files and tar metadata from the
release. The archive uses gzip level 9 with a zero
timestamp and no original filename. Update the version pin, checksums, and
license files together. The OpenSSL source files in the archive are unmodified.
