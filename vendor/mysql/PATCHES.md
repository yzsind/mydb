# Local TLS parser maintenance

This directory contains the verified crates.io mysql 28.0.4 release. The
upstream archive checksum and repository are recorded in `UPSTREAM.json`.
Both upstream licenses are retained. `Cargo.toml.orig` is the unmodified
upstream manifest for provenance; Cargo uses the patched `Cargo.toml`.

The local patch removes the unmaintained rustls-pemfile dependency and calls
the maintained `rustls::pki_types::pem::PemObject` parsers directly. Certificate
chains, PEM/DER fallback, and PKCS#1/PKCS#8/SEC1 key selection retain the
upstream behavior. Parser failures remain errors. CA and hostname verification,
dangerous opt-in flags, cipher suites, protocol negotiation, and client
authentication are unchanged.

The root build fingerprint includes this directory. Docker builds copy it,
and release inventories include its notices and upstream provenance rather
than treating it as first-party code. Upstream updates must reapply and review
this patch and refresh the recorded source identity; do not silently replace
the patched source with an unverified registry or Git snapshot.
