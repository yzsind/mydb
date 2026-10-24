# Security Policy

## Scope

This policy covers the MyDB server: the MySQL protocol implementation, authentication, the SQL engine, storage, WAL, and replication. It applies to the latest published release of this repository.

MyDB is still working toward its first product release. It has not yet undergone production hardening: treat deployments as evaluation-grade, avoid exposing instances to untrusted networks, and keep the default loopback-only root account behavior unless you intentionally change it.

## Reporting a vulnerability

Please do **not** open public issues for security vulnerabilities.

Report privately via either channel:

- **Email**: yzsind@163.com
- **GitHub**: use the repository's private vulnerability reporting ("Report a vulnerability") once it is enabled on the public repository.

Include when possible:

- A minimal reproduction (SQL sequence, protocol trace, or script).
- Affected version or commit hash.
- Impact assessment and any workaround you identified.

You should receive an acknowledgment within a few days. Please avoid public disclosure until a fix or mitigation is published. We will credit reporters in release notes unless you prefer to remain anonymous.

## Safe usage notes

- Set `MYDB_ROOT_PASSWORD` at first start; the default `root`/`root` account is loopback-restricted but intended for local development only.
- TLS is advertised by default with an ephemeral self-signed certificate; configure a proper certificate for production-bound use (see `docs/configuration.md`).
- Statement and error behavior for unsupported features is designed to fail explicitly; if you find a silent no-op, that is a bug worth reporting.
