# Security Deployment

The public preview supports controlled evaluation. Keep the default quick-start
commands unchanged: the MySQL listener binds all IPv4 interfaces on port 8738.
Account matching and listener binding are separate controls. A fresh root account
accepts loopback connections; Docker bridge connections need a separate account.

## Initialize and connect

Set MYDB_ROOT_PASSWORD through a protected environment on the first start of a
fresh data directory. It does not change existing passwords. Use ALTER USER for
an initialized account. Configure a persistent certificate/key and
`require_secure_transport=ON`; connect with VERIFY_IDENTITY and the issuing CA.
See [TLS setup](configuration.md#tls). The default ephemeral certificate provides
local TLS negotiation but no stable verified server identity.

## Give applications scoped privileges

Run as an administrator, replacing the sample password and host pattern:

```sql
CREATE USER 'app_reader'@'%' IDENTIFIED BY 'replace-with-a-unique-password';
GRANT SELECT ON test.* TO 'app_reader'@'%';
SHOW GRANTS FOR 'app_reader'@'%';
```

Test with a new app_reader connection: SELECT on test succeeds, INSERT and reads
of unrelated schemas fail. Give writers only the required INSERT/UPDATE/DELETE
privileges; keep schema changes and administrative capabilities on separate
accounts. Review activated roles as well as direct grants. Revoke privileges and
test subsequent prepared executions when changing access.

For tenant isolation, use [RLS](row-level-security.md), ordinary object grants and
a trusted connection context. SUPER and BYPASS_RLS bypass row checks. Validate RLS
using an application account without those privileges, and reset tenant context
when returning pooled connections. RLS administration and context privileges are
separate from SUPER.

## Credentials and outbound access

[API keys](rest-api.md#create-list-reveal-and-revoke-api-keys) act as their owning
SQL account. Key administration is top-level TLS MySQL SQL; SHOW API KEY can reveal
a full credential. Keep admin access separate and revoke unused keys. REST/MCP,
AI credentials and TLS keys have distinct storage and rotation procedures:

| Material | Procedure |
| --- | --- |
| REST master key and previous keys | [Encryption and recovery](rest-api.md#credential-encryption-and-recovery) |
| AI secrets and encryption key | [AI credentials](ai.md#encrypted-api-keys) |
| TLS certificate/private key and CA | [TLS configuration](configuration.md#tls) |
| Backup encryption key | [Encrypted backups](backup-and-recovery.md#92-configure-encrypted-archiving) |

Back up required keys separately in protected storage before rotation or recovery.
A data backup alone does not recreate external key files or node-local policy.
Rehearse reopening encrypted credentials after restart and recovery before
retiring old keys. Never paste secret material into issue reports or SQL logs.

[HTTP](http.md) and [AI](ai.md) require their own privileges and outbound policy;
SELECT permission alone does not authorize arbitrary remote access. Allow only
required destinations, bound time and response sizes, and inspect remote outcomes
before retrying external side effects. SQL rollback does not undo those effects.
