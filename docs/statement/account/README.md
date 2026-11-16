# Account, Role, and Privilege Statements

| Group | Statements |
| --- | --- |
| User | [CREATE](create-user.md) · [ALTER](alter-user.md) · [DROP](drop-user.md) · [RENAME](rename-user.md) · [SET PASSWORD](set-password.md) |
| Role | [CREATE](create-role.md) · [DROP](drop-role.md) · [SET ROLE](set-role.md) · [SET DEFAULT ROLE](set-default-role.md) |
| Granting | [GRANT](grant.md) · [REVOKE](revoke.md) |
| Viewing | [SHOW GRANTS](show-grants.md) · [SHOW CREATE USER](show-create-user.md) · [SHOW PRIVILEGES](show-privileges.md) |

Account identifiers use `'user'@'host'`; omitted host defaults to `%`. USER() identifies the login and peer, while CURRENT_USER() identifies the account matched for authorization. Specify hosts explicitly when writing grants or comparing account identities. Passwords, authentication plugins, TLS requirements and account quotas are independent of unsupported RESOURCE GROUP scheduling.

| REST credentials | Statements |
| --- | --- |
| API key lifecycle | [CREATE](create-api-key.md), [SHOW one credential](show-api-key.md), [SHOW metadata](show-api-keys.md), [DROP](drop-api-key.md), [ROTATE MASTER KEY](rotate-api-key-master-key.md) |

See [authorization scopes](grant.md#behavior-and-limitations), [account options](create-user.md#syntax), [entry privileges](../execution-contracts.md#privilege-scope), and [REST identity rules](../../rest-api.md). Host patterns `%` and `_` are distinct from an omitted host; account-management statements address the exact stored user/host pair.

Host matching is case-insensitive for host patterns. `%` matches a run and `_` a single character. `localhost` also matches loopback peers, including IPv4/IPv6 loopback, because mydb's listener is TCP. An explicit literal IP/host is more specific than a wildcard pattern. Match the authenticated CURRENT_USER() account before changing self credentials; a login user string alone is not a unique account identity.
