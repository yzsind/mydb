# Account, Role, and Privilege Statements

| Group | Statements |
| --- | --- |
| User | [CREATE](create-user.md) · [ALTER](alter-user.md) · [DROP](drop-user.md) · [RENAME](rename-user.md) · [SET PASSWORD](set-password.md) |
| Role | [CREATE](create-role.md) · [DROP](drop-role.md) · [SET ROLE](set-role.md) · [SET DEFAULT ROLE](set-default-role.md) |
| Granting | [GRANT](grant.md) · [REVOKE](revoke.md) |
| Viewing | [SHOW GRANTS](show-grants.md) · [SHOW CREATE USER](show-create-user.md) · [SHOW PRIVILEGES](show-privileges.md) |

Account identifiers uniformly take the form `'user'@'host'`; the exact default applied when host is omitted and the host matching rules follow the current authentication implementation.
