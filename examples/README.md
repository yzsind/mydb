# Client Examples

The Java, Python, Go, C++, and Rust examples are runnable programs with result
checks. They cover connections, SELECT, DDL, parameterized DML, batch inserts,
generated keys, transaction commit/rollback, savepoints, visibility across two
connections, duplicate-key errors, and stored procedures with IN/OUT/INOUT
parameters and multiple result sets. Each example uses an isolated database,
cleans up on success or ordinary exceptions, and exits with a nonzero status on
failure.

| Language | Source | Client dependencies | Run command |
| --- | --- | --- | --- |
| Java | [java/JdbcDemo.java](java/JdbcDemo.java) | JDK 8+, Connector/J jar | `bash examples/run_java_demo.sh` |
| Python | [python/demo.py](python/demo.py) | Python 3.9+, PyMySQL 1.1.2 with RSA authentication dependencies | `bash examples/run_client_demo.sh python` |
| Go | [go/main.go](go/main.go) | Go 1.24+, go-sql-driver/mysql 1.10.0 | `bash examples/run_client_demo.sh go` |
| C++ | [cpp/demo.cpp](cpp/demo.cpp) | C++17 compiler, MySQL 8+ C client headers and libmysqlclient | `bash examples/run_client_demo.sh cpp` |
| Rust | [rust/src/main.rs](rust/src/main.rs) | Rust/Cargo, mysql crate 28.0.4 | `bash examples/run_client_demo.sh rust` |

## Running Python, Go, C++, and Rust

Start a local mydb instance from the repository root. The Java example can use
the same server:

```bash
cargo build
MYDB_DATA_DIR=/tmp/mydb/client-example-data \
  ./target/debug/mydb --host 127.0.0.1 --port 4837
```

In another terminal, install the Python dependencies and run all four languages:

```bash
python3 -m venv /tmp/mydb/client-example-venv
/tmp/mydb/client-example-venv/bin/python -m pip install -r examples/python/requirements.txt
export PYTHON=/tmp/mydb/client-example-venv/bin/python
export MYDB_PORT=4837
bash examples/run_client_demo.sh all
```

Use the commands in the table to run each language separately. Go and Rust
resolve dependencies using `go.sum` and `Cargo.lock`; their standard tools
download any uncached dependencies on the first run. For C++, the script obtains
compiler and linker flags from `pkg-config mysqlclient`, falling back to
`mysql_config` when unavailable. Set `CXX` to select a compiler, or set
`MYSQL_CONFIG=/absolute/path/mysql_config` to select a specific MySQL client
installation. Missing tools, missing dependencies, and failed checks cause the
script to report an error and exit.

All four languages read `MYDB_HOST` (default `127.0.0.1`), `MYDB_PORT` (default
`8738`), `MYDB_USER` (default `root`), `MYDB_PASSWORD` (default `root`; an empty
password is allowed), and `MYDB_DB` (default `test`). The initial database must
exist. The account needs privileges to create/drop databases, perform table and
index DDL, read/write data, and create/execute/drop stored procedures. Defaults
are intended for local TCP connections with TLS disabled. Java-specific
`MYDB_JDBC_*` variables do not affect the other languages.

The script places temporary build artifacts under `/tmp/mydb`. The C++ executable
is deleted on exit. Go and Rust caches are retained by default in
`/tmp/mydb/demo-go-cache`, `/tmp/mydb/demo-go-mod`, and
`/tmp/mydb/demo-rust-target`; standard tool environment variables can override
these locations. Each run prints its database name (`python_demo_*`, `go_demo_*`,
`cpp_demo_*`, or `rust_demo_*`) and prints `PASS all <language> examples` after all
checks and cleanup succeed. Forced termination or a lost connection may leave a
database behind; use the printed name to clean it up manually.

The examples demonstrate common client APIs:

- Python uses DB-API `%s` parameter binding, `Decimal`, `executemany`, and
  `callproc`. It consumes `nextset()` before reading PyMySQL's
  `@_account_summary_*` output variables.
- Go uses `PrepareContext`, `sql.Tx`, `sql.NullString`, `NextResultSet`, and
  timeouts. A dedicated `sql.Conn` keeps `USE`, procedure calls, and output
  variables on the same session.
- C++ uses `unique_ptr` to release connections, result sets, and prepared
  statements, and `MYSQL_BIND` to bind parameters. Result lengths preserve
  embedded binary NUL bytes and SQL NULL values; `mysql_next_result` consumes
  multiple result sets.
- Rust uses `Queryable`, `exec_batch`, `Transaction`, `Option`, and `Result`.
  Transactions roll back through Drop when an operation returns an error, and
  `QueryResult::iter()` iterates over multiple result sets.

Amounts use decimal values or exact strings to avoid floating-point conversion.
The Go and C++ batch examples reuse a prepared statement within a transaction.
The Go driver does not advertise `CLIENT_PS_MULTI_RESULTS`, so it binds the IN
parameter to a session variable before issuing a text-protocol `CALL`. C++ and
Rust demonstrate the same session-variable approach.

To include these four complete examples in the compatibility matrix, prepare all
dependencies first and enable them explicitly:

```bash
MYDB_PORT=4837 MYDB_CLIENT_DEMOS=1 bash scripts/run_client_compat_matrix.sh
```

## Java JDBC Examples

[java/JdbcDemo.java](java/JdbcDemo.java) is a complete runnable example with result
checks. [java/JdbcSelectOne.java](java/JdbcSelectOne.java) is a minimal connection
probe that executes only `SELECT 1`. When `MYDB_CONNECTOR_J_JAR` is set,
`scripts/run_client_compat_matrix.sh` compiles and runs both programs.

### Coverage

| Method | Demonstrations and checks |
| --- | --- |
| `main` / `demoConnection` | `DriverManager`, connection properties, timeouts, `isValid`, `SELECT 1`, and resource cleanup with try-with-resources |
| `demoDdl` | Create a table, add a column with a default, and create an index |
| `demoDml` | `PreparedStatement` parameter binding, INSERT/UPDATE/DELETE affected rows, generated keys, and `addBatch` / `executeBatch` |
| `demoSelect` | `Statement` / `PreparedStatement` queries, `ResultSet` and metadata, aggregates, BIGINT, DECIMAL, Chinese text and emoji, SQL NULL, binary data, and microsecond timestamps |
| `demoTransactions` | READ COMMITTED, disabling autocommit, two connections checking uncommitted and committed visibility, savepoint rollback/release, full rollback, duplicate-key errors, and rollback after an error |
| `demoProcedure` | Create a procedure, use `CallableStatement` with IN/OUT/INOUT parameters, iterate over multiple result sets and update counts, read output parameters, and drop the procedure/index/table |

Each run creates an isolated `jdbc_demo_<random UUID>` database on the server and
cleans it up through try-with-resources, including when execution fails. The
account needs privileges to create/drop databases, create/alter tables, manage
indexes, read/write data, and create/execute/drop stored procedures. The default
root account can be used locally. The database in the connection URL (default
`test`) must already exist. Tables and data in existing databases are not modified.
Forced termination or a lost connection may leave the temporary database behind;
use the printed database name to clean it up manually.

### Running Locally

Requires JDK 8+ and a MySQL Connector/J 8.x or 9.x jar. Supply the jar yourself;
the example does not download dependencies automatically.

Start mydb from the repository root:

```bash
cargo build
MYDB_DATA_DIR=/tmp/mydb/jdbc-example-data \
  ./target/debug/mydb --host 127.0.0.1 --port 4837
```

Run the example in another terminal:

```bash
export MYDB_CONNECTOR_J_JAR=/absolute/path/mysql-connector-j-8.4.0.jar
MYDB_PORT=4837 bash examples/run_java_demo.sh
```

The script compiles the source with UTF-8 encoding, places class files in an
isolated temporary directory under `/tmp/mydb`, and deletes them on exit. Each
stage prints `PASS`. After all checks and cleanup succeed, it prints
`PASS all JDBC examples`. Any exception or unexpected result produces a nonzero
exit status.

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `MYDB_CONNECTOR_J_JAR` | Required | Path to the Connector/J jar; needed only by the runner script |
| `MYDB_HOST` / `MYDB_PORT` | `127.0.0.1` / `8738` | Server address |
| `MYDB_USER` / `MYDB_PASSWORD` | `root` / `root` | Credentials; an empty password is allowed |
| `MYDB_DB` | `test` | Database used for the initial connection |
| `MYDB_JDBC_SERVER_PREPARED` | `true` | Sets `useServerPrepStmts`; accepts only `true` / `false`. Silent client-preparation fallback for unsupported statements is disabled |
| `MYDB_JDBC_URL` | Constructed from the address and database above | Optional complete JDBC URL; can add or override connection properties |

The password is read from an environment variable, so it need not appear in
source code or command-line arguments. Default connection properties are intended
for local examples and set `sslMode=DISABLED` and `allowPublicKeyRetrieval=true`.
For remote connections, use `MYDB_JDBC_URL` to configure
`sslMode=VERIFY_IDENTITY`, trusted certificates, and other properties.

Verify server-side and client-side preparation separately:

```bash
MYDB_PORT=4837 MYDB_JDBC_SERVER_PREPARED=true bash examples/run_java_demo.sh
MYDB_PORT=4837 MYDB_JDBC_SERVER_PREPARED=false bash examples/run_java_demo.sh
```

The transaction example keeps DDL outside business transactions, explicitly calls
`commit` / `rollback`, and rolls back any remaining transaction before restoring
autocommit. It checks duplicate-key errors using `SQLException` error code `1062`
and SQLState `23000`; other SQL exceptions propagate to the caller.

JDBC sends the entire `CREATE PROCEDURE ... BEGIN ... END` as a single statement;
do not send the mysql CLI's `DELIMITER` command. The call consumes all result sets
and update counts through `execute` / `getMoreResults` before reading OUT/INOUT
parameters through `getBigDecimal` / `getInt`.
