import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

/** Runnable JDBC examples with result checks and an isolated, disposable schema. */
public class JdbcDemo {
    private static final byte[] PAYLOAD = {0, (byte) 255, 7};
    private static final Timestamp CREATED_AT = Timestamp.valueOf("2026-01-02 03:04:05.123456");

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null ? fallback : value;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    private static void verifyBalance(Connection connection, long id, String expected) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("SELECT balance FROM accounts WHERE id = ?")) {
            query.setLong(1, id);
            try (ResultSet rows = query.executeQuery()) {
                require(rows.next(), "Missing account " + id);
                require(rows.getBigDecimal("balance").compareTo(new BigDecimal(expected)) == 0,
                        "Unexpected balance for account " + id + ": " + rows.getBigDecimal("balance"));
                require(!rows.next(), "Duplicate account " + id);
            }
        }
    }

    private static void demoConnection(Connection connection) throws SQLException {
        require(connection.getAutoCommit(), "New connections should use autocommit");
        require(connection.isValid(5), "Connection validation failed");
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT 1 AS value")) {
            require(rows.next() && rows.getInt("value") == 1 && !rows.next(), "SELECT 1 failed");
        }
        System.out.println("PASS connection, isValid and SELECT 1 ("
                + connection.getMetaData().getDriverName() + " "
                + connection.getMetaData().getDriverVersion() + ")");
    }

    private static void demoDdl(Connection connection) throws SQLException {
        execute(connection, "CREATE TABLE accounts ("
                + "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
                + "name VARCHAR(64) NOT NULL UNIQUE, balance DECIMAL(12,2) NOT NULL, "
                + "note VARCHAR(128), payload VARBINARY(16), created_at DATETIME(6))");
        execute(connection, "ALTER TABLE accounts ADD COLUMN active BOOLEAN NOT NULL DEFAULT TRUE");
        execute(connection, "CREATE INDEX accounts_balance_idx ON accounts(balance)");
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT active FROM accounts")) {
            require(rows.getMetaData().getColumnCount() == 1 && !rows.next(), "DDL schema mismatch");
        }
        System.out.println("PASS DDL: CREATE TABLE, ALTER TABLE and CREATE INDEX");
    }

    private static long demoDml(Connection connection) throws SQLException {
        long aliceId;
        String sql = "INSERT INTO accounts(name,balance,note,payload,created_at) VALUES(?,?,?,?,?)";
        try (PreparedStatement insert = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            insert.setString(1, "Alice 中文🔎");
            insert.setBigDecimal(2, new BigDecimal("1000.00"));
            insert.setNull(3, Types.VARCHAR);
            insert.setBytes(4, PAYLOAD);
            insert.setTimestamp(5, CREATED_AT);
            require(insert.executeUpdate() == 1, "INSERT affected rows");
            try (ResultSet keys = insert.getGeneratedKeys()) {
                require(keys.next(), "Missing generated key");
                aliceId = keys.getLong(1);
                require(aliceId > 0 && !keys.next(), "Invalid generated key");
            }
        }
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO accounts(name,balance) VALUES(?,?)")) {
            insert.setString(1, "Bob");
            insert.setBigDecimal(2, new BigDecimal("500.00"));
            insert.addBatch();
            insert.setString(1, "Carol");
            insert.setBigDecimal(2, new BigDecimal("250.00"));
            insert.addBatch();
            int[] counts = insert.executeBatch();
            require(counts.length == 2, "Batch result count");
            for (int count : counts) {
                require(count == 1 || count == Statement.SUCCESS_NO_INFO, "Batch INSERT failed: " + count);
            }
        }
        try (PreparedStatement update = connection.prepareStatement(
                "UPDATE accounts SET balance = balance + ? WHERE name = ?")) {
            update.setBigDecimal(1, new BigDecimal("10.00"));
            update.setString(2, "Carol");
            require(update.executeUpdate() == 1, "UPDATE affected rows");
        }
        verifyNamedBalance(connection, "Carol", "260.00");
        try (PreparedStatement delete = connection.prepareStatement("DELETE FROM accounts WHERE name = ?")) {
            delete.setString(1, "Carol");
            require(delete.executeUpdate() == 1, "DELETE affected rows");
        }
        System.out.println("PASS DML: parameter binding, generated keys, batch INSERT, UPDATE and DELETE");
        return aliceId;
    }

    private static void demoSelect(Connection connection, long aliceId) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(
                "SELECT id,name,balance,note,payload,created_at,active FROM accounts WHERE id = ?")) {
            query.setLong(1, aliceId);
            try (ResultSet rows = query.executeQuery()) {
                require(rows.getMetaData().getColumnCount() == 7, "SELECT column count");
                require(rows.next(), "Missing inserted row");
                require(rows.getLong("id") == aliceId, "BIGINT round trip");
                require("Alice 中文🔎".equals(rows.getString("name")), "Unicode round trip");
                require(rows.getBigDecimal("balance").compareTo(new BigDecimal("1000.00")) == 0,
                        "DECIMAL round trip");
                require(rows.getString("note") == null && rows.wasNull(), "SQL NULL round trip");
                require(Arrays.equals(rows.getBytes("payload"), PAYLOAD), "Binary round trip");
                require(CREATED_AT.equals(rows.getTimestamp("created_at")), "DATETIME(6) round trip");
                require(rows.getBoolean("active"), "ALTER TABLE default");
                require(!rows.next(), "Unexpected SELECT row");
            }
        }
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(
                     "SELECT COUNT(*) AS account_count, SUM(balance) AS total FROM accounts")) {
            require(rows.next() && rows.getLong("account_count") == 2, "Unexpected row count after DML");
            require(rows.getBigDecimal("total").compareTo(new BigDecimal("1500.00")) == 0,
                    "Aggregate result");
            require(!rows.next(), "Unexpected aggregate row");
        }
        System.out.println("PASS SELECT: metadata, BIGINT, DECIMAL, Unicode, NULL, binary, timestamp and aggregate");
    }

    private static void demoTransactions(Connection connection, Connection observer, long aliceId)
            throws SQLException {
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        observer.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        connection.setAutoCommit(false);
        try {
            try (PreparedStatement debit = connection.prepareStatement(
                    "UPDATE accounts SET balance = balance - ? WHERE id = ?")) {
                debit.setBigDecimal(1, new BigDecimal("100.00"));
                debit.setLong(2, aliceId);
                require(debit.executeUpdate() == 1, "Debit affected rows");
            }
            try (Statement credit = connection.createStatement()) {
                require(credit.executeUpdate("UPDATE accounts SET balance = balance + 100 WHERE name = 'Bob'") == 1,
                        "Credit affected rows");
            }
            verifyBalance(connection, aliceId, "900.00");
            verifyBalance(observer, aliceId, "1000.00");
            Savepoint saved = connection.setSavepoint("before_temporary_change");
            execute(connection, "UPDATE accounts SET balance = 0");
            connection.rollback(saved);
            connection.releaseSavepoint(saved);
            verifyBalance(connection, aliceId, "900.00");
            connection.commit();
            verifyBalance(observer, aliceId, "900.00");
            verifyNamedBalance(observer, "Bob", "600.00");

            execute(connection, "DELETE FROM accounts");
            connection.rollback();
            verifyBalance(connection, aliceId, "900.00");
            verifyNamedBalance(connection, "Bob", "600.00");
            verifyBalance(observer, aliceId, "900.00");

            execute(connection, "UPDATE accounts SET balance = balance + 10");
            try (PreparedStatement duplicate = connection.prepareStatement(
                    "INSERT INTO accounts(id,name,balance) VALUES(?,?,?)")) {
                duplicate.setLong(1, aliceId);
                duplicate.setString(2, "Duplicate");
                duplicate.setBigDecimal(3, BigDecimal.ZERO);
                duplicate.executeUpdate();
                throw new IllegalStateException("Duplicate primary key was accepted");
            } catch (SQLException expected) {
                if (expected.getErrorCode() != 1062 || !"23000".equals(expected.getSQLState())) {
                    throw expected;
                }
                connection.rollback();
                System.out.println("Expected duplicate key: SQLState=" + expected.getSQLState()
                        + ", code=" + expected.getErrorCode());
            }
            verifyBalance(connection, aliceId, "900.00");
            verifyNamedBalance(connection, "Bob", "600.00");
            verifyBalance(observer, aliceId, "900.00");
        } finally {
            // Never restore autocommit while a transaction still has uncommitted work.
            connection.rollback();
            connection.setAutoCommit(true);
        }
        System.out.println("PASS transactions: READ COMMITTED, visibility, savepoint, commit, rollback and error recovery");
    }

    private static void verifyNamedBalance(Connection connection, String name, String expected) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("SELECT balance FROM accounts WHERE name = ?")) {
            query.setString(1, name);
            try (ResultSet rows = query.executeQuery()) {
                require(rows.next() && rows.getBigDecimal(1).compareTo(new BigDecimal(expected)) == 0 && !rows.next(),
                        "Unexpected balance for " + name);
            }
        }
    }

    private static void demoProcedure(Connection connection, long aliceId) throws SQLException {
        // DELIMITER is a mysql CLI command; JDBC sends the complete body as one statement.
        execute(connection, "CREATE PROCEDURE account_summary("
                + "IN p_id BIGINT, OUT p_balance DECIMAL(12,2), INOUT p_calls INT) "
                + "READS SQL DATA BEGIN "
                + "SELECT balance INTO p_balance FROM accounts WHERE id = p_id; "
                + "SET p_calls = p_calls + 1; "
                + "SELECT name,balance FROM accounts WHERE id = p_id; "
                + "SELECT COUNT(*) AS account_count FROM accounts; END");
        try (CallableStatement call = connection.prepareCall("{call account_summary(?,?,?)}")) {
            call.setLong(1, aliceId);
            call.registerOutParameter(2, Types.DECIMAL);
            call.setInt(3, 10);
            call.registerOutParameter(3, Types.INTEGER);
            int resultCount = 0;
            boolean hasRows = call.execute();
            // Consume every result set/update count before reading OUT parameters or reusing the connection.
            while (true) {
                if (hasRows) {
                    try (ResultSet rows = call.getResultSet()) {
                        resultCount++;
                        require(rows.next(), "Empty procedure result " + resultCount);
                        if (resultCount == 1) {
                            require("Alice 中文🔎".equals(rows.getString("name")), "Procedure account name");
                            require(rows.getBigDecimal("balance").compareTo(new BigDecimal("900.00")) == 0,
                                    "Procedure result balance");
                        } else {
                            require(resultCount == 2 && rows.getLong("account_count") == 2,
                                    "Procedure account count");
                        }
                        require(!rows.next(), "Unexpected procedure row");
                    }
                } else if (call.getUpdateCount() == -1) {
                    break;
                }
                hasRows = call.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
            }
            require(resultCount == 2, "Expected two procedure result sets");
            require(call.getBigDecimal(2).compareTo(new BigDecimal("900.00")) == 0, "OUT balance");
            require(call.getInt(3) == 11 && !call.wasNull(), "INOUT call count");
        }
        verifyBalance(connection, aliceId, "900.00");
        execute(connection, "DROP PROCEDURE account_summary");
        execute(connection, "DROP INDEX accounts_balance_idx ON accounts");
        execute(connection, "DROP TABLE accounts");
        System.out.println("PASS CallableStatement: IN/OUT/INOUT, multiple result sets and DROP DDL");
    }

    public static void main(String[] args) throws Exception {
        require(args.length == 0, "Configure this example with MYDB_* environment variables; see examples/README.md");
        String serverPrepared = env("MYDB_JDBC_SERVER_PREPARED", "true");
        require(serverPrepared.equals("true") || serverPrepared.equals("false"),
                "MYDB_JDBC_SERVER_PREPARED must be true or false");
        String url = env("MYDB_JDBC_URL", "jdbc:mysql://" + env("MYDB_HOST", "127.0.0.1") + ":"
                + env("MYDB_PORT", "8738") + "/" + env("MYDB_DB", "test"));
        Properties properties = new Properties();
        properties.setProperty("user", env("MYDB_USER", "root"));
        properties.setProperty("password", env("MYDB_PASSWORD", "root"));
        properties.setProperty("sslMode", "DISABLED");
        properties.setProperty("allowPublicKeyRetrieval", "true");
        properties.setProperty("connectTimeout", "5000");
        properties.setProperty("socketTimeout", "15000");
        properties.setProperty("characterEncoding", "UTF-8");
        properties.setProperty("connectionTimeZone", "UTC");
        properties.setProperty("useServerPrepStmts", serverPrepared);
        properties.setProperty("emulateUnsupportedPstmts", "false");
        Class.forName("com.mysql.cj.jdbc.Driver");

        String schema = "jdbc_demo_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection connection = DriverManager.getConnection(url, properties)) {
            demoConnection(connection);
            // Only drop a schema that this run successfully created; never drop a preexisting database.
            execute(connection, "CREATE DATABASE " + schema);
            try (AutoCloseable cleanup = () -> {
                if (!connection.getAutoCommit()) {
                    connection.rollback();
                    connection.setAutoCommit(true);
                }
                execute(connection, "DROP DATABASE " + schema);
                System.out.println("PASS cleanup: dropped " + schema);
            }) {
                connection.setCatalog(schema);
                System.out.println("Using isolated schema " + schema + ", useServerPrepStmts=" + serverPrepared);
                demoDdl(connection);
                long aliceId = demoDml(connection);
                demoSelect(connection, aliceId);
                try (Connection observer = DriverManager.getConnection(url, properties)) {
                    observer.setCatalog(schema);
                    demoTransactions(connection, observer, aliceId);
                }
                demoProcedure(connection, aliceId);
            }
        }
        System.out.println("PASS all JDBC examples");
    }
}
