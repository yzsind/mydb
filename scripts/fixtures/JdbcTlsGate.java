import com.mysql.cj.jdbc.JdbcConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;

/** Run only against an owned disposable instance with the default root/root account. */
public class JdbcTlsGate {
    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private static Connection connect(String url, String user, String password) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        properties.setProperty("connectTimeout", "5000");
        properties.setProperty("socketTimeout", "10000");
        return DriverManager.getConnection(url, properties);
    }

    private static void verifyTls(Connection connection, String label, String expectedMode) throws SQLException {
        require(!connection.unwrap(JdbcConnection.class).getPropertySet()
                .getBooleanProperty("allowPublicKeyRetrieval").getValue(),
                "Public-key retrieval must remain disabled");
        String mode = connection.unwrap(JdbcConnection.class).getPropertySet()
                .getEnumProperty("sslMode").getValue().toString();
        require(mode.equals(expectedMode), "Unexpected TLS mode: " + mode);
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rows = statement.executeQuery("SELECT 1")) {
                require(rows.next() && rows.getInt(1) == 1 && !rows.next(), "SELECT 1");
            }
            try (ResultSet rows = statement.executeQuery("SHOW SESSION STATUS LIKE 'Ssl_version'")) {
                require(rows.next() && !rows.getString(2).isEmpty(), "Missing TLS version");
                System.out.println("PASS " + label + ": " + rows.getString(2)
                        + ", sslMode=" + mode + ", allowPublicKeyRetrieval=false");
            }
            try (ResultSet rows = statement.executeQuery("SHOW SESSION STATUS LIKE 'Ssl_cipher'")) {
                require(rows.next() && !rows.getString(2).isEmpty(), "Missing TLS cipher");
                System.out.println("  cipher=" + rows.getString(2));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        require(args.length == 1, "usage: JdbcTlsGate <port>");
        Class.forName("com.mysql.cj.jdbc.Driver");
        String base = "jdbc:mysql://127.0.0.1:" + args[0] + "/test";
        String user = "jdbc_tls_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        String account = "'" + user + "'@'localhost'";
        String password = "tls-gate-password";
        try (Connection admin = connect(base, "root", "root");
             Statement statement = admin.createStatement()) {
            verifyTls(admin, "first root connection", "PREFERRED");
            statement.execute("CREATE USER " + account + " IDENTIFIED BY '" + password + "'");
            try {
                statement.execute("GRANT SELECT ON test.* TO " + account);
                try (ResultSet rows = statement.executeQuery(
                        "SELECT plugin FROM mysql.user WHERE User='" + user + "' AND Host='localhost'")) {
                    require(rows.next() && rows.getString(1).equals("caching_sha2_password"),
                            "Default authentication plugin must remain caching_sha2_password");
                }
                // Account creation may populate the fast-auth cache, so clear it explicitly.
                statement.execute("FLUSH PRIVILEGES");
                try (Connection ignored = connect(base + "?sslMode=DISABLED", user, password)) {
                    throw new AssertionError("Cold non-TLS authentication unexpectedly succeeded");
                } catch (SQLException error) {
                    require(error.getMessage().contains("Public Key Retrieval is not allowed"),
                            "Unexpected non-TLS error: " + error);
                    System.out.println("PASS cold non-TLS connection rejected: " + error.getMessage());
                }
                for (String mode : new String[] {"default", "REQUIRED"}) {
                    statement.execute("FLUSH PRIVILEGES");
                    String url = base + (mode.equals("default") ? "" : "?sslMode=" + mode);
                    try (Connection connection = connect(url, user, password)) {
                        verifyTls(connection, mode + " cold authentication",
                                mode.equals("default") ? "PREFERRED" : mode);
                    }
                    try (Connection connection = connect(url, user, password)) {
                        verifyTls(connection, mode + " cached authentication",
                                mode.equals("default") ? "PREFERRED" : mode);
                    }
                }
                statement.execute("FLUSH PRIVILEGES");
                try (Connection ignored = connect(base, user, "wrong-password")) {
                    throw new AssertionError("Wrong password was accepted");
                } catch (SQLException error) {
                    require(error.getErrorCode() == 1045, "Unexpected wrong-password error: " + error);
                    System.out.println("PASS wrong password rejected: 1045");
                }
            } finally {
                statement.execute("DROP USER " + account);
            }
        }
        System.out.println("PASS JDBC TLS gate");
    }
}
