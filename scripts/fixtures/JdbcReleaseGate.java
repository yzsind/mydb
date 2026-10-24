import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;

/** Typed server-prepared JDBC gate against an owned disposable instance. */
public class JdbcReleaseGate {
    private static final Calendar UTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    private static final BigDecimal VALUE = new BigDecimal("1234567890.12500000");
    private static final byte[] BYTES = new byte[] {0, (byte)255, (byte)128, 7};
    private static final byte[] LOB = new byte[20000];
    static { for (int i = 0; i < LOB.length; i++) LOB[i] = (byte)(i % 251); }
    private static void require(boolean condition, String detail) {
        if (!condition) throw new AssertionError(detail);
    }
    private static String url(String port) {
        return "jdbc:mysql://127.0.0.1:" + port + "/test?useSSL=false&allowPublicKeyRetrieval=true"
            + "&useServerPrepStmts=true&cachePrepStmts=true&characterEncoding=UTF-8&connectionTimeZone=UTC";
    }
    private static void verify(Connection connection) throws Exception {
        String sql = "SELECT id,v,u,b,p,ts,s FROM jdbc_release ORDER BY id";
        try (Statement text = connection.createStatement(); ResultSet rows = text.executeQuery(sql);
             PreparedStatement binary = connection.prepareStatement(sql); ResultSet prepared = binary.executeQuery()) {
            require(binary.getClass().getName().contains("ServerPreparedStatement"), "driver fell back to client preparation");
            ResultSetMetaData a = rows.getMetaData(), b = prepared.getMetaData();
            require(a.getColumnCount() == 7 && b.getColumnCount() == 7, "column count");
            for (int i = 1; i <= 7; i++) {
                require(a.getColumnType(i) == b.getColumnType(i), "protocol type " + i);
                require(a.getPrecision(i) == b.getPrecision(i), "protocol precision " + i);
                require(a.getScale(i) == b.getScale(i), "protocol scale " + i);
                require(a.isSigned(i) == b.isSigned(i), "protocol signedness " + i);
            }
            require(a.getScale(2) == 8 && !a.isSigned(3), "decimal/unsigned metadata");
            for (int id = 1; id <= 5; id++) {
                require(rows.next() && prepared.next(), "missing row " + id);
                for (ResultSet result : new ResultSet[] {rows, prepared}) {
                    require(result.getLong(1) == id, "id");
                    require(result.getBigDecimal(2).compareTo(VALUE.add(BigDecimal.ONE)) == 0, "decimal");
                    require(result.getString(3).equals("18446744073709551615"), "unsigned maximum");
                    require(Arrays.equals(result.getBytes(4), BYTES), "binary");
                    require(Arrays.equals(result.getBytes(5), LOB), "LOB");
                    require(result.getTimestamp(6, UTC).equals(Timestamp.valueOf("2024-01-02 03:04:05.123456")), "timestamp");
                    require(result.getString(7).equals("中文🔎"), "unicode");
                }
            }
            require(!rows.next() && !prepared.next(), "extra rows");
        }
    }
    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection connection = DriverManager.getConnection(url(args[0]), "root", "root")) {
            if (args[1].equals("verify")) { verify(connection); System.out.println("PASS JDBC reopen"); return; }
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE jdbc_release(id BIGINT PRIMARY KEY,v DECIMAL(25,8),u BIGINT UNSIGNED,b VARBINARY(16),p LONGBLOB,ts DATETIME(6),s VARCHAR(64),KEY kv(v))");
            }
            connection.setAutoCommit(false);
            try (PreparedStatement insert = connection.prepareStatement("INSERT INTO jdbc_release VALUES(?,?,?,?,?,?,?)")) {
                require(insert.getClass().getName().contains("ServerPreparedStatement"), "insert preparation");
                for (int id = 1; id <= 5; id++) {
                    insert.setLong(1, id); insert.setBigDecimal(2, VALUE);
                    insert.setBigDecimal(3, new BigDecimal("18446744073709551615"));
                    insert.setBytes(4, BYTES); insert.setBytes(5, LOB);
                    insert.setTimestamp(6, Timestamp.valueOf("2024-01-02 03:04:05.123456"), UTC);
                    insert.setString(7, "中文🔎"); insert.addBatch();
                }
                require(insert.executeBatch().length == 5, "batch results");
            }
            try (Statement statement = connection.createStatement()) { statement.executeUpdate("UPDATE jdbc_release SET v=v+1"); }
            Savepoint kept = connection.setSavepoint("kept");
            try (Statement statement = connection.createStatement()) { statement.executeUpdate("UPDATE jdbc_release SET v=v+100"); }
            connection.rollback(kept); connection.commit(); verify(connection);
            try (Statement statement = connection.createStatement()) { statement.executeUpdate("DELETE FROM jdbc_release"); }
            connection.rollback(); verify(connection);
            try (Statement statement = connection.createStatement()) {
                try { statement.executeUpdate("INSERT INTO jdbc_release(id) VALUES(1)"); throw new AssertionError("duplicate accepted"); }
                catch (SQLException error) { require(error.getErrorCode() == 1062, "duplicate error"); }
            }
            connection.rollback();
            System.out.println("PASS JDBC typed text/binary metadata, server preparation, batch, savepoint, commit/rollback and duplicate rejection");
        }
    }
}
