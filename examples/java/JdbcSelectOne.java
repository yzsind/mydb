/** Minimal MySQL Connector/J smoke used by scripts/run_client_compat_matrix.sh. */
public class JdbcSelectOne {
    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("usage: JdbcSelectOne <host> <port> <user> <password> <database>");
            System.exit(2);
        }
        String url = "jdbc:mysql://" + args[0] + ":" + args[1] + "/" + args[4]
                + "?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=5000";
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (java.sql.Connection connection = java.sql.DriverManager.getConnection(url, args[2], args[3]);
             java.sql.Statement statement = connection.createStatement();
             java.sql.ResultSet resultSet = statement.executeQuery("SELECT 1 AS v")) {
            if (!resultSet.next() || resultSet.getInt("v") != 1) {
                System.exit(1);
            }
        }
    }
}
