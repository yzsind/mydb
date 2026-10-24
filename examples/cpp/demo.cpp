// Self-checking C++17 examples using the MySQL C API with RAII resource ownership.
#include <mysql.h>

#include <array>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

using Row = std::vector<std::optional<std::string>>;
using ResultSet = std::vector<Row>;
using Results = std::vector<ResultSet>;

static std::string env(const char* name, const char* fallback) {
    const char* value = std::getenv(name);
    return value == nullptr ? fallback : value;
}

static void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

class SqlError : public std::runtime_error {
public:
    unsigned int code;
    std::string state;
    SqlError(unsigned int code, const char* state, const char* message)
        : std::runtime_error(std::to_string(code) + "/" + state + ": " + message), code(code), state(state) {}
};

class Connection {
    std::unique_ptr<MYSQL, decltype(&mysql_close)> handle_{mysql_init(nullptr), mysql_close};

public:
    Connection() {
        require(handle_ != nullptr, "mysql_init failed");
        unsigned int connect_timeout = 5, io_timeout = 15;
        mysql_ssl_mode ssl_mode = SSL_MODE_DISABLED;
        require(mysql_options(get(), MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout) == 0 &&
                mysql_options(get(), MYSQL_OPT_READ_TIMEOUT, &io_timeout) == 0 &&
                mysql_options(get(), MYSQL_OPT_WRITE_TIMEOUT, &io_timeout) == 0 &&
                mysql_options(get(), MYSQL_SET_CHARSET_NAME, "utf8mb4") == 0 &&
                mysql_options(get(), MYSQL_OPT_SSL_MODE, &ssl_mode) == 0,
                "Failed to set connection options");
        const auto port_text = env("MYDB_PORT", "8738");
        std::size_t end = 0;
        const auto port = std::stoul(port_text, &end);
        require(end == port_text.size() && port > 0 && port <= 65535, "Invalid MYDB_PORT");
        // Use TCP even when MYDB_HOST=localhost; do not silently select a Unix socket.
        unsigned int protocol = MYSQL_PROTOCOL_TCP;
        require(mysql_options(get(), MYSQL_OPT_PROTOCOL, &protocol) == 0, "Failed to select TCP");
        if (!mysql_real_connect(get(), env("MYDB_HOST", "127.0.0.1").c_str(),
                env("MYDB_USER", "root").c_str(), env("MYDB_PASSWORD", "root").c_str(),
                env("MYDB_DB", "test").c_str(), static_cast<unsigned int>(port), nullptr,
                CLIENT_MULTI_RESULTS)) fail();
    }
    MYSQL* get() const { return handle_.get(); }
    [[noreturn]] void fail() const {
        throw SqlError(mysql_errno(get()), mysql_sqlstate(get()), mysql_error(get()));
    }
    Results query(const std::string& sql) {
        if (mysql_real_query(get(), sql.data(), static_cast<unsigned long>(sql.size())) != 0) fail();
        Results sets;
        int next;
        do {
            std::unique_ptr<MYSQL_RES, decltype(&mysql_free_result)> result(mysql_store_result(get()), mysql_free_result);
            if (result) {
                ResultSet rows;
                while (MYSQL_ROW row = mysql_fetch_row(result.get())) {
                    const auto* lengths = mysql_fetch_lengths(result.get());
                    require(lengths != nullptr, "Missing result lengths");
                    Row values;
                    for (unsigned int i = 0; i < mysql_num_fields(result.get()); ++i) {
                        // Preserve SQL NULL and embedded NUL bytes; rows are not C strings.
                        values.push_back(row[i] == nullptr ? std::nullopt :
                            std::optional<std::string>(std::string(row[i], lengths[i])));
                    }
                    rows.push_back(std::move(values));
                }
                if (mysql_errno(get()) != 0) fail();
                sets.push_back(std::move(rows));
            } else if (mysql_field_count(get()) != 0) {
                fail();
            }
            result.reset();
            next = mysql_next_result(get());
            if (next > 0) fail();
        } while (next == 0);
        return sets;
    }
    void use(const std::string& schema) {
        if (mysql_select_db(get(), schema.c_str()) != 0) fail();
    }
};

struct Parameter {
    std::string value;
    enum_field_types type = MYSQL_TYPE_STRING;
};

class Statement {
    std::unique_ptr<MYSQL_STMT, decltype(&mysql_stmt_close)> handle_;
    [[noreturn]] void fail() const {
        throw SqlError(mysql_stmt_errno(handle_.get()), mysql_stmt_sqlstate(handle_.get()), mysql_stmt_error(handle_.get()));
    }

public:
    Statement(Connection& connection, const std::string& sql)
        : handle_(mysql_stmt_init(connection.get()), mysql_stmt_close) {
        require(handle_ != nullptr, "mysql_stmt_init failed");
        if (mysql_stmt_prepare(handle_.get(), sql.data(), static_cast<unsigned long>(sql.size())) != 0) fail();
    }
    my_ulonglong execute(std::vector<Parameter> parameters) {
        require(mysql_stmt_param_count(handle_.get()) == parameters.size(), "Parameter count mismatch");
        std::vector<MYSQL_BIND> bindings(parameters.size());
        std::vector<unsigned long> lengths(parameters.size());
        for (std::size_t i = 0; i < parameters.size(); ++i) {
            lengths[i] = static_cast<unsigned long>(parameters[i].value.size());
            bindings[i].buffer_type = parameters[i].type;
            bindings[i].buffer = parameters[i].value.data();
            bindings[i].buffer_length = lengths[i];
            bindings[i].length = &lengths[i];
        }
        if (!bindings.empty() && mysql_stmt_bind_param(handle_.get(), bindings.data()) != 0) fail();
        if (mysql_stmt_execute(handle_.get()) != 0) fail();
        return mysql_stmt_affected_rows(handle_.get());
    }
    my_ulonglong insert_id() const { return mysql_stmt_insert_id(handle_.get()); }
};

class Transaction {
    Connection& connection_;
    bool active_ = true;

public:
    explicit Transaction(Connection& connection) : connection_(connection) { connection_.query("START TRANSACTION"); }
    ~Transaction() {
        // Destructors cannot throw; explicit commit/rollback paths report errors to the caller.
        if (active_) mysql_rollback(connection_.get());
    }
    void commit() {
        if (mysql_commit(connection_.get()) != 0) connection_.fail();
        active_ = false;
    }
    void rollback() {
        if (mysql_rollback(connection_.get()) != 0) connection_.fail();
        active_ = false;
    }
};

static void balance(Connection& connection, const std::string& name, const std::string& expected) {
    Statement bind(connection, "SET @account_name=?");
    bind.execute({{name}});
    require(connection.query("SELECT balance FROM accounts WHERE name=@account_name") ==
            Results{ResultSet{Row{expected}}}, "Unexpected balance for " + name);
}

static void ddl_dml_select(Connection& connection) {
    require(mysql_ping(connection.get()) == 0, "mysql_ping failed");
    require(connection.query("SELECT 1") == Results{ResultSet{Row{"1"}}}, "SELECT 1");
    connection.query("CREATE TABLE accounts(id BIGINT PRIMARY KEY AUTO_INCREMENT,"
        "name VARCHAR(64) NOT NULL UNIQUE, balance DECIMAL(12,2) NOT NULL,"
        "note VARCHAR(128), payload VARBINARY(16), created_at DATETIME(6))");
    connection.query("ALTER TABLE accounts ADD COLUMN active BOOLEAN NOT NULL DEFAULT TRUE");
    connection.query("CREATE INDEX accounts_balance_idx ON accounts(balance)");
    const std::string payload("\0\xff\x07", 3);
    const std::string created = "2026-01-02 03:04:05.123456";
    Statement insert(connection, "INSERT INTO accounts(name,balance,note,payload,created_at) VALUES(?,?,?,?,?)");
    require(insert.execute({{"Alice 中文🔎"}, {"1000.00"}, {"", MYSQL_TYPE_NULL},
            {payload, MYSQL_TYPE_BLOB}, {created}}) == 1, "INSERT affected rows");
    const auto id = insert.insert_id();
    require(id > 0, "Generated key");
    {
        // The C API has no addBatch: reuse a prepared statement in an atomic transaction.
        Transaction tx(connection);
        Statement batch(connection, "INSERT INTO accounts(name,balance) VALUES(?,?)");
        require(batch.execute({{"Bob"}, {"500.00"}}) == 1, "Batch Bob");
        require(batch.execute({{"Carol"}, {"250.00"}}) == 1, "Batch Carol");
        tx.commit();
    }
    Statement update(connection, "UPDATE accounts SET balance=balance+? WHERE name=?");
    require(update.execute({{"10.00"}, {"Carol"}}) == 1, "UPDATE affected rows");
    balance(connection, "Carol", "260.00");
    Statement remove(connection, "DELETE FROM accounts WHERE name=?");
    require(remove.execute({{"Carol"}}) == 1, "DELETE affected rows");
    require(connection.query("SELECT id,name,balance,note,payload,created_at,active FROM accounts WHERE id=" +
            std::to_string(id)) == Results{ResultSet{Row{std::to_string(id), "Alice 中文🔎", "1000.00",
            std::nullopt, payload, created, "1"}}}, "Typed SELECT round trip");
    require(connection.query("SELECT COUNT(*),SUM(balance) FROM accounts") ==
            Results{ResultSet{Row{"2", "1500.00"}}}, "Aggregate SELECT");
    std::cout << "PASS C++ connection, DDL, prepared DML, batch, generated key and typed SELECT\n";
}

static void transactions(Connection& connection, Connection& observer) {
    connection.query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");
    observer.query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");
    {
        Transaction tx(connection);
        Statement transfer(connection, "UPDATE accounts SET balance=balance+? WHERE name=?");
        require(transfer.execute({{"-100.00"}, {"Alice 中文🔎"}}) == 1, "Debit affected rows");
        require(transfer.execute({{"100.00"}, {"Bob"}}) == 1, "Credit affected rows");
        balance(connection, "Alice 中文🔎", "900.00");
        balance(observer, "Alice 中文🔎", "1000.00");
        connection.query("SAVEPOINT kept");
        connection.query("UPDATE accounts SET balance=0");
        connection.query("ROLLBACK TO SAVEPOINT kept");
        connection.query("RELEASE SAVEPOINT kept");
        tx.commit();
    }
    balance(observer, "Alice 中文🔎", "900.00");
    balance(observer, "Bob", "600.00");
    {
        Transaction tx(connection);
        connection.query("DELETE FROM accounts");
        tx.rollback();
    }
    balance(connection, "Alice 中文🔎", "900.00");
    balance(connection, "Bob", "600.00");
    {
        Transaction tx(connection);
        connection.query("UPDATE accounts SET balance=balance+10");
        try {
            Statement duplicate(connection, "INSERT INTO accounts(name,balance) VALUES(?,?)");
            duplicate.execute({{"Bob"}, {"1.00"}});
            throw std::runtime_error("Duplicate key accepted");
        } catch (const SqlError& error) {
            if (error.code != 1062 || error.state != "23000") throw;
            tx.rollback();
        }
    }
    balance(connection, "Alice 中文🔎", "900.00");
    balance(observer, "Bob", "600.00");
    std::cout << "PASS C++ commit, rollback, savepoints, cross-connection visibility and error recovery\n";
}

static void procedure(Connection& connection) {
    connection.query("CREATE PROCEDURE account_summary("
        "IN p_name VARCHAR(64), OUT p_balance DECIMAL(12,2), INOUT p_calls INT) "
        "READS SQL DATA BEGIN "
        "SELECT balance INTO p_balance FROM accounts WHERE name=p_name; "
        "SET p_calls=p_calls+1; "
        "SELECT name,balance FROM accounts WHERE name=p_name; "
        "SELECT COUNT(*) AS account_count FROM accounts; END");
    Statement inputs(connection, "SET @name=?, @balance=NULL, @calls=?");
    inputs.execute({{"Alice 中文🔎"}, {"10"}});
    // query() uses mysql_next_result to consume every result before reading OUT parameters.
    require(connection.query("CALL account_summary(@name,@balance,@calls)") ==
            Results{ResultSet{Row{"Alice 中文🔎", "900.00"}}, ResultSet{Row{"2"}}}, "Procedure result sets");
    require(connection.query("SELECT @balance,@calls") == Results{ResultSet{Row{"900.00", "11"}}},
            "Procedure OUT/INOUT");
    connection.query("DROP PROCEDURE account_summary");
    connection.query("DROP INDEX accounts_balance_idx ON accounts");
    connection.query("DROP TABLE accounts");
    std::cout << "PASS C++ CALL, IN/OUT/INOUT, multiple result sets and DROP DDL\n";
}

static void run() {
    Connection connection;
    std::random_device random;
    std::ostringstream name;
    name << "cpp_demo_" << std::hex << std::setfill('0');
    for (int i = 0; i < 4; ++i) name << std::setw(8) << random();
    const auto schema = name.str();
    connection.query("CREATE DATABASE " + schema);
    std::cout << "Using isolated schema " << schema << '\n';
    const auto cleanup = [&] {
        if (mysql_rollback(connection.get()) != 0) connection.fail();
        connection.query("DROP DATABASE " + schema);
        std::cout << "PASS C++ cleanup: dropped " << schema << '\n';
    };
    try {
        connection.use(schema);
        ddl_dml_select(connection);
        Connection observer;
        observer.use(schema);
        transactions(connection, observer);
        procedure(connection);
    } catch (...) {
        try { cleanup(); }
        catch (const std::exception& error) { std::cerr << "Cleanup failed: " << error.what() << '\n'; }
        throw;
    }
    cleanup();
}

int main() {
    try {
        run();
        std::cout << "PASS all C++ examples\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
