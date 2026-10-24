use mysql::{prelude::Queryable, Conn, IsolationLevel, OptsBuilder, TxOpts};
use std::{
    env,
    error::Error,
    io, process,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn env_or(name: &str, fallback: &str) -> String {
    env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

fn require(condition: bool, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message).into())
    }
}

fn connect() -> Result<Conn> {
    Ok(Conn::new(
        OptsBuilder::new()
            .ip_or_hostname(Some(env_or("MYDB_HOST", "127.0.0.1")))
            .tcp_port(env_or("MYDB_PORT", "8738").parse()?)
            .user(Some(env_or("MYDB_USER", "root")))
            .pass(Some(env_or("MYDB_PASSWORD", "root")))
            .db_name(Some(env_or("MYDB_DB", "test")))
            .prefer_socket(false)
            .tcp_connect_timeout(Some(Duration::from_secs(5)))
            .read_timeout(Some(Duration::from_secs(15)))
            .write_timeout(Some(Duration::from_secs(15)))
            .init(vec!["SET NAMES utf8mb4"]),
    )?)
}

fn balance<Q: Queryable>(conn: &mut Q, name: &str, expected: &str) -> Result<()> {
    let amount: Option<String> =
        conn.exec_first("SELECT balance FROM accounts WHERE name=?", (name,))?;
    require(
        amount.as_deref() == Some(expected),
        &format!("Unexpected balance for {name}: {amount:?}"),
    )
}

fn ddl_dml_select(conn: &mut Conn) -> Result<()> {
    require(
        conn.query_first::<u8, _>("SELECT 1")? == Some(1),
        "SELECT 1",
    )?;
    conn.query_drop(
        "CREATE TABLE accounts(id BIGINT PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(64) NOT NULL UNIQUE, balance DECIMAL(12,2) NOT NULL,
        note VARCHAR(128), payload VARBINARY(16), created_at DATETIME(6))",
    )?;
    conn.query_drop("ALTER TABLE accounts ADD COLUMN active BOOLEAN NOT NULL DEFAULT TRUE")?;
    conn.query_drop("CREATE INDEX accounts_balance_idx ON accounts(balance)")?;
    let insert =
        conn.prep("INSERT INTO accounts(name,balance,note,payload,created_at) VALUES(?,?,?,?,?)")?;
    let payload = vec![0_u8, 255, 7];
    conn.exec_drop(
        &insert,
        (
            "Alice 中文🔎",
            "1000.00",
            None::<String>,
            &payload,
            "2026-01-02 03:04:05.123456",
        ),
    )?;
    let id = conn.last_insert_id();
    require(
        id > 0 && conn.affected_rows() == 1,
        "INSERT / generated key",
    )?;
    conn.close(insert)?;
    // exec_batch reuses one server-prepared statement; the transaction makes the batch atomic.
    let mut tx = conn.start_transaction(TxOpts::default())?;
    tx.exec_batch(
        "INSERT INTO accounts(name,balance) VALUES(?,?)",
        [("Bob", "500.00"), ("Carol", "250.00")],
    )?;
    tx.commit()?;
    conn.exec_drop(
        "UPDATE accounts SET balance=balance+? WHERE name=?",
        ("10.00", "Carol"),
    )?;
    require(conn.affected_rows() == 1, "UPDATE affected rows")?;
    balance(conn, "Carol", "260.00")?;
    conn.exec_drop("DELETE FROM accounts WHERE name=?", ("Carol",))?;
    require(conn.affected_rows() == 1, "DELETE affected rows")?;
    // Keep DECIMAL as text, avoiding loss of precision through floating point.
    type Account = (u64, String, String, Option<String>, Vec<u8>, String, bool);
    let row: Option<Account> = conn.exec_first(
        "SELECT id,name,balance,note,payload,CAST(created_at AS CHAR),active FROM accounts WHERE id=?", (id,))?;
    require(
        row == Some((
            id,
            "Alice 中文🔎".into(),
            "1000.00".into(),
            None,
            payload,
            "2026-01-02 03:04:05.123456".into(),
            true,
        )),
        "Typed SELECT round trip",
    )?;
    let aggregate: Option<(u64, String)> =
        conn.query_first("SELECT COUNT(*),SUM(balance) FROM accounts")?;
    require(aggregate == Some((2, "1500.00".into())), "Aggregate SELECT")?;
    println!("PASS Rust connection, DDL, prepared DML, batch, generated key and typed SELECT");
    Ok(())
}

fn transactions(conn: &mut Conn, observer: &mut Conn) -> Result<()> {
    observer.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")?;
    let opts = TxOpts::default().set_isolation_level(Some(IsolationLevel::ReadCommitted));
    let mut tx = conn.start_transaction(opts)?;
    tx.exec_drop(
        "UPDATE accounts SET balance=balance-? WHERE name=?",
        ("100.00", "Alice 中文🔎"),
    )?;
    require(tx.affected_rows() == 1, "Debit affected rows")?;
    tx.exec_drop(
        "UPDATE accounts SET balance=balance+? WHERE name=?",
        ("100.00", "Bob"),
    )?;
    require(tx.affected_rows() == 1, "Credit affected rows")?;
    balance(&mut tx, "Alice 中文🔎", "900.00")?;
    balance(observer, "Alice 中文🔎", "1000.00")?;
    tx.query_drop("SAVEPOINT kept")?;
    tx.query_drop("UPDATE accounts SET balance=0")?;
    tx.query_drop("ROLLBACK TO SAVEPOINT kept")?;
    tx.query_drop("RELEASE SAVEPOINT kept")?;
    tx.commit()?;
    balance(observer, "Alice 中文🔎", "900.00")?;
    balance(observer, "Bob", "600.00")?;

    let mut tx = conn.start_transaction(TxOpts::default())?;
    tx.query_drop("DELETE FROM accounts")?;
    require(tx.affected_rows() == 2, "Transactional DELETE")?;
    tx.rollback()?;
    balance(conn, "Alice 中文🔎", "900.00")?;
    balance(conn, "Bob", "600.00")?;

    let mut tx = conn.start_transaction(TxOpts::default())?;
    tx.query_drop("UPDATE accounts SET balance=balance+10")?;
    match tx.exec_drop(
        "INSERT INTO accounts(name,balance) VALUES(?,?)",
        ("Bob", "1.00"),
    ) {
        Err(mysql::Error::MySqlError(error)) if error.code == 1062 && error.state == "23000" => {}
        Err(error) => return Err(error.into()),
        Ok(()) => return Err(io::Error::other("Duplicate key accepted").into()),
    }
    tx.rollback()?;
    // Transaction::drop also rolls back if an earlier operation returns an error.
    balance(conn, "Alice 中文🔎", "900.00")?;
    balance(observer, "Bob", "600.00")?;
    println!(
        "PASS Rust commit, rollback, savepoints, cross-connection visibility and error recovery"
    );
    Ok(())
}

fn procedure(conn: &mut Conn) -> Result<()> {
    conn.query_drop(
        "CREATE PROCEDURE account_summary(
        IN p_name VARCHAR(64), OUT p_balance DECIMAL(12,2), INOUT p_calls INT)
        READS SQL DATA BEGIN
        SELECT balance INTO p_balance FROM accounts WHERE name=p_name;
        SET p_calls=p_calls+1;
        SELECT name,balance FROM accounts WHERE name=p_name;
        SELECT COUNT(*) AS account_count FROM accounts; END",
    )?;
    conn.exec_drop("SET @name=?, @balance=NULL, @calls=10", ("Alice 中文🔎",))?;
    // Session variables and CALL must use the same Conn. Drain all results before querying OUT values.
    let mut result = conn.query_iter("CALL account_summary(@name,@balance,@calls)")?;
    let mut count = 0;
    while let Some(mut set) = result.iter() {
        if set.columns().as_ref().is_empty() {
            for row in set {
                row?;
            }
            continue;
        }
        count += 1;
        let row = set
            .next()
            .transpose()?
            .ok_or_else(|| io::Error::other("Empty procedure result"))?;
        if count == 1 {
            let account: (String, String) = mysql::from_row_opt(row)?;
            require(
                account == ("Alice 中文🔎".into(), "900.00".into()),
                "Procedure account",
            )?;
        } else {
            let (accounts,): (u64,) = mysql::from_row_opt(row)?;
            require(count == 2 && accounts == 2, "Procedure count")?;
        }
        require(
            set.next().transpose()?.is_none(),
            "Unexpected procedure row",
        )?;
    }
    drop(result);
    let outputs: Option<(String, u64)> = conn.query_first("SELECT @balance,@calls")?;
    require(
        count == 2 && outputs == Some(("900.00".into(), 11)),
        "Procedure OUT/INOUT",
    )?;
    conn.query_drop("DROP PROCEDURE account_summary")?;
    conn.query_drop("DROP INDEX accounts_balance_idx ON accounts")?;
    conn.query_drop("DROP TABLE accounts")?;
    println!("PASS Rust CALL, IN/OUT/INOUT, multiple result sets and DROP DDL");
    Ok(())
}

fn run() -> Result<()> {
    let mut conn = connect()?;
    conn.ping()?;
    let schema = format!(
        "rust_demo_{}_{}",
        process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    conn.query_drop(format!("CREATE DATABASE {schema}"))?;
    println!("Using isolated schema {schema}");
    let result = (|| -> Result<()> {
        conn.select_db(&schema)?;
        ddl_dml_select(&mut conn)?;
        let mut observer = connect()?;
        observer.select_db(&schema)?;
        transactions(&mut conn, &mut observer)?;
        procedure(&mut conn)
    })();
    // Attempt cleanup on both success and error, without hiding the original error.
    let cleanup = conn.query_drop(format!("DROP DATABASE {schema}"));
    match (result, cleanup) {
        (Ok(()), Ok(())) => println!("PASS Rust cleanup: dropped {schema}"),
        (Err(error), Ok(())) => {
            println!("PASS Rust cleanup: dropped {schema}");
            return Err(error);
        }
        (Ok(()), Err(error)) => return Err(error.into()),
        (Err(error), Err(cleanup)) => {
            return Err(io::Error::other(format!("{error}; cleanup failed: {cleanup}")).into())
        }
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        process::exit(1);
    }
    println!("PASS all Rust examples");
}
