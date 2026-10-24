"""Self-checking PyMySQL examples against an isolated disposable schema."""

import os
import uuid
from datetime import datetime
from decimal import Decimal

import pymysql


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def connect():
    return pymysql.connect(
        host=os.getenv("MYDB_HOST", "127.0.0.1"),
        port=int(os.getenv("MYDB_PORT", "8738")),
        user=os.getenv("MYDB_USER", "root"),
        password=os.getenv("MYDB_PASSWORD", "root"),
        database=os.getenv("MYDB_DB", "test"),
        charset="utf8mb4",
        autocommit=True,
        connect_timeout=5,
        read_timeout=15,
        write_timeout=15,
        ssl_disabled=True,
    )


def balance(connection, name, expected):
    with connection.cursor() as cursor:
        cursor.execute("SELECT balance FROM accounts WHERE name=%s", (name,))
        require(cursor.fetchall() == ((Decimal(expected),),), "Unexpected balance for " + name)


def demo_ddl_dml_select(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        require(cursor.fetchall() == ((1,),), "SELECT 1")
        cursor.execute("""CREATE TABLE accounts (
            id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64) NOT NULL UNIQUE,
            balance DECIMAL(12,2) NOT NULL, note VARCHAR(128), payload VARBINARY(16),
            created_at DATETIME(6))""")
        cursor.execute("ALTER TABLE accounts ADD COLUMN active BOOLEAN NOT NULL DEFAULT TRUE")
        cursor.execute("CREATE INDEX accounts_balance_idx ON accounts(balance)")
        created = datetime(2026, 1, 2, 3, 4, 5, 123456)
        payload = b"\x00\xff\x07"
        cursor.execute(
            "INSERT INTO accounts(name,balance,note,payload,created_at) VALUES(%s,%s,%s,%s,%s)",
            ("Alice 中文🔎", Decimal("1000.00"), None, payload, created),
        )
        require(cursor.rowcount == 1 and cursor.lastrowid > 0, "INSERT / generated key")
        alice_id = cursor.lastrowid
        cursor.executemany(
            "INSERT INTO accounts(name,balance) VALUES(%s,%s)",
            [("Bob", Decimal("500.00")), ("Carol", Decimal("250.00"))],
        )
        require(cursor.rowcount == 2, "Batch INSERT")
        cursor.execute("UPDATE accounts SET balance=balance+%s WHERE name=%s", (Decimal("10.00"), "Carol"))
        require(cursor.rowcount == 1, "UPDATE affected rows")
        balance(connection, "Carol", "260.00")
        cursor.execute("DELETE FROM accounts WHERE name=%s", ("Carol",))
        require(cursor.rowcount == 1, "DELETE affected rows")
        cursor.execute("SELECT id,name,balance,note,payload,created_at,active FROM accounts WHERE id=%s", (alice_id,))
        require(len(cursor.description) == 7, "SELECT metadata")
        require(cursor.fetchall() == ((alice_id, "Alice 中文🔎", Decimal("1000.00"), None, payload, created, 1),),
                "Typed SELECT round trip")
        cursor.execute("SELECT COUNT(*),SUM(balance) FROM accounts")
        require(cursor.fetchall() == ((2, Decimal("1500.00")),), "Aggregate SELECT")
    print("PASS Python connection, DDL, parameterized DML, batch, generated key and typed SELECT")


def demo_transactions(connection, observer):
    for session in (connection, observer):
        with session.cursor() as cursor:
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    connection.begin()
    try:
        with connection.cursor() as cursor:
            cursor.execute("UPDATE accounts SET balance=balance-%s WHERE name=%s", (Decimal("100.00"), "Alice 中文🔎"))
            require(cursor.rowcount == 1, "Debit affected rows")
            cursor.execute("UPDATE accounts SET balance=balance+100 WHERE name='Bob'")
            require(cursor.rowcount == 1, "Credit affected rows")
            balance(connection, "Alice 中文🔎", "900.00")
            balance(observer, "Alice 中文🔎", "1000.00")
            cursor.execute("SAVEPOINT kept")
            cursor.execute("UPDATE accounts SET balance=0")
            cursor.execute("ROLLBACK TO SAVEPOINT kept")
            cursor.execute("RELEASE SAVEPOINT kept")
        connection.commit()
        balance(observer, "Alice 中文🔎", "900.00")
        balance(observer, "Bob", "600.00")

        connection.begin()
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM accounts")
            require(cursor.rowcount == 2, "Transactional DELETE")
        connection.rollback()
        balance(connection, "Alice 中文🔎", "900.00")
        balance(connection, "Bob", "600.00")

        connection.begin()
        with connection.cursor() as cursor:
            cursor.execute("UPDATE accounts SET balance=balance+10")
            try:
                cursor.execute("INSERT INTO accounts(name,balance) VALUES(%s,%s)", ("Bob", Decimal("1.00")))
            except pymysql.err.IntegrityError as error:
                require(error.args[0] == 1062, "Expected duplicate-key error 1062")
                connection.rollback()
            else:
                raise RuntimeError("Duplicate key accepted")
        balance(connection, "Alice 中文🔎", "900.00")
        balance(observer, "Bob", "600.00")
    finally:
        connection.rollback()
    print("PASS Python commit, rollback, savepoints, cross-connection visibility and error recovery")


def demo_procedure(connection):
    with connection.cursor() as cursor:
        # Send the complete body as one query; DELIMITER belongs to the mysql CLI.
        cursor.execute("""CREATE PROCEDURE account_summary(
            IN p_name VARCHAR(64), OUT p_balance DECIMAL(12,2), INOUT p_calls INT)
            READS SQL DATA BEGIN
              SELECT balance INTO p_balance FROM accounts WHERE name=p_name;
              SET p_calls=p_calls+1;
              SELECT name,balance FROM accounts WHERE name=p_name;
              SELECT COUNT(*) AS account_count FROM accounts;
            END""")
        cursor.callproc("account_summary", ("Alice 中文🔎", None, 10))
        results = []
        while True:
            if cursor.description is not None:
                results.append(cursor.fetchall())
            if not cursor.nextset():
                break
        require(results == [(("Alice 中文🔎", Decimal("900.00")),), ((2,),)], "Procedure result sets")
        # PyMySQL exposes OUT/INOUT values through these session variables, not callproc's return value.
        cursor.execute("SELECT @_account_summary_1, @_account_summary_2")
        require(cursor.fetchall() == ((Decimal("900.00"), 11),), "Procedure OUT/INOUT")
        cursor.execute("DROP PROCEDURE account_summary")
        cursor.execute("DROP INDEX accounts_balance_idx ON accounts")
        cursor.execute("DROP TABLE accounts")
    print("PASS Python callproc, IN/OUT/INOUT, multiple result sets and DROP DDL")


def main():
    schema = "python_demo_" + uuid.uuid4().hex
    with connect() as connection:
        connection.ping(reconnect=False)
        with connection.cursor() as cursor:
            cursor.execute("CREATE DATABASE " + schema)
        print("Using isolated schema " + schema)
        try:
            connection.select_db(schema)
            demo_ddl_dml_select(connection)
            with connect() as observer:
                observer.select_db(schema)
                demo_transactions(connection, observer)
            demo_procedure(connection)
        finally:
            connection.rollback()
            with connection.cursor() as cursor:
                cursor.execute("DROP DATABASE " + schema)
            print("PASS Python cleanup: dropped " + schema)
    print("PASS all Python examples")


if __name__ == "__main__":
    main()
