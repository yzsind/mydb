// Self-checking database/sql examples using dedicated connections for session state.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
)

func env(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return fallback
}

func require(ok bool, message string) error {
	if !ok {
		return errors.New(message)
	}
	return nil
}

type querier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func balance(ctx context.Context, q querier, name, expected string) error {
	var value string
	if err := q.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE name=?", name).Scan(&value); err != nil {
		return err
	}
	return require(value == expected, "unexpected balance for "+name+": "+value)
}

func affected(result sql.Result, expected int64) error {
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	return require(count == expected, fmt.Sprintf("affected rows: got %d, want %d", count, expected))
}

func ddlDmlSelect(ctx context.Context, conn *sql.Conn) error {
	var one int
	if err := conn.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		return err
	}
	if err := require(one == 1, "SELECT 1"); err != nil {
		return err
	}
	for _, query := range []string{
		`CREATE TABLE accounts(id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64) NOT NULL UNIQUE,
		balance DECIMAL(12,2) NOT NULL, note VARCHAR(128), payload VARBINARY(16), created_at DATETIME(6))`,
		"ALTER TABLE accounts ADD COLUMN active BOOLEAN NOT NULL DEFAULT TRUE",
		"CREATE INDEX accounts_balance_idx ON accounts(balance)",
	} {
		if _, err := conn.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	insert, err := conn.PrepareContext(ctx,
		"INSERT INTO accounts(name,balance,note,payload,created_at) VALUES(?,?,?,?,?)")
	if err != nil {
		return err
	}
	defer insert.Close()
	created := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
	payload := []byte{0, 255, 7}
	result, err := insert.ExecContext(ctx, "Alice 中文🔎", "1000.00", nil, payload, created)
	if err != nil {
		return err
	}
	if err := affected(result, 1); err != nil {
		return err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	if err := require(id > 0, "generated key"); err != nil {
		return err
	}
	// database/sql has no addBatch API: reuse one prepared statement in a transaction.
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	batch, err := tx.PrepareContext(ctx, "INSERT INTO accounts(name,balance) VALUES(?,?)")
	if err != nil {
		return err
	}
	defer batch.Close()
	for _, row := range [][2]string{{"Bob", "500.00"}, {"Carol", "250.00"}} {
		result, err := batch.ExecContext(ctx, row[0], row[1])
		if err != nil {
			return err
		}
		if err := affected(result, 1); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	result, err = conn.ExecContext(ctx, "UPDATE accounts SET balance=balance+? WHERE name=?", "10.00", "Carol")
	if err != nil {
		return err
	}
	if err := affected(result, 1); err != nil {
		return err
	}
	if err := balance(ctx, conn, "Carol", "260.00"); err != nil {
		return err
	}
	result, err = conn.ExecContext(ctx, "DELETE FROM accounts WHERE name=?", "Carol")
	if err != nil {
		return err
	}
	if err := affected(result, 1); err != nil {
		return err
	}
	var gotID int64
	var name, amount string
	var note sql.NullString
	var gotPayload []byte
	var gotCreated time.Time
	var active bool
	err = conn.QueryRowContext(ctx,
		"SELECT id,name,balance,note,payload,created_at,active FROM accounts WHERE id=?", id).
		Scan(&gotID, &name, &amount, &note, &gotPayload, &gotCreated, &active)
	if err != nil {
		return err
	}
	if err := require(gotID == id && name == "Alice 中文🔎" && amount == "1000.00" && !note.Valid &&
		string(gotPayload) == string(payload) && gotCreated.Equal(created) && active, "typed SELECT round trip"); err != nil {
		return err
	}
	var count int
	var total string
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*),SUM(balance) FROM accounts").Scan(&count, &total); err != nil {
		return err
	}
	if err := require(count == 2 && total == "1500.00", "aggregate SELECT"); err != nil {
		return err
	}
	fmt.Println("PASS Go connection, DDL, prepared DML, batch, generated key and typed SELECT")
	return nil
}

func transactions(ctx context.Context, conn, observer *sql.Conn) error {
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, query := range []string{
		"UPDATE accounts SET balance=balance-100 WHERE name='Alice 中文🔎'",
		"UPDATE accounts SET balance=balance+100 WHERE name='Bob'",
	} {
		result, err := tx.ExecContext(ctx, query)
		if err != nil {
			return err
		}
		if err := affected(result, 1); err != nil {
			return err
		}
	}
	if err := balance(ctx, tx, "Alice 中文🔎", "900.00"); err != nil {
		return err
	}
	if err := balance(ctx, observer, "Alice 中文🔎", "1000.00"); err != nil {
		return err
	}
	for _, query := range []string{"SAVEPOINT kept", "UPDATE accounts SET balance=0",
		"ROLLBACK TO SAVEPOINT kept", "RELEASE SAVEPOINT kept"} {
		if _, err := tx.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if err := balance(ctx, observer, "Alice 中文🔎", "900.00"); err != nil {
		return err
	}
	if err := balance(ctx, observer, "Bob", "600.00"); err != nil {
		return err
	}
	tx, err = conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "DELETE FROM accounts"); err != nil {
		return err
	}
	if err := tx.Rollback(); err != nil {
		return err
	}
	if err := balance(ctx, conn, "Alice 中文🔎", "900.00"); err != nil {
		return err
	}
	tx, err = conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "UPDATE accounts SET balance=balance+10"); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO accounts(name,balance) VALUES(?,?)", "Bob", "1.00")
	var duplicate *mysql.MySQLError
	if !errors.As(err, &duplicate) || duplicate.Number != 1062 || string(duplicate.SQLState[:]) != "23000" {
		return fmt.Errorf("expected duplicate-key error 1062/23000, got %v", err)
	}
	if err := tx.Rollback(); err != nil {
		return err
	}
	if err := balance(ctx, conn, "Alice 中文🔎", "900.00"); err != nil {
		return err
	}
	if err := balance(ctx, conn, "Bob", "600.00"); err != nil {
		return err
	}
	fmt.Println("PASS Go commit, rollback, savepoints, cross-connection visibility and error recovery")
	return nil
}

func procedure(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx, `CREATE PROCEDURE account_summary(
		IN p_name VARCHAR(64), OUT p_balance DECIMAL(12,2), INOUT p_calls INT)
		READS SQL DATA BEGIN
		SELECT balance INTO p_balance FROM accounts WHERE name=p_name;
		SET p_calls=p_calls+1;
		SELECT name,balance FROM accounts WHERE name=p_name;
		SELECT COUNT(*) AS account_count FROM accounts; END`)
	if err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "SET @name=?, @balance=NULL, @calls=10", "Alice 中文🔎"); err != nil {
		return err
	}
	// This driver negotiates text multi-results, not prepared CALL multi-results.
	// Bind the input separately and keep CALL/output variables on the same sql.Conn.
	rows, err := conn.QueryContext(ctx, "CALL account_summary(@name,@balance,@calls)")
	if err != nil {
		return err
	}
	defer rows.Close()
	sets := 0
	for {
		columns, err := rows.Columns()
		if err != nil {
			return err
		}
		if len(columns) > 0 {
			sets++
			if !rows.Next() {
				if err := rows.Err(); err != nil {
					return err
				}
				return errors.New("empty procedure result")
			}
			if sets == 1 {
				var name, amount string
				if err := rows.Scan(&name, &amount); err != nil {
					return err
				}
				if err := require(name == "Alice 中文🔎" && amount == "900.00", "procedure account"); err != nil {
					return err
				}
			} else {
				var count int
				if err := rows.Scan(&count); err != nil {
					return err
				}
				if err := require(sets == 2 && count == 2, "procedure count"); err != nil {
					return err
				}
			}
			if rows.Next() {
				return errors.New("unexpected procedure row")
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if !rows.NextResultSet() {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	var amount string
	var calls int
	if err := conn.QueryRowContext(ctx, "SELECT @balance,@calls").Scan(&amount, &calls); err != nil {
		return err
	}
	if err := require(sets == 2 && amount == "900.00" && calls == 11, "procedure OUT/INOUT"); err != nil {
		return err
	}
	for _, query := range []string{"DROP PROCEDURE account_summary", "DROP INDEX accounts_balance_idx ON accounts", "DROP TABLE accounts"} {
		if _, err := conn.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	fmt.Println("PASS Go CALL, IN/OUT/INOUT, multiple result sets and DROP DDL")
	return nil
}

func run() (err error) {
	config := mysql.NewConfig()
	config.User = env("MYDB_USER", "root")
	config.Passwd = env("MYDB_PASSWORD", "root")
	config.Net = "tcp"
	config.Addr = net.JoinHostPort(env("MYDB_HOST", "127.0.0.1"), env("MYDB_PORT", "8738"))
	config.DBName = env("MYDB_DB", "test")
	config.ParseTime = true
	config.Timeout, config.ReadTimeout, config.WriteTimeout = 5*time.Second, 15*time.Second, 15*time.Second
	config.Params = map[string]string{"charset": "utf8mb4"}
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.PingContext(ctx); err != nil {
		return err
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	schema := fmt.Sprintf("go_demo_%x", token)
	if _, err := conn.ExecContext(ctx, "CREATE DATABASE "+schema); err != nil {
		return err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, cleanupErr := conn.ExecContext(cleanupCtx, "DROP DATABASE "+schema)
		err = errors.Join(err, cleanupErr)
		if cleanupErr == nil {
			fmt.Println("PASS Go cleanup: dropped", schema)
		}
	}()
	fmt.Println("Using isolated schema", schema)
	if _, err := conn.ExecContext(ctx, "USE "+schema); err != nil {
		return err
	}
	if err := ddlDmlSelect(ctx, conn); err != nil {
		return err
	}
	observer, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer observer.Close()
	for _, query := range []string{"USE " + schema, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"} {
		if _, err := observer.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	if err := transactions(ctx, conn, observer); err != nil {
		return err
	}
	return procedure(ctx, conn)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("PASS all Go examples")
}
