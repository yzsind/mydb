# SQL Reference Example Setup

Examples are independent demonstrations. Use a disposable local instance and schema for DDL/DML examples; the following setup is destructive only within the explicitly selected `statement_examples` schema. Run it again before examples that require the original rows or definitions. Explicit schema names such as `app`, `test`, or replication endpoint/UUID/file paths in individual pages are illustrative; replace them with your selected schema and configured environment.

## Shared Tables

```sql
-- This reset permanently discards only the disposable example schema.
SET SESSION mydb_recyclebin = OFF;
DROP DATABASE IF EXISTS statement_examples;
CREATE DATABASE statement_examples;
SET SESSION mydb_recyclebin = ON;
USE statement_examples;
CREATE TABLE customer (
  id BIGINT PRIMARY KEY, customer_id BIGINT UNIQUE,
  name VARCHAR(80), email VARCHAR(255) NOT NULL UNIQUE,
  disabled BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_login TIMESTAMP NULL
);
CREATE TABLE orders (
  id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL,
  total DECIMAL(20,2) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE sales (
  id BIGINT, sale_year INT, PRIMARY KEY(id, sale_year)
) PARTITION BY RANGE(sale_year) (
  PARTITION p2024 VALUES LESS THAN (2025),
  PARTITION p_future VALUES LESS THAN MAXVALUE
);
CREATE TABLE account (id BIGINT PRIMARY KEY, balance DECIMAL(20,2) NOT NULL);
CREATE TABLE accounts (id BIGINT PRIMARY KEY, tenant_id BIGINT, balance DECIMAL(20,2));
CREATE TABLE counter (id BIGINT PRIMARY KEY, value BIGINT NOT NULL);
CREATE TABLE jobs (id BIGINT PRIMARY KEY, state VARCHAR(24), created_at DATETIME);
CREATE TABLE session_token (id BIGINT PRIMARY KEY, expires_at DATETIME);
CREATE TABLE audit_log (id BIGINT PRIMARY KEY, created_at DATETIME);
CREATE TABLE event (id BIGINT PRIMARY KEY, kind VARCHAR(24), created_at DATETIME, payload JSON);
CREATE TABLE events (id BIGINT PRIMARY KEY, amount DECIMAL(20,2));
CREATE TABLE blobs (id BIGINT PRIMARY KEY, payload LONGBLOB);
CREATE TABLE settings (name VARCHAR(80) PRIMARY KEY, value VARCHAR(255));
CREATE TABLE snapshot (id BIGINT PRIMARY KEY, payload JSON);
CREATE TABLE staging_snapshot LIKE snapshot;
CREATE TABLE archive_order LIKE orders;
INSERT INTO customer(id,customer_id,name,email) VALUES
  (1,1,'Ada','ada@example.invalid'),(2,2,'Lin','lin@example.invalid');
INSERT INTO orders(id,customer_id,total) VALUES (1,1,120.00),(2,1,30.00),(3,2,80.00);
INSERT INTO account VALUES(1,100.00),(2,100.00);
INSERT INTO counter VALUES(1,0);
INSERT INTO jobs VALUES(1,'ready','2026-01-01 00:00:00');
INSERT INTO events VALUES(1,10.00),(2,20.00);
```

The customer examples use both `id` and `customer_id`; they contain the same values in this fixture. This is not a requirement that production schemas duplicate a key. Examples that add a primary key assume a separate table without an existing primary key. File-load examples require files with the specified encoding; server-side paths must pass secure_file_priv and filesystem permissions.

## Complete Stored Cursor Example

In the mysql command-line client:

```sql
DELIMITER //
CREATE PROCEDURE sum_orders(OUT p_total DECIMAL(20,2))
BEGIN
  DECLARE done BOOLEAN DEFAULT FALSE;
  DECLARE v_total DECIMAL(20,2);
  DECLARE c CURSOR FOR SELECT total FROM orders ORDER BY id;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done=TRUE;
  SET p_total=0;
  OPEN c;
  read_loop: LOOP
    FETCH c INTO v_total;
    IF done THEN LEAVE read_loop; END IF;
    SET p_total=p_total+v_total;
  END LOOP read_loop;
  CLOSE c;
END//
DELIMITER ;
CALL sum_orders(@total);
SELECT @total; -- 230.00
DROP PROCEDURE sum_orders;
```

An API sends the whole CREATE PROCEDURE as a single SQL string and omits DELIMITER and `//`. DECLARE/FETCH/IF snippets in the stored-program reference are inner-body examples, not standalone requests. A top-level session SQL cursor is a different extension:

```sql
DECLARE recent_orders CURSOR FOR SELECT id,total FROM orders ORDER BY id;
FETCH 2 FROM recent_orders;
COMMIT;
FETCH NEXT FROM recent_orders; -- The session cursor remains open.
CLOSE recent_orders;
```

## Explicit Transaction Around a Procedure

A procedure call is not automatically an atomic business transaction. The caller can group its writes:

```sql
START TRANSACTION;
CALL transfer(1,2,10.00);
COMMIT;
```

This assumes the transfer procedure from [CREATE PROCEDURE](programmatic/create-procedure.md). On a call error, issue ROLLBACK if the whole transfer must be abandoned. Do not put implicit-commit DDL/administration inside a transaction that is intended to remain rollbackable.

## Administration Examples

Replication, backup, accounts, file export/import, restart and shutdown examples require their own configuration and privileges. Endpoint names, UUIDs, GTID sets and job IDs are placeholders obtained from the corresponding status views. Returning a job or accepting a request does not prove asynchronous completion; poll its state. Examples are never a reason to operate on an existing unrelated local server.
