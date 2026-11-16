#!/usr/bin/env python3
"""Attach mydb as a replica of a running MySQL server (MySQL -> mydb, ROW/FULL).

End-to-end setup: preflight the source, provision a replication account,
(optionally) snapshot existing data, launch mydb, issue
`CHANGE REPLICATION SOURCE TO` + `START REPLICA`, then smoke-test the link.

Example:
    python3 scripts/setup_mysql_to_mydb_replica.py \
        --source-host 127.0.0.1 --source-port 3306 \
        --mydb target/release/mydb --mydb-port 13390

Requires PyMySQL. Work dir must live under /tmp/mydb per repo convention.
"""
import argparse
import os
import secrets
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pymysql

SMOKE_DB = "mydb_repl_smoke"


def die(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def wait_for(check, timeout, what):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            value = check()
            if value:
                return value
        except Exception as exc:  # noqa: BLE001 - report the last error on timeout
            last = exc
        time.sleep(0.2)
    die(f"timed out waiting for {what} ({last})")


def sql_quote(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def query(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        if cur.description is None:
            return []
        return list(cur.fetchall())


def query_dict(conn, sql):
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(sql)
        return list(cur.fetchall())


def source_status(conn):
    """SHOW MASTER STATUS (<=8.0) / SHOW BINARY LOG STATUS (8.4+) -> (file, pos)."""
    for sql in ("SHOW MASTER STATUS", "SHOW BINARY LOG STATUS"):
        try:
            rows = query_dict(conn, sql)
        except pymysql.err.Error:
            continue
        if rows:
            row = rows[0]
            keys = list(row.keys())
            return str(row.get("File") or row[keys[0]]), int(row.get("Position") or row[keys[1]])
    die("cannot read binlog coordinates from source (SHOW MASTER STATUS / SHOW BINARY LOG STATUS)")


def preflight_source(args, conn):
    vars_ = query_dict(conn, "SELECT @@log_bin AS log_bin, @@binlog_format AS fmt, "
                             "@@binlog_row_image AS img, @@binlog_row_value_options AS vo, "
                             "@@gtid_mode AS gtid, @@server_id AS sid")[0]
    problems = []
    if str(vars_["log_bin"]) not in ("1", "ON"):
        problems.append("log_bin is OFF: the source must be restarted with --log-bin (dynamic change impossible)")
    if vars_["fmt"] != "ROW":
        problems.append(f"binlog_format={vars_['fmt']}: run SET GLOBAL binlog_format=ROW on the source")
    if vars_["img"] != "FULL":
        problems.append(f"binlog_row_image={vars_['img']}: run SET GLOBAL binlog_row_image=FULL on the source")
    if str(vars_["vo"] or "") != "":
        problems.append(f"binlog_row_value_options={vars_['vo']}: run SET GLOBAL binlog_row_value_options='' on the source")
    if problems:
        die("source preflight failed:\n  - " + "\n  - ".join(problems))
    if vars_["sid"] == args.replica_server_id:
        die(f"source server_id == replica server_id == {args.replica_server_id}; pass --replica-server-id")
    print(f"[1/6] source preflight OK (format=ROW image=FULL gtid_mode={vars_['gtid']} "
          f"server_id={vars_['sid']})")
    return vars_


def provision_replication_user(args, conn):
    repl_user = args.repl_user
    repl_password = args.repl_password
    if args.admin_user:
        if not repl_password:
            repl_password = secrets.token_urlsafe(12)
        # IF NOT EXISTS keeps a stale password on re-runs; the follow-up ALTER
        # USER pins the account to the password used by this channel.
        query(conn, f"CREATE USER IF NOT EXISTS '{repl_user}'@'%' IDENTIFIED BY "
                    f"{sql_quote(repl_password)}")
        query(conn, f"ALTER USER '{repl_user}'@'%' IDENTIFIED BY {sql_quote(repl_password)}")
        # 8.0 spelling; REPLICATION REPLICA (8.4+) is an alias of the same grant.
        query(conn, f"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{repl_user}'@'%'")
        conn.commit()
        print(f"[2/6] replication account '{repl_user}'@'%' ready (REPLICATION SLAVE, REPLICATION CLIENT)")
    elif not repl_password:
        die("without --admin-user you must pass --repl-user and --repl-password of an existing "
            "account holding REPLICATION SLAVE")
    # Prove the account can actually reach the binlog stream metadata.
    probe = pymysql.connect(host=args.source_host, port=args.source_port,
                            user=repl_user, password=repl_password,
                            charset="utf8mb4")
    try:
        source_status(probe)
    finally:
        probe.close()
    return repl_user, repl_password


def run_snapshot(args, source, log_file, log_pos):
    """Dump source databases and restore into mydb; return dump-consistent coordinates."""
    if not args.databases:
        die("--snapshot requires --databases (comma list of databases to copy)")
    dump_path = args.work_dir / "snapshot.sql"
    dump_cmd = [
        "mysqldump", f"-h{args.source_host}", f"-P{args.source_port}",
        f"-u{source['user']}",
        f"--password={source['password'] or ''}",
        "--single-transaction", "--skip-lock-tables", "--skip-triggers",
        "--set-gtid-purged=OFF", "--no-tablespaces", "--column-statistics=0",
        "--databases", *args.databases.split(","),
    ]
    # 8.0 spells it --master-data; 8.4+ --source-data. Both embed the
    # coordinates matching the --single-transaction snapshot.
    for coord_flag in ("--source-data=2", "--master-data=2"):
        cmd = dump_cmd + [coord_flag]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        except FileNotFoundError:
            die("mysqldump not found on PATH; install it or use incremental-only mode")
        if result.returncode == 0:
            dump_path.write_text(result.stdout)
            break
        if "unknown option" not in result.stderr and "unknown variable" not in result.stderr:
            sys.stderr.write(result.stderr)
            die(f"mysqldump failed (rc={result.returncode})")
    else:
        die("mysqldump rejected both --source-data and --master-data")
    # Coordinates embedded as a comment by --source-data=2.
    text = dump_path.read_text()
    marker = None
    for line in text.splitlines():
        if line.startswith("--") and "CHANGE REPLICATION SOURCE TO" in line:
            marker = line
    if marker is None:
        die("could not find snapshot coordinates in mysqldump output")
    import re
    m = re.search(r"SOURCE_LOG_FILE='([^']+)',\s*SOURCE_LOG_POS=(\d+)", marker)
    if not m:
        die(f"unrecognized coordinate comment: {marker}")
    log_file, log_pos = m.group(1), int(m.group(2))
    restore_filtered_dump(args, dump_path)
    return log_file, log_pos


def restore_filtered_dump(args, dump_path):
    """Stream the dump into mydb, dropping session/bootstrap statements mydb rejects.

    mysqldump emits `/*!80000 SET SESSION information_schema_stats_expiry=0 */`
    and friends; mydb lacks several of those sysvars (docs/sysvar-config-bugs
    audit V-01) and 1193-aborts mid-restore, so header/footer SET blocks,
    LOCK/UNLOCK TABLES guards and version comments are stripped. DDL and
    INSERT payloads are kept verbatim, one statement per execution.
    """
    stmts = []
    buf = []
    for line in dump_path.read_text(errors="replace").splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith("/*!") or stripped.upper().startswith(("SET ", "LOCK TABLES", "UNLOCK TABLES")):
            continue
        if stripped.startswith("--") or not stripped:
            if not buf:
                continue
        buf.append(line)
        if stripped.endswith(";"):
            stmts.append("".join(buf))
            buf = []
    mydb = connect_mydb(args)
    try:
        total = len(stmts)
        for i, stmt in enumerate(stmts, 1):
            with mydb.cursor() as cur:
                cur.execute(stmt)
        mydb.commit()
        print(f"[3/6] snapshot restored: {total} statements into mydb")
    finally:
        mydb.close()


def launch_mydb(args):
    data_dir = args.work_dir / "data"
    log_path = args.work_dir / "mydb.log"
    env = dict(os.environ)
    env["MYDB_DATA_DIR"] = str(data_dir)
    if args.mydb_root_password:
        env["MYDB_ROOT_PASSWORD"] = args.mydb_root_password
    log = log_path.open("ab")
    proc = subprocess.Popen(
        [str(args.mydb.resolve()), "--host", "127.0.0.1", "--port", str(args.mydb_port)],
        env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    try:
        wait_for(lambda: ping_mydb(args), 60, "mydb to accept connections")
    except BaseException:
        proc.send_signal(signal.SIGTERM)
        raise
    print(f"[4/6] mydb up on 127.0.0.1:{args.mydb_port} (pid {proc.pid}, data {data_dir}, log {log_path})")
    return proc


def connect_mydb(args):
    return pymysql.connect(host="127.0.0.1", port=args.mydb_port, user="root",
                           password=args.mydb_root_password or "root", charset="utf8mb4",
                           autocommit=True)


def ping_mydb(args):
    conn = connect_mydb(args)
    conn.ping(reconnect=False)
    conn.close()
    return True


def replica_status(args):
    conn = connect_mydb(args)
    try:
        rows = query_dict(conn, "SHOW REPLICA STATUS")
        return rows[0] if rows else None
    finally:
        conn.close()


def configure_replication(args, repl_user, repl_password, log_file, log_pos, gtid_mode):
    conn = connect_mydb(args)
    clauses = [
        f"SOURCE_HOST={sql_quote(args.source_host)}",
        f"SOURCE_PORT={args.source_port}",
        f"SOURCE_USER={sql_quote(repl_user)}",
        f"SOURCE_PASSWORD={sql_quote(repl_password)}",
        f"SOURCE_SERVER_ID={args.replica_server_id}",
        "SOURCE_SSL=0",
    ]
    if args.auto_position:
        if gtid_mode != "ON":
            die(f"--auto-position needs gtid_mode=ON on the source (got {gtid_mode})")
        clauses.append("SOURCE_AUTO_POSITION=1")
    else:
        clauses += [f"SOURCE_LOG_FILE={sql_quote(log_file)}", f"SOURCE_LOG_POS={log_pos}"]
    if args.databases:
        # Exact-DB allow-list; syntax `db1,db2` mirrors replicate-do-db.
        clauses.append("REPLICATE_DO_DB=" + ",".join(sql_quote(d) for d in args.databases.split(",")))
    sql = "CHANGE REPLICATION SOURCE TO " + ", ".join(clauses)
    try:
        query(conn, sql)
    except pymysql.err.Error as exc:
        die(f"CHANGE REPLICATION SOURCE TO failed: {exc}\n  {sql}")
    finally:
        conn.close()
    print(f"[5/6] channel configured ({'GTID auto-position' if args.auto_position else f'{log_file}:{log_pos}'})")


def start_and_verify(args):
    conn = connect_mydb(args)
    query(conn, "START REPLICA")
    conn.close()
    def healthy():
        st = replica_status(args)
        return st if st and st["Replica_IO_Running"] == "Yes" and st["Replica_SQL_Running"] == "Yes" else None
    try:
        wait_for(healthy, 60, "replica IO+SQL threads to reach Yes")
    except SystemExit:
        st = replica_status(args) or {}
        die("replica threads did not start: "
            f"Last_IO_Error={st.get('Last_IO_Error')!r} Last_SQL_Error={st.get('Last_SQL_Error')!r}")
    print("[6/6] replica is running (Replica_IO_Running=Yes, Replica_SQL_Running=Yes)")


def smoke_test(args, source):
    target_db = args.databases.split(",")[0].strip() if args.databases else SMOKE_DB
    table = "mydb_repl_smoke"
    # Bootstrap without a default schema (CREATE DATABASE), then operate with
    # the target db as the session default: MySQL's replicate-do-db filters
    # statement events by the *session* schema, so fully-qualified DDL from a
    # schema-less session would be filtered out — the smoke write must look
    # like ordinary client traffic.
    boot = pymysql.connect(host=args.source_host, port=args.source_port,
                           user=source["user"], password=source["password"], charset="utf8mb4",
                           autocommit=True)
    try:
        if not args.databases:
            query(boot, f"CREATE DATABASE IF NOT EXISTS `{target_db}`")
    finally:
        boot.close()
    src = pymysql.connect(host=args.source_host, port=args.source_port,
                          user=source["user"], password=source["password"], charset="utf8mb4",
                          database=target_db, autocommit=True)
    try:
        query(src, f"DROP TABLE IF EXISTS `{table}`")
        query(src, f"CREATE TABLE `{table}` (id INT PRIMARY KEY, note VARCHAR(64))")
        query(src, f"INSERT INTO `{table}` VALUES (1, 'replicated-by-mydb')")
    finally:
        src.close()
    mydb = connect_mydb(args)
    try:
        def arrived():
            try:
                rows = query(mydb, f"SELECT note FROM `{target_db}`.`{table}` WHERE id = 1")
                return rows and rows[0][0] == "replicated-by-mydb"
            except pymysql.err.Error:
                return False
        wait_for(arrived, 60, "smoke row to replicate into mydb")
        st = replica_status(args)
        print(f"smoke OK: row replicated through {st['Relay_Source_Log_File']}:{st['Exec_Source_Log_Pos']} "
              f"(Seconds_Behind_Source={st['Seconds_Behind_Source']})")
        # Leave no residue on the source.
        src = pymysql.connect(host=args.source_host, port=args.source_port,
                              user=source["user"], password=source["password"], charset="utf8mb4",
                              database=target_db, autocommit=True)
        try:
            query(src, f"DROP TABLE IF EXISTS `{table}`")
            if not args.databases:
                query(src, f"DROP DATABASE IF EXISTS `{target_db}`")
        finally:
            src.close()
    finally:
        mydb.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source-host", default="127.0.0.1")
    ap.add_argument("--source-port", type=int, default=3306)
    ap.add_argument("--admin-user", default=None,
                    help="source admin account used to create the replication user "
                         "(omit to reuse an existing account via --repl-user/--repl-password)")
    ap.add_argument("--admin-password", default="")
    ap.add_argument("--repl-user", default="mydb_repl")
    ap.add_argument("--repl-password", default=None)
    ap.add_argument("--mydb", type=Path, default=Path("target/release/mydb"))
    ap.add_argument("--mydb-port", type=int, default=13390)
    ap.add_argument("--mydb-root-password", default=None,
                    help="initial root password for the fresh mydb instance (default 'root')")
    ap.add_argument("--mydb-attach", action="store_true",
                    help="do not launch mydb; configure replication on an already-running "
                         "instance at --mydb-port")
    ap.add_argument("--replica-server-id", type=int, default=9001)
    ap.add_argument("--auto-position", action="store_true",
                    help="use GTID auto-position (source must have gtid_mode=ON)")
    ap.add_argument("--databases", default=None,
                    help="comma list replicated via REPLICATE_DO_DB filter (also scopes --snapshot)")
    ap.add_argument("--snapshot", action="store_true",
                    help="copy existing data with mysqldump before attaching (needs --databases)")
    ap.add_argument("--no-smoke", action="store_true", help="skip the end-to-end smoke insert")
    ap.add_argument("--work-dir", type=Path, default=Path("/tmp/mydb/mysql-replica-setup"))
    args = ap.parse_args()

    if not args.work_dir.resolve().is_relative_to(Path("/tmp/mydb").resolve()):
        ap.error("--work-dir must live under /tmp/mydb")
    args.work_dir.mkdir(parents=True, exist_ok=True)

    source = {"user": args.admin_user or args.repl_user,
              "password": args.admin_password or args.repl_password or ""}
    conn = pymysql.connect(host=args.source_host, port=args.source_port,
                           user=source["user"], password=source["password"],
                           charset="utf8mb4", autocommit=True)
    source_vars = preflight_source(args, conn)
    repl_user, repl_password = provision_replication_user(args, conn)
    log_file, log_pos = source_status(conn)
    conn.close()

    proc = None
    if not args.mydb_attach:
        if not args.mydb.exists():
            die(f"mydb binary not found: {args.mydb} (build with cargo build --release)")
        proc = launch_mydb(args)
    elif not ping_mydb(args):
        die(f"--mydb-attach set but nothing reachable on 127.0.0.1:{args.mydb_port}")

    if args.snapshot:
        log_file, log_pos = run_snapshot(args, source, log_file, log_pos)

    configure_replication(args, repl_user, repl_password, log_file, log_pos, source_vars["gtid"])
    start_and_verify(args)
    if not args.no_smoke:
        smoke_test(args, source)

    st = replica_status(args)
    print("\n=== replica summary ===")
    print(f"source          : {args.source_host}:{args.source_port} (server_id={st['Source_Server_Id']})")
    print(f"channel         : {st['Channel_Name']}  auto_position={st['Auto_Position']}")
    print(f"read through    : {st['Source_Log_File']}:{st['Read_Source_Log_Pos']}")
    print(f"applied through : {st['Relay_Source_Log_File']}:{st['Exec_Source_Log_Pos']}")
    print(f"mydb            : 127.0.0.1:{args.mydb_port} -uroot "
          f"{'-p' + (args.mydb_root_password or 'root') if (args.mydb_root_password or 'root') != 'root' else '-proot'}")
    if proc is not None:
        print(f"mydb pid        : {proc.pid} (logs: {args.work_dir / 'mydb.log'}) — stays running after this script")
    print("manage with     : SHOW REPLICA STATUS\\G | STOP REPLICA | START REPLICA | RESET REPLICA ALL")
    print("read-only tip   : SET GLOBAL mydb_replica_read_only=ON to guard against accidental writes")


if __name__ == "__main__":
    main()
