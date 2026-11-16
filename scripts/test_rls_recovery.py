#!/usr/bin/env python3
"""Local RLS crash recovery, native replay, planned promotion and backup checks."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

import pymysql
from test_native_readiness import Servers, catch_up, free_port, query, status
from test_row_security import connect, denied


def run(binary, root):
    servers = Servers(binary, root)
    clients = []
    passed = []
    try:
        master_port, replica_port, master_repl, replica_repl = [free_port() for _ in range(4)]
        master_env = dict(MYDB_ROLE="master", MYDB_REPL_PORT=str(master_repl))
        process, master = servers.start("master", master_port, master_env)
        for sql in [
            "CREATE TABLE secured(id INT PRIMARY KEY,tenant_id INT)",
            "INSERT INTO secured VALUES(1,1),(2,2)",
            "CREATE USER alice IDENTIFIED BY 'secret'",
            "GRANT SELECT,INSERT ON test.secured TO alice",
            "ALTER TABLE secured ENABLE ROW LEVEL SECURITY",
            "CREATE POLICY tenant ON secured USING(tenant_id=1)",
            "CREATE TABLE exchange_part(id INT PRIMARY KEY,tenant_id INT) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)",
            "CREATE TABLE exchange_stage(id INT PRIMARY KEY,tenant_id INT)",
            "INSERT INTO exchange_stage VALUES(1,1),(2,2)",
            "ALTER TABLE exchange_part ENABLE ROW LEVEL SECURITY",
            "ALTER TABLE exchange_stage ENABLE ROW LEVEL SECURITY",
            "CREATE POLICY tenant ON exchange_part USING(tenant_id=1)",
            "CREATE POLICY tenant ON exchange_stage USING(tenant_id=1)",
            "GRANT SELECT,ALTER ON test.exchange_part TO alice",
            "GRANT SELECT,ALTER ON test.exchange_stage TO alice",
            "ALTER TABLE exchange_part EXCHANGE PARTITION p0 WITH TABLE exchange_stage",
        ]:
            query(master, sql)
        # Abruptly stop only the process owned by this test. No checkpoint or
        # orderly shutdown may substitute for WAL recovery here.
        process.kill()
        process.wait(timeout=30)
        _, master = servers.start("master", master_port, master_env)
        alice = connect(master_port, "alice")
        clients.append(alice)
        assert query(alice, "SELECT id FROM secured ORDER BY id") == ((1,),)
        denied(alice, "INSERT INTO secured VALUES(3,2)")
        assert query(master, "SELECT id FROM exchange_part ORDER BY id") == ((1,), (2,))
        assert query(alice, "SELECT id FROM exchange_part ORDER BY id") == ((1,),)
        denied(alice, "ALTER TABLE exchange_part EXCHANGE PARTITION p0 WITH TABLE exchange_stage")
        passed.append("crash_wal_recovery_reenforces_policy")

        replica_env = dict(
            MYDB_ROLE="replica", MYDB_REPL_PORT=str(replica_repl),
            MYDB_REPL_MASTER_HOST="127.0.0.1", MYDB_REPL_MASTER_PORT=str(master_repl))
        replica_process, replica = servers.start("replica", replica_port, replica_env)
        catch_up(master, replica)
        reader = connect(replica_port, "alice")
        clients.append(reader)
        assert query(reader, "SELECT id FROM secured ORDER BY id") == ((1,),)
        assert query(reader, "SELECT id FROM exchange_part ORDER BY id") == ((1,),)
        passed.append("native_base_backup_preserves_policy_and_account")

        query(master, "ALTER TABLE exchange_part EXCHANGE PARTITION p0 WITH TABLE exchange_stage")
        catch_up(master, replica)
        assert query(reader, "SELECT id FROM exchange_part ORDER BY id") == ()
        assert query(reader, "SELECT id FROM exchange_stage ORDER BY id") == ((1,),)
        assert query(replica, "SELECT id FROM exchange_stage ORDER BY id") == ((1,), (2,))
        passed.append("native_wal_exchange_preserves_data_and_both_policies")

        for phase in range(2):
            reader.close()
            replica.close()
            replica_process.terminate()
            replica_process.wait(timeout=30)
            replica_process, replica = servers.start("replica", replica_port, replica_env)
            # Keep both descriptors cold until the exchange has replayed.
            query(master, "ALTER TABLE exchange_part EXCHANGE PARTITION p0 WITH TABLE exchange_stage")
            catch_up(master, replica)
            reader = connect(replica_port, "alice")
            clients.append(reader)
            populated = "exchange_part" if phase == 0 else "exchange_stage"
            empty = "exchange_stage" if phase == 0 else "exchange_part"
            assert query(reader, f"SELECT id FROM {populated} ORDER BY id") == ((1,),)
            assert query(reader, f"SELECT id FROM {empty} ORDER BY id") == ()
            assert query(replica, f"SELECT id FROM {populated} ORDER BY id") == ((1,), (2,))
        passed.append("native_wal_repeated_exchange_after_cold_replica_restart")

        # secured has not been opened since the last replica restart: a real
        # policy-version change must still capture its previous payload.
        query(master, "ALTER POLICY tenant ON secured USING(tenant_id=2)")
        query(master, "INSERT INTO secured VALUES(4,2)")
        catch_up(master, replica)
        assert query(reader, "SELECT id FROM secured ORDER BY id") == ((2,), (4,))
        query(master, "GRANT SUPER ON *.* TO alice")
        catch_up(master, replica)
        assert query(reader, "SELECT COUNT(*) FROM secured") == ((3,),)
        query(master, "REVOKE SUPER ON *.* FROM alice")
        catch_up(master, replica)
        assert query(reader, "SELECT id FROM secured ORDER BY id") == ((2,), (4,))
        passed.append("native_wal_policy_publication_and_super_revocation")

        fence = status(master, "STOP MASTER WRITES WAIT 30")
        token = str(fence["Fence_Token"])
        assert all(c in "0123456789abcdefABCDEF-" for c in token)
        query(replica, f"PROMOTE TO MASTER AT LSN {int(fence['Fence_LSN'])} FENCE_TOKEN='{token}' WAIT 30")
        assert query(reader, "SELECT id FROM secured ORDER BY id") == ((2,), (4,))
        denied(reader, "INSERT INTO secured VALUES(5,1)")
        query(reader, "INSERT INTO secured VALUES(6,2)")
        passed.append("planned_promotion_preserves_read_and_write_enforcement")

        repository = root / "repository"
        query(replica, f"BACKUP DATABASE TO 'file://{repository}'")
        deadline = time.monotonic() + 90
        while True:
            job = status(replica, "SHOW MYDB BACKUP JOBS")
            if job["State"] == "COMPLETED":
                break
            assert job["State"] not in ("FAILED", "CANCELLED"), job
            assert time.monotonic() < deadline, job
            time.sleep(.1)
        bases = query(replica, f"SHOW MYDB BACKUPS FROM 'file://{repository}'")
        assert bases
        backup_port = free_port()
        env = {k: v for k, v in os.environ.items() if not k.startswith("MYDB_")}
        env.update(MYDB_MEMORY_TARGET="512M", TMPDIR="/tmp/mydb")
        with (root / "backup-query.log").open("ab") as output:
            backup = subprocess.Popen([str(binary), "--backup-repository", str(repository),
                "--base", str(job["Base_ID"]), "--runtime-dir", str(root / "backup-runtime"),
                "--host", "127.0.0.1", "--port", str(backup_port)], env=env,
                stdout=output, stderr=subprocess.STDOUT)
        servers.processes.append(backup)
        deadline = time.monotonic() + 90
        while True:
            assert backup.poll() is None, "backup query startup failed"
            try:
                snapshot = connect(backup_port, "alice")
                clients.append(snapshot)
                break
            except pymysql.Error:
                assert time.monotonic() < deadline, "backup query startup timeout"
                time.sleep(.1)
        assert query(snapshot, "SELECT id FROM secured ORDER BY id") == ((2,), (4,), (6,))
        assert query(snapshot, "SELECT id FROM exchange_stage ORDER BY id") == ((1,),)
        passed.append("sealed_backup_queries_enforce_persisted_policy")
    finally:
        for connection in clients:
            try:
                connection.close()
            except pymysql.Error:
                pass
        servers.close()
        (root / "result.json").write_text(json.dumps({"passed": passed}, indent=2) + "\n")
    return passed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    args = parser.parse_args()
    base = Path("/tmp/mydb/rls/recovery")
    base.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="run-", dir=base))
    passed = run(args.binary.resolve(), root)
    print(json.dumps({"passed": passed, "artifacts": str(root)}, indent=2))


if __name__ == "__main__":
    main()
