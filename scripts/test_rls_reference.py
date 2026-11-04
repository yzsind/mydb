#!/usr/bin/env python3
"""Compare randomized RLS results with an independent, trusted row oracle."""
import argparse
import json
from pathlib import Path
import random
import tempfile

from test_native_readiness import Servers, free_port, query
from test_row_security import connect, denied


def run(binary, root, seed):
    rng = random.Random(seed)
    data = [(i, rng.choice([None, 0, 1, 2, 3, 4]), rng.randint(-100, 100),
             rng.choice([None, "a", "B", "z"])) for i in range(1, 2001)]
    servers = Servers(binary, root)
    evidence = {"seed": seed, "rows": len(data), "rounds": []}
    app = None
    try:
        port = free_port()
        _, admin = servers.start("database", port)
        for sql in [
            "CREATE TABLE secured(id INT PRIMARY KEY,tenant_id INT,v INT,label VARCHAR(10) COLLATE utf8mb4_bin,KEY kt(tenant_id,id))",
            "CREATE TABLE membership(tenant_id INT)",
            "INSERT INTO membership VALUES(1),(1),(3),(NULL)",
            "CREATE USER alice IDENTIFIED BY 'secret'",
            "GRANT SELECT,INSERT ON test.secured TO alice",
            "GRANT SELECT ON test.membership TO alice",
            "GRANT RLS_CONTEXT_ADMIN ON *.* TO alice",
        ]:
            query(admin, sql)
        with admin.cursor() as cursor:
            cursor.executemany("INSERT INTO secured VALUES(%s,%s,%s,%s)", data)
        query(admin, "ALTER TABLE secured ENABLE ROW LEVEL SECURITY")
        app = connect(port, "alice")
        query(app, "PREPARE point FROM 'SELECT id FROM secured WHERE id=?'")
        for iteration in range(12):
            if iteration:
                for name in ("tenants", "special", "bounds"):
                    query(admin, f"DROP POLICY {name} ON secured")
            tenants = sorted(rng.sample(range(5), 2))
            cutoff = rng.randint(-80, 80)
            special = rng.randint(0, 4)
            expression = f"tenant_id IN({','.join(map(str, tenants))},NULL)"
            if iteration % 3 == 1:
                expression = "tenant_id=CAST(RLS_CONTEXT('tenant_id') AS UNSIGNED)"
                query(app, f"SET RLS CONTEXT 'tenant_id'='{tenants[0]}'")
                eligible = lambda row: row[1] == tenants[0]
            elif iteration % 3 == 2:
                expression = "EXISTS(SELECT 1 FROM membership m WHERE m.tenant_id=secured.tenant_id)"
                eligible = lambda row: row[1] in (1, 3)
            else:
                eligible = lambda row: row[1] is not None and row[1] in tenants
            query(admin, f"CREATE POLICY tenants ON secured USING({expression})")
            query(admin, f"CREATE POLICY special ON secured USING(tenant_id={special} AND label='B')")
            query(admin, f"CREATE POLICY bounds ON secured AS RESTRICTIVE USING(v>={cutoff})")
            visible = [row for row in data if (eligible(row) or row[1] == special and row[3] == "B") and row[2] >= cutoff]
            expected = tuple((row[0],) for row in visible)
            for sql in ["SELECT id FROM secured ORDER BY id",
                        "SELECT id FROM secured IGNORE INDEX(kt) ORDER BY id",
                        "SELECT /*+ PARALLEL(4) */ id FROM secured ORDER BY id",
                        "SELECT id FROM secured WHERE id>0 ORDER BY id"]:
                assert query(app, sql) == expected, (iteration, sql)
            aggregate = ((len(visible), sum(row[2] for row in visible) if visible else None),)
            assert query(app, "SELECT COUNT(*),SUM(v) FROM secured") == aggregate, iteration
            assert query(app, "SELECT COUNT(*) FROM secured a JOIN secured b ON a.id=b.id") == ((len(visible),),), iteration
            for identifier in rng.sample(range(1, 2001), 8):
                query(app, f"SET @id={identifier}")
                assert query(app, "EXECUTE point USING @id") == tuple(row for row in expected if row[0] == identifier), iteration
            denied(app, "INSERT IGNORE INTO secured VALUES(3000,NULL,-101,NULL)")
            assert query(admin, "SELECT COUNT(*) FROM secured") == ((len(data),),)
            evidence["rounds"].append({"iteration": iteration, "predicate": expression,
                "special": special, "cutoff": cutoff, "visible_rows": len(visible)})
        evidence["passed"] = True
    finally:
        if app:
            app.close()
        servers.close()
        (root / "result.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=20261104)
    args = parser.parse_args()
    base = Path("/tmp/mydb/rls/reference")
    base.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="run-", dir=base))
    result = run(args.binary.resolve(), root, args.seed)
    print(json.dumps({"passed": result["passed"], "rounds": len(result["rounds"]),
                      "artifacts": str(root)}, indent=2))


if __name__ == "__main__":
    main()
