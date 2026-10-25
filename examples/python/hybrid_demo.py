"""Executable checks for authorization, RRF, reranking and versioned evidence."""
import json
import os
import uuid
import math
import pymysql
from hybrid import EvidenceUnavailable, HybridRetriever, Principal, schema_sql


def main():
    connection = pymysql.connect(host=os.getenv("MYDB_HOST", "127.0.0.1"),
                                 port=int(os.getenv("MYDB_PORT", "8738")),
                                 user=os.getenv("MYDB_USER", "root"),
                                 password=os.getenv("MYDB_PASSWORD", "root"),
                                 autocommit=True, charset="utf8mb4", ssl_disabled=True)
    database = "hybrid_demo_" + uuid.uuid4().hex
    try:
        with connection.cursor() as cursor:
            cursor.execute("CREATE DATABASE " + database)
            cursor.execute("USE " + database)
            for sql in schema_sql(3):
                cursor.execute(sql)
            rows = [
                (1, 1, 10, 0, "database transaction guide", [1, 0, 0], "v1", 1, 0, None, "doc:10", 1, 0, 26, 1, 1),
                (1, 2, 10, 1, "database rollback details", [0.9, 0.1, 0], "v1", 1, 0, None, "doc:10", 1, 27, 52, 1, 1),
                (1, 3, 20, 0, "database private record", [1, 0, 0], "v1", 1, 0, None, "doc:20", 1, 0, 23, 1, 1),
                (2, 1, 30, 0, "database foreign tenant", [1, 0, 0], "v1", 1, 0, None, "doc:30", 1, 0, 23, 1, 1),
                (1, 4, 40, 0, "database expired guide", [1, 0, 0], "v1", 1, 0, "2020-01-01", "doc:40", 1, 0, 22, 1, 1),
                (1, 5, 50, 0, "database deleted guide", [1, 0, 0], "v1", 1, 1, None, "doc:50", 1, 0, 22, 1, 1),
                (1, 6, 60, 0, "database model migration", [1, 0, 0], "v0", 0, 0, None, "doc:60", 1, 0, 24, 1, 1),
            ]
            for row in rows:
                row = (*row[:5], json.dumps(row[5]), *row[6:], json.dumps({"kind": "guide"}))
                cursor.execute("INSERT INTO rag_chunks VALUES(" + ",".join(["%s"] * 17) + ")", row)
            cursor.executemany("INSERT INTO rag_acl VALUES(%s,%s,%s)",
                               [(1, 10, 7), (2, 30, 7), (1, 40, 7), (1, 50, 7), (1, 60, 7)])
            cursor.execute("CREATE TABLE candidate_a(id INT,relevance INT)")
            cursor.execute("CREATE TABLE candidate_b(id INT,distance INT)")
            cursor.execute("INSERT INTO candidate_a VALUES(1,3),(1,2),(2,1)")
            cursor.execute("INSERT INTO candidate_b VALUES(2,0),(3,1)")
            cursor.execute("""WITH
              ar AS (SELECT id,ROW_NUMBER() OVER(ORDER BY relevance DESC,id) AS rk FROM candidate_a ORDER BY relevance DESC,id LIMIT 3),
              br AS (SELECT id,ROW_NUMBER() OVER(ORDER BY distance,id) AS rk FROM candidate_b ORDER BY distance,id LIMIT 2),
              all_ranks AS (SELECT id,1 AS branch_id,rk FROM ar UNION ALL SELECT id,2 AS branch_id,rk FROM br),
              best AS (SELECT id,branch_id,MIN(rk) AS rk FROM all_ranks GROUP BY id,branch_id)
              SELECT id,SUM(1e0/(60+rk)) AS score,MIN(rk),COUNT(*) FROM best GROUP BY id
              ORDER BY score DESC,MIN(rk),id""")
            manual = cursor.fetchall()
            cursor.execute("""SELECT id,rrf_score,best_rank,match_count FROM (
              (SELECT id FROM candidate_a ORDER BY relevance DESC,id LIMIT 3) RRF
              (SELECT id FROM candidate_b ORDER BY distance,id LIMIT 2)) h
              ORDER BY rrf_score DESC,best_rank,id""")
            native = cursor.fetchall()
            expected = [(2,1/63+1/61,1,2),(1,1/61,1,1),(3,1/62,2,1)]
            for actual in (manual, native):
                assert len(actual) == len(expected)
                for row, oracle in zip(actual, expected):
                    assert row[0] == oracle[0] and row[2:] == oracle[2:]
                    assert math.isclose(float(row[1]),oracle[1],abs_tol=1e-9,rel_tol=1e-9)
        retriever = HybridRetriever(connection, Principal(1, 7))
        assert not retriever.retrieve("database", [1,0,0], "V1", weights=(0,1)).candidates
        assert not retriever.retrieve("database", [1,0,0], "v1 ", weights=(0,1)).candidates
        result = retriever.retrieve("database", [1, 0, 0], "v1", details=True)
        assert {c.chunk_id for c in result.candidates} == {1, 2, 6}
        migrating = next(c for c in result.candidates if c.chunk_id == 6)
        assert migrating.rrf_details[1]["rank"] is None
        assert all(c.tenant_id == 1 for c in result.candidates)
        calls = []

        def rerank(query, candidates):
            calls.append((query, len(candidates)))
            return [float(c.chunk_id == 2) for c in candidates]

        selected = retriever.search("database", [1, 0, 0], "v1", limit=1,
                                    per_document=1, rerank=rerank)
        assert calls == [("database", 3)]
        assert selected.candidates[0].chunk_id == 2
        assert retriever.fetch_evidence(selected.candidates)[0].body == "database rollback details"
        assert {row["chunk_id"] for row in retriever.context(selected.candidates[0])} == {1, 2}
        with connection.cursor() as cursor:
            cursor.execute("UPDATE rag_chunks SET content_version=2,body='changed' WHERE tenant_id=1 AND chunk_id=2")
        try:
            retriever.fetch_evidence(selected.candidates)
        except EvidenceUnavailable:
            pass
        else:
            raise AssertionError("stale evidence was accepted")
        fresh = next(c for c in retriever.retrieve("database", [1,0,0], "v1").candidates if c.chunk_id == 2)
        try:
            retriever.fetch_evidence([selected.candidates[0], fresh])
        except EvidenceUnavailable:
            pass
        else:
            raise AssertionError("mixed stale/fresh versions of one identity were accepted")
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM rag_acl WHERE tenant_id=1 AND document_id=10 AND principal_id=7")
        assert {c.chunk_id for c in retriever.retrieve("database", [1, 0, 0], "v1").candidates} == {6}
        if os.getenv("MYDB_TEST_LANGCHAIN"):
            from hybrid_langchain import as_runnable
            documents = as_runnable(retriever, lambda _: [1, 0, 0], "v1", limit=1).invoke("database")
            assert documents[0].metadata["chunk_id"] == 6
        print("PASS hybrid retrieval, tenant/ACL/expiry/deletion/model filters, external rerank, evidence versions and context")
    finally:
        with connection.cursor() as cursor:
            cursor.execute("DROP DATABASE IF EXISTS " + database)
        connection.close()


if __name__ == "__main__":
    main()
