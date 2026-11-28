#!/usr/bin/env python3
"""Build and query a tiny GraphRAG knowledge base on a local mydb server."""

import argparse
import os

import mysql.connector


def demo(connection):
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE rag_documents (id VARCHAR(16) PRIMARY KEY, passage VARCHAR(200) NOT NULL, embedding VECTOR(3) NOT NULL)")
    cursor.execute("""
        INSERT INTO rag_documents VALUES
          ('d1', 'mydb uses MVCC for concurrent transactions.', TO_VECTOR('[1,0,0]')),
          ('d2', 'Snapshots determine which row versions are visible.', TO_VECTOR('[0,1,0]')),
          ('d3', 'WAL recovery replays durable changes after a crash.', TO_VECTOR('[0,0,1]'))
    """)
    cursor.execute("CREATE INDEX rag_embedding ON rag_documents(embedding) USING HNSW WITH (metric=l2)")
    cursor.execute("CREATE GRAPH rag_knowledge ANY PROPERTY GRAPH")
    cursor.execute("SESSION SET GRAPH rag_knowledge")
    cursor.execute("CREATE UNIQUE GRAPH INDEX entity_key ON rag_knowledge NODE LABEL Entity (key STRING)")
    cursor.execute("""
        INSERT (a:Entity & Passage {key: 'd1'}), (b:Entity & Passage {key: 'd2'}),
               (c:Entity & Passage {key: 'd3'}),
               (a)-[:Explains {citation: 'mvcc-and-locking'}]->(b),
               (b)-[:Explains {citation: 'backup-and-recovery'}]->(c)
    """)

    cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    cursor.execute("START TRANSACTION READ ONLY")
    prepared = connection.cursor(prepared=True)
    try:
        prepared.execute("""
            SELECT id FROM rag_documents
            ORDER BY L2_DISTANCE(embedding, TO_VECTOR(%s)), id LIMIT 1
        """, ("[1,0,0]",))
        seeds = [row[0] for row in prepared.fetchall()]
        marks = ", ".join(["%s"] * len(seeds))
        gql = f"""
            FOR seed IN [{marks}]
            MATCH (start:Entity {{key: seed}})-[edge:Explains]->(related:Passage)
            RETURN DISTINCT CAST(related.key AS STRING) AS document_id,
                            CAST(edge.citation AS STRING) AS citation
            ORDER BY document_id
        """
        prepared.execute(gql, tuple(seeds))
        citations = dict(prepared.fetchall())
        ids = sorted(set(seeds) | citations.keys())
        marks = ", ".join(["%s"] * len(ids))
        prepared.execute(f"SELECT id, passage FROM rag_documents WHERE id IN ({marks}) ORDER BY id", tuple(ids))
        passages = prepared.fetchall()
        assert passages == [
            ("d1", "mydb uses MVCC for concurrent transactions."),
            ("d2", "Snapshots determine which row versions are visible."),
        ], passages
        assert citations == {"d2": "mvcc-and-locking"}, citations
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        prepared.close()
        cursor.close()
    for doc_id, passage in passages:
        print(f"{doc_id} [{citations.get(doc_id, 'vector seed')}] {passage}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=4837)
    parser.add_argument("--database", default="test")
    args = parser.parse_args()
    with mysql.connector.connect(
        host="127.0.0.1",
        port=args.port,
        database=args.database,
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", "root"),
        autocommit=True,
        use_pure=True,
    ) as connection:
        demo(connection)


if __name__ == "__main__":
    main()
