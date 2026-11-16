"""Offline contract tests; integration coverage lives in hybrid_demo.py."""
from dataclasses import replace
import math
import unittest

from hybrid import EvidenceUnavailable, Candidate, HybridRetriever, Principal, SearchResult, _identifier, schema_sql


def candidate(chunk, document=1):
    return Candidate(1, chunk, document, chunk, "body", "doc:1", None, None, None, 1, 1, 1.0/chunk, None)


class StubRetriever(HybridRetriever):
    def retrieve(self, *args, **kwargs):
        return SearchResult((candidate(1), candidate(2), candidate(3, 2)), 3, True, False, "v1")


class HybridContractTests(unittest.TestCase):
    def test_identifiers_principal_and_schema(self):
        self.assertEqual(_identifier("db.chunks"), "`db`.`chunks`")
        for value in ("chunks;DROP TABLE x", "a.b.c", "`x`", "a b", ""):
            with self.assertRaises(ValueError):
                _identifier(value)
        for value in (-1, True, 2**64, "1"):
            with self.assertRaises(ValueError):
                Principal(value, 1)
        for value in (0, 16384, True):
            with self.assertRaises(ValueError):
                schema_sql(value)

    def test_invalid_queries_never_touch_connection(self):
        retriever = HybridRetriever(None, Principal(1, 7))
        for options in ({'branch_limit': 0}, {'fusion_limit': -1}, {'rank_constant': 2**32},
                        {'weights': (0, 0)}, {'weights': (math.inf, 1)}, {'weights': (True, 1)},
                        {'details': "TRUE"}, {'weights': (1,)}):
            with self.assertRaises(ValueError):
                retriever.retrieve("query", [1, 0], "v1", **options)
        for vector in ([], [0, 0], [math.nan], [math.inf], [1]*16384):
            with self.assertRaises(ValueError):
                retriever.retrieve("query", vector, "v1")

    def test_rerank_batch_scores_group_cap_and_shortfall(self):
        retriever = StubRetriever(None, Principal(1, 7))
        calls = []
        def rerank(query, rows):
            calls.append((query, len(rows)))
            return [0, 3, 2]
        result = retriever.search("q", [1], "v1", limit=3, per_document=1, rerank=rerank)
        self.assertEqual(calls, [("q", 3)])
        self.assertEqual([c.chunk_id for c in result.candidates], [2, 3])
        self.assertEqual([c.rerank_score for c in result.candidates], [3, 2])
        self.assertTrue(result.insufficient_candidates)
        self.assertEqual(result.candidate_count, 3)
        for scores in ([1], [1, 2, math.nan]):
            with self.assertRaises(ValueError):
                retriever.search("q", [1], "v1", rerank=lambda *_: scores)

    def test_evidence_checks_each_version_even_for_repeated_identity(self):
        class EvidenceRetriever(HybridRetriever):
            def _rows(self, *_):
                return [dict(chunk_id=1,content_version=2,document_version=1,body="new",
                             source_uri="doc:1",source_page=None,char_start=None,char_end=None)]
        retriever = EvidenceRetriever(None, Principal(1,7))
        stale, fresh = candidate(1), replace(candidate(1),content_version=2)
        with self.assertRaises(EvidenceUnavailable):
            retriever.fetch_evidence([stale,fresh])
        self.assertEqual(retriever.fetch_evidence([fresh])[0].body,"new")

    def test_evidence_rejects_cross_tenant_before_io(self):
        retriever = HybridRetriever(None, Principal(2, 7))
        with self.assertRaises(ValueError):
            retriever.fetch_evidence([candidate(1)])
        with self.assertRaises(ValueError):
            retriever.context(candidate(1))
        self.assertEqual(retriever.fetch_evidence([]), ())


if __name__ == "__main__":
    unittest.main()
