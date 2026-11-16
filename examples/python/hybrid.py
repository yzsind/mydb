"""Tenant-scoped RRF retrieval and evidence fetching over a DB-API connection.

The application supplies the authenticated principal and embeddings. This
module neither calls a model service nor infers authorization from agent input.
"""
from dataclasses import dataclass, replace
import json
import math
import re
from typing import Callable, Optional, Sequence, Tuple


def _identifier(name: str) -> str:
    parts = name.split(".")
    if not 1 <= len(parts) <= 2 or any(not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", p) for p in parts):
        raise ValueError("Expected an ASCII table name, optionally database-qualified")
    return ".".join("`" + p + "`" for p in parts)


def _unsigned(value: int, name: str, maximum: int = 2**64 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise ValueError(name + " must be an unsigned integer in range")
    return value


@dataclass(frozen=True)
class Principal:
    tenant_id: int
    principal_id: int

    def __post_init__(self):
        _unsigned(self.tenant_id, "tenant_id")
        _unsigned(self.principal_id, "principal_id")


@dataclass(frozen=True)
class Candidate:
    tenant_id: int
    chunk_id: int
    document_id: int
    chunk_order: int
    body: str
    source_uri: str
    source_page: Optional[int]
    char_start: Optional[int]
    char_end: Optional[int]
    content_version: int
    document_version: int
    rrf_score: float
    rrf_details: Optional[list]
    rerank_score: Optional[float] = None
    metadata: Optional[dict] = None


@dataclass(frozen=True)
class SearchResult:
    candidates: Tuple[Candidate, ...]
    candidate_count: int
    at_fusion_limit: bool
    insufficient_candidates: bool
    query_model_version: str


class EvidenceUnavailable(RuntimeError):
    """A candidate is no longer authorized, visible, or at its original version."""


class HybridRetriever:
    def __init__(self, connection, principal: Principal, table="rag_chunks", acl_table="rag_acl"):
        self.connection = connection
        self.principal = principal
        self.table = _identifier(table)
        self.acl = _identifier(acl_table)

    def _eligible(self, alias: str) -> str:
        return (
            f"{alias}.tenant_id=%s AND {alias}.is_deleted=0 "
            f"AND ({alias}.expires_at IS NULL OR {alias}.expires_at>CURRENT_TIMESTAMP) "
            f"AND EXISTS(SELECT 1 FROM {self.acl} AS acl "
            f"WHERE acl.tenant_id={alias}.tenant_id AND acl.document_id={alias}.document_id "
            "AND acl.principal_id=%s)"
        )

    def _rows(self, sql, parameters):
        with self.connection.cursor() as cursor:
            cursor.execute(sql, parameters)
            names = [column[0] for column in cursor.description]
            return [dict(zip(names, row)) for row in cursor.fetchall()]

    def retrieve(self, query: str, vector: Sequence[float], model_version: str,
                 branch_limit=100, fusion_limit=100, weights=(1.0, 1.0),
                 rank_constant=60, details=False) -> SearchResult:
        if not isinstance(query, str) or not isinstance(model_version, str) or not model_version:
            raise ValueError("query and a nonempty model_version are required")
        _unsigned(branch_limit, "branch_limit", 10000)
        _unsigned(fusion_limit, "fusion_limit", 10000)
        _unsigned(rank_constant, "rank_constant", 2**32 - 1)
        if branch_limit == 0 or fusion_limit == 0:
            raise ValueError("retrieval windows must be positive")
        if not isinstance(details, bool):
            raise ValueError("details must be a boolean")
        if len(weights) != 2 or any(isinstance(w, bool) for w in weights):
            raise ValueError("two numeric weights are required")
        weights = tuple(float(w) for w in weights)
        if any(not math.isfinite(w) or w < 0 for w in weights) or not any(weights):
            raise ValueError("two finite nonnegative weights, at least one positive, are required")
        vector = [float(value) for value in vector]
        if not 1 <= len(vector) <= 16383 or not all(math.isfinite(value) for value in vector) or not any(vector):
            raise ValueError("COSINE retrieval requires a finite nonzero query vector")
        details_option = "TRUE" if details else "FALSE"
        details_projection = "s.rrf_details" if details else "NULL AS rrf_details"
        sql = f"""SELECT d.tenant_id,d.chunk_id,d.document_id,d.chunk_order,d.body,
            d.source_uri,d.source_page,d.char_start,d.char_end,
            d.content_version,d.document_version,d.metadata,s.rrf_score,{details_projection}
        FROM (
            SELECT h.* FROM (
                (SELECT c.tenant_id,c.chunk_id FROM {self.table} AS c
                 WHERE {self._eligible('c')} AND MATCH(c.body) AGAINST(%s)
                 ORDER BY MATCH(c.body) AGAINST(%s) DESC,c.chunk_id LIMIT %s) AS lexical
                RRF
                (SELECT c.tenant_id,c.chunk_id FROM {self.table} AS c
                 WHERE {self._eligible('c')} AND c.embedding_ready=1
                   AND c.embedding_model_version=%s
                 ORDER BY COSINE_DISTANCE(c.embedding,TO_VECTOR(%s)),c.chunk_id LIMIT %s) AS semantic
                WITH(RANK_CONSTANT=%s,WEIGHTS=(%s,%s),SCORE_DETAILS={details_option})
            ) AS h ORDER BY h.rrf_score DESC,h.best_rank,h.tenant_id,h.chunk_id LIMIT %s
        ) AS s JOIN {self.table} AS d
          ON d.tenant_id=s.tenant_id AND d.chunk_id=s.chunk_id
        ORDER BY s.rrf_score DESC,s.best_rank,s.tenant_id,s.chunk_id"""
        tenant, principal = self.principal.tenant_id, self.principal.principal_id
        rows = self._rows(sql, (tenant, principal, query, query, branch_limit,
                               tenant, principal, model_version, json.dumps(vector), branch_limit,
                               rank_constant, *weights, fusion_limit))
        candidates = []
        for row in rows:
            if row["rrf_details"] is not None:
                row["rrf_details"] = json.loads(row["rrf_details"])
            if row["metadata"] is not None:
                row["metadata"] = json.loads(row["metadata"])
            row["rrf_score"] = float(row["rrf_score"])
            candidates.append(Candidate(**row))
        return SearchResult(tuple(candidates), len(candidates), len(candidates) == fusion_limit,
                            False, model_version)

    def search(self, query: str, vector: Sequence[float], model_version: str, limit=20,
               per_document=2, rerank: Optional[Callable[[str, Sequence[Candidate]], Sequence[float]]] = None,
               **options) -> SearchResult:
        _unsigned(limit, "limit", 1000)
        _unsigned(per_document, "per_document", 1000)
        if not limit or not per_document:
            raise ValueError("limit and per_document must be positive")
        result = self.retrieve(query, vector, model_version, **options)
        candidates = list(result.candidates)
        if rerank is not None and candidates:
            scores = [float(score) for score in rerank(query, tuple(candidates))]
            if len(scores) != len(candidates) or not all(math.isfinite(score) for score in scores):
                raise ValueError("rerank must return one finite score per input candidate")
            candidates = [replace(candidate, rerank_score=score) for score, candidate in sorted(
                zip(scores, candidates),
                key=lambda pair: (-pair[0], -pair[1].rrf_score, pair[1].tenant_id, pair[1].chunk_id))]
        counts, selected = {}, []
        for candidate in candidates:
            identity = (candidate.tenant_id, candidate.document_id)
            if counts.get(identity, 0) >= per_document:
                continue
            counts[identity] = counts.get(identity, 0) + 1
            selected.append(candidate)
            if len(selected) == limit:
                break
        return replace(result, candidates=tuple(selected), insufficient_candidates=len(selected) < limit)

    def fetch_evidence(self, candidates: Sequence[Candidate]) -> Tuple[Candidate, ...]:
        if not candidates:
            return ()
        if len(candidates) > 1000 or any(c.tenant_id != self.principal.tenant_id for c in candidates):
            raise ValueError("evidence must belong to the bound tenant; batch limit is 1000")
        clauses, parameters = [], [self.principal.tenant_id, self.principal.principal_id]
        for candidate in candidates:
            clauses.append("(d.chunk_id=%s AND d.content_version=%s AND d.document_version=%s)")
            parameters.extend((candidate.chunk_id, candidate.content_version, candidate.document_version))
        rows = self._rows(
            f"SELECT d.chunk_id,d.content_version,d.document_version,d.body,d.source_uri,d.source_page,d.char_start,d.char_end "
            f"FROM {self.table} AS d WHERE {self._eligible('d')} AND (" + " OR ".join(clauses) + ")",
            tuple(parameters))
        def identity(candidate):
            return (candidate.chunk_id, candidate.content_version, candidate.document_version)
        available = {(row["chunk_id"], row["content_version"], row["document_version"]): row for row in rows}
        if any(identity(candidate) not in available for candidate in candidates):
            raise EvidenceUnavailable("Candidate is unavailable or its content/document version changed")
        return tuple(replace(candidate, **{key: value for key, value in available[identity(candidate)].items()
                                           if key != "chunk_id"}) for candidate in candidates)

    def context(self, candidate: Candidate, radius=1):
        if candidate.tenant_id != self.principal.tenant_id:
            raise ValueError("candidate does not belong to the bound tenant")
        _unsigned(radius, "radius", 100)
        rows = self._rows(f"""SELECT d.chunk_id,d.chunk_order,d.body,d.source_uri,
            d.source_page,d.char_start,d.char_end,d.content_version,d.document_version
            FROM {self.table} AS anchor JOIN {self.table} AS d
              ON d.tenant_id=anchor.tenant_id AND d.document_id=anchor.document_id
             AND d.document_version=anchor.document_version
            WHERE {self._eligible('d')} AND anchor.chunk_id=%s AND anchor.content_version=%s
              AND anchor.document_version=%s AND anchor.is_deleted=0
              AND (anchor.expires_at IS NULL OR anchor.expires_at>CURRENT_TIMESTAMP)
              AND d.chunk_order BETWEEN anchor.chunk_order-%s AND anchor.chunk_order+%s
            ORDER BY d.chunk_order,d.chunk_id""",
            (self.principal.tenant_id, self.principal.principal_id, candidate.chunk_id,
             candidate.content_version, candidate.document_version, radius, radius))
        if not any(row["chunk_id"] == candidate.chunk_id for row in rows):
            raise EvidenceUnavailable("Context anchor is unavailable or its version changed")
        return rows


def schema_sql(dimensions=384):
    _unsigned(dimensions, "dimensions", 16383)
    if not dimensions:
        raise ValueError("dimensions must be positive")
    return (f"""CREATE TABLE rag_chunks (
        tenant_id BIGINT UNSIGNED NOT NULL,chunk_id BIGINT UNSIGNED NOT NULL,
        document_id BIGINT UNSIGNED NOT NULL,chunk_order INT NOT NULL,
        body TEXT NOT NULL,embedding VECTOR({dimensions}) NOT NULL,
        embedding_model_version VARCHAR(64) COLLATE utf8mb4_0900_bin NOT NULL,embedding_ready BOOLEAN NOT NULL,
        is_deleted BOOLEAN NOT NULL DEFAULT FALSE,expires_at DATETIME(6),
        source_uri VARCHAR(1024) NOT NULL,source_page INT,char_start BIGINT,char_end BIGINT,
        content_version BIGINT UNSIGNED NOT NULL,document_version BIGINT UNSIGNED NOT NULL,
        metadata JSON,
        PRIMARY KEY(tenant_id,chunk_id),KEY document_chunks(tenant_id,document_id,chunk_order),
        FULLTEXT KEY body_ft(body))""",
        "CREATE INDEX embedding_hnsw ON rag_chunks(embedding) USING HNSW WITH(metric=cosine)",
        """CREATE TABLE rag_acl(tenant_id BIGINT UNSIGNED NOT NULL,
        document_id BIGINT UNSIGNED NOT NULL,principal_id BIGINT UNSIGNED NOT NULL,
        PRIMARY KEY(tenant_id,document_id,principal_id))""")
