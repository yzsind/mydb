"""Demand-driven top-K over an append-only, two-hop neighborhood."""

from bisect import bisect_left, insort
from collections import Counter, defaultdict
from dataclasses import dataclass
from heapq import heapify, heappop, heappush


@dataclass
class QueryState:
    authors: set
    result: list


class NeighborhoodTopK:
    def __init__(self):
        self.adjacent = defaultdict(set)
        self.by_author = defaultdict(list)
        self.messages = {}
        self.states = {}
        self.by_seed = defaultdict(set)
        self.by_subscribed_author = defaultdict(set)
        self.dirty = set()
        self.stats = Counter()

    def add_link(self, a, b):
        if b in self.adjacent[a]:
            return
        if self.states:
            # A two-hop result can only use the new edge if its start is an
            # endpoint or was already adjacent to an endpoint.
            for seed in self.adjacent[a] | self.adjacent[b] | {a, b}:
                self.dirty.update(self.by_seed.get(seed, ()))
        self.adjacent[a].add(b)
        self.adjacent[b].add(a)

    def add_message(self, author, ident, when, body):
        if ident in self.messages:
            raise ValueError(f"Duplicate message {ident}")
        self.messages[ident] = body
        insort(self.by_author[author], (when, -ident))
        rank = (-when, ident, author)
        for key in self.by_subscribed_author.get(author, ()):
            if key in self.dirty:
                continue
            self.stats["message_subscriptions_checked"] += 1
            _, cutoff, limit = key
            if when >= cutoff or limit == 0:
                continue
            results = self.states[key].result
            if len(results) == limit and rank >= results[-1]:
                continue
            insort(results, rank)
            if len(results) > limit:
                results.pop()
            self.stats["message_updates_to_top_k"] += 1

    def authors(self, seed):
        direct = self.adjacent.get(seed, set())
        result = set(direct)
        for friend in direct:
            result.update(self.adjacent[friend])
        result.discard(seed)
        return result

    def top_k(self, authors, cutoff, limit):
        queue = []
        for author in authors:
            messages = self.by_author.get(author, ())
            i = bisect_left(messages, (cutoff,)) - 1
            if i >= 0:
                when, reverse_id = messages[i]
                queue.append((-when, -reverse_id, author, i))
        heapify(queue)
        result = []
        while len(result) < limit and queue:
            neg_when, ident, author, i = heappop(queue)
            result.append((neg_when, ident, author))
            if i > 0:
                when, reverse_id = self.by_author[author][i - 1]
                heappush(queue, (-when, -reverse_id, author, i - 1))
        return result

    def read(self, key, reuse):
        seed, cutoff, limit = key
        if not reuse:
            self.stats["full_plan_evaluations"] += 1
            return self.top_k(self.authors(seed), cutoff, limit)
        previous = self.states.get(key)
        if previous is not None and key not in self.dirty:
            self.stats["incremental_reads"] += 1
            return previous.result
        authors = self.authors(seed)
        if previous is None:
            self.by_seed[seed].add(key)
            old_authors = set()
        else:
            old_authors = previous.authors
        for author in old_authors - authors:
            self.by_subscribed_author[author].remove(key)
        for author in authors - old_authors:
            self.by_subscribed_author[author].add(key)
        state = QueryState(authors, self.top_k(authors, cutoff, limit))
        self.states[key] = state
        self.dirty.discard(key)
        self.stats["full_plan_evaluations"] += 1
        return state.result
