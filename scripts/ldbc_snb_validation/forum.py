"""Incremental forum scores over dated memberships in a two-hop neighborhood."""

from bisect import bisect_left, bisect_right, insort
from collections import Counter, defaultdict
from dataclasses import dataclass
from heapq import nsmallest


@dataclass
class ForumView:
    authors: set
    scores: dict
    ranked: list

    def add_score(self, forum, count, limit):
        previous = self.scores.get(forum)
        if previous is not None and count == 0:
            return
        score = (previous or 0) + count
        self.scores[forum] = score
        if limit == 0:
            return
        if previous is not None:
            old = (-previous, forum)
            index = bisect_left(self.ranked, old)
            if index < len(self.ranked) and self.ranked[index] == old:
                self.ranked.pop(index)
        rank = (-score, forum)
        if len(self.ranked) < limit or rank < self.ranked[-1]:
            insort(self.ranked, rank)
            if len(self.ranked) > limit:
                self.ranked.pop()


class ForumActivity:
    def __init__(self, adjacency):
        self.adjacency = adjacency
        self.memberships = defaultdict(dict)
        self.ordered_memberships = {}
        self.posts = Counter()
        self.views = {}
        self.by_seed = defaultdict(set)
        self.by_author = defaultdict(set)
        self.stats = Counter()

    def add_member(self, author, forum, when):
        forums = self.memberships[author]
        previous = forums.get(forum)
        if previous is not None and when <= previous:
            return
        forums[forum] = when
        ordered = self.ordered_memberships.get(author)
        if ordered is not None:
            if previous is not None:
                ordered.pop(bisect_left(ordered, (previous, forum)))
            insort(ordered, (when, forum))
        if not self.views:
            return
        for key in self.by_author.get(author, ()):
            _, minimum, limit = key
            if when > minimum and (previous is None or previous <= minimum):
                self.views[key].add_score(forum, self.posts[author, forum], limit)
                self.stats["membership_deltas"] += 1

    def add_post(self, author, forum):
        self.posts[author, forum] += 1
        if not self.views:
            return
        joined = self.memberships.get(author, {}).get(forum)
        if joined is None:
            return
        for key in self.by_author.get(author, ()):
            _, minimum, limit = key
            if joined > minimum:
                self.views[key].add_score(forum, 1, limit)
                self.stats["post_deltas"] += 1

    def eligible_forums(self, author, minimum):
        ordered = self.ordered_memberships.get(author)
        if ordered is None:
            ordered = sorted(
                (joined, forum)
                for forum, joined in self.memberships.get(author, {}).items()
            )
            self.ordered_memberships[author] = ordered
        start = bisect_right(ordered, (minimum, float("inf")))
        return (forum for _, forum in ordered[start:])

    def add_reachable(self, key, author):
        seed, minimum, limit = key
        view = self.views[key]
        if author == seed or author in view.authors:
            return
        view.authors.add(author)
        self.by_author[author].add(key)
        for forum in self.eligible_forums(author, minimum):
            view.add_score(forum, self.posts[author, forum], limit)
        self.stats["new_reachable_authors"] += 1

    def link_added(self, a, b):
        # The only new walks of length at most two contain the appended edge.
        for endpoint, opposite in ((a, b), (b, a)):
            for key in self.by_seed.get(endpoint, ()):
                self.add_reachable(key, opposite)
                for author in self.adjacency[opposite]:
                    self.add_reachable(key, author)
            for neighbor in self.adjacency[endpoint]:
                for key in self.by_seed.get(neighbor, ()):
                    self.add_reachable(key, opposite)

    def read(self, seed, minimum, limit):
        key = (seed, minimum, limit)
        view = self.views.get(key)
        if view is None:
            direct = self.adjacency.get(seed, ())
            authors = set(direct)
            for friend in direct:
                authors.update(self.adjacency[friend])
            authors.discard(seed)
            scores = Counter()
            for author in authors:
                self.by_author[author].add(key)
                for forum in self.eligible_forums(author, minimum):
                    scores[forum] += self.posts[author, forum]
            rank = nsmallest(
                limit, ((-count, forum) for forum, count in scores.items())
            )
            view = ForumView(authors, scores, rank)
            self.views[key] = view
            self.by_seed[seed].add(key)
            self.stats["initial_evaluations"] += 1
        else:
            self.stats["incremental_reads"] += 1
        return view.ranked
