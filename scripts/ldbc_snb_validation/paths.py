"""Shortest paths in an undirected graph with dependency-aware adjacency reads."""

from collections import defaultdict
from functools import cache


def shortest(a, b, neighbors, paths):
    if a == b:
        return 0, [(a,)] if paths else []

    fronts = [{a}, {b}]
    reached = [{a}, {b}]
    parents = [{}, {}]
    distance = 0
    while fronts[0] and fronts[1]:
        side = int(len(fronts[1]) < len(fronts[0]))
        other = 1 - side
        following = defaultdict(list)
        meetings = []
        distance += 1
        for vertex in fronts[side]:
            for adjacent in neighbors(vertex):
                if adjacent in fronts[other]:
                    if not paths:
                        return distance, []
                    meetings.append(
                        (vertex, adjacent) if side == 0 else (adjacent, vertex)
                    )
                elif adjacent not in reached[side]:
                    following[adjacent].append(vertex)
        if meetings:
            # Each half has a complete BFS layer. All shortest paths cross
            # one of the edges between these two frontier layers.
            return distance, join_paths(meetings, (a, b), parents)
        fronts[side] = set(following)
        reached[side].update(following)
        if paths:
            parents[side].update(following)
    return -1, []


def join_paths(meetings, roots, parents):
    @cache
    def prefixes(side, vertex):
        if vertex == roots[side]:
            return [(vertex,)]
        return [
            path + (vertex,)
            for parent in parents[side][vertex]
            for path in prefixes(side, parent)
        ]

    return [
        left + right[::-1]
        for a, b in meetings
        for left in prefixes(0, a)
        for right in prefixes(1, b)
    ]
