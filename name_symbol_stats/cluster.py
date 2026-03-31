from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Sequence


@dataclass(frozen=True)
class ScoredPair:
    left: str
    right: str
    score: float


class UnionFind:
    def __init__(self, nodes: Iterable[str]) -> None:
        self.parent = {node: node for node in nodes}

    def find(self, node: str) -> str:
        root = self.parent.setdefault(node, node)
        while self.parent[root] != root:
            self.parent[root] = self.parent[self.parent[root]]
            root = self.parent[root]
        return root

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root


def cluster_pairs_by_threshold(
    nodes: Sequence[str],
    pairs: Sequence[ScoredPair],
    *,
    threshold: float,
) -> list[tuple[str, ...]]:
    union_find = UnionFind(nodes)
    active_nodes: set[str] = set()
    for pair in pairs:
        if pair.score < threshold:
            continue
        union_find.union(pair.left, pair.right)
        active_nodes.add(pair.left)
        active_nodes.add(pair.right)

    groups: dict[str, set[str]] = defaultdict(set)
    for node in active_nodes:
        groups[union_find.find(node)].add(node)

    return sorted(
        (tuple(sorted(group)) for group in groups.values() if len(group) >= 2),
        key=lambda group: (len(group), group),
    )
