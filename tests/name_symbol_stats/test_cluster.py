import unittest

from name_symbol_stats.cluster import ScoredPair, cluster_pairs_by_threshold


class ClusterTests(unittest.TestCase):
    def test_clusters_expand_when_threshold_is_lowered(self) -> None:
        nodes = ["a", "b", "c", "d", "e"]
        pairs = [
            ScoredPair("a", "b", 91.0),
            ScoredPair("b", "c", 86.0),
            ScoredPair("d", "e", 95.0),
        ]

        high_threshold = cluster_pairs_by_threshold(nodes, pairs, threshold=90.0)
        low_threshold = cluster_pairs_by_threshold(nodes, pairs, threshold=85.0)

        self.assertEqual(sorted(sorted(group) for group in high_threshold), [["a", "b"], ["d", "e"]])
        self.assertEqual(sorted(sorted(group) for group in low_threshold), [["a", "b", "c"], ["d", "e"]])


if __name__ == "__main__":
    unittest.main()
