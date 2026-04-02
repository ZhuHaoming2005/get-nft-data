import unittest

from name_symbol_stats_v2.main import build_parser


class MainParserTests(unittest.TestCase):
    def test_prepare_name_tasks_accepts_blocking_strategy(self):
        parser = build_parser()
        args = parser.parse_args([
            'prepare-name-tasks',
            '--run-label', 'apr01',
            '--chains', 'ethereum',
            '--blocking-strategy', 'adaptive_v1',
        ])
        self.assertEqual(args.blocking_strategy, 'adaptive_v1')


if __name__ == '__main__':
    unittest.main()
