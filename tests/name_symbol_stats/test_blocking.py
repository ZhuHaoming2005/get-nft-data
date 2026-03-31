import unittest

from name_symbol_stats.blocking import BlockingRecord, partition_records
from name_symbol_stats.normalize import build_name_block_key, normalize_name


class BlockingTests(unittest.TestCase):
    def _record(self, contract_address: str, raw_name: str) -> BlockingRecord:
        name_norm = normalize_name(raw_name)
        return BlockingRecord(
            chain="ethereum",
            contract_address=contract_address,
            nft_count=10,
            name_norm=name_norm,
            name_len=len(name_norm),
            name_block_key=build_name_block_key(name_norm),
        )

    def test_unrelated_names_do_not_share_a_partition(self) -> None:
        records = [
            self._record("0x1", "CryptoPunks"),
            self._record("0x2", "Mutant Ape Yacht Club"),
        ]

        partitions = partition_records(records, max_block_size=10)

        self.assertEqual(len(partitions), 2)
        self.assertEqual({len(partition.records) for partition in partitions}, {1})

    def test_large_block_is_split_into_smaller_partitions(self) -> None:
        records = [
            self._record("0x1", "ape alpha"),
            self._record("0x2", "ape beta"),
            self._record("0x3", "ape gamma"),
            self._record("0x4", "ape delta"),
        ]

        partitions = partition_records(records, max_block_size=2)

        self.assertGreater(len(partitions), 1)
        self.assertTrue(all(len(partition.records) <= 2 for partition in partitions))


if __name__ == "__main__":
    unittest.main()
