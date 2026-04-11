import json
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

import top_contract_analysis as mod
import top_contract_analysis.batch as batch_mod
import top_contract_analysis.build_rust_ext as build_rust_mod
import top_contract_analysis.export_snapshot as export_mod
import top_contract_analysis.rust_bridge as rust_bridge
from top_contract_analysis.duckdb_store import DuckDBFeatureStore
from top_contract_analysis.rust_bridge import score_metadata_pairs, score_name_pairs
from top_contract_analysis.signal_cache import ContractSignalCache


class RustBridgeTests(unittest.TestCase):
    def test_metadata_document_from_json_flattens_selected_fields(self):
        document = rust_bridge.metadata_document_from_json(
            json.dumps(
                {
                    'name': 'Azuki #1',
                    'description': 'A red hooded anime portrait',
                    'attributes': [
                        {'trait_type': 'Background', 'value': 'Off White D'},
                        {'trait_type': 'Clothing', 'value': 'Maroon Kimono'},
                    ],
                    'external_url': 'https://example.com/azuki/1',
                }
            )
        )

        self.assertIn('red hooded anime portrait', document)
        self.assertIn('off white d', document)
        self.assertIn('maroon kimono', document)
        self.assertIn('https://example.com/azuki/1', document)

    def test_metadata_keywords_returns_ranked_terms(self):
        keywords = rust_bridge.metadata_keywords(
            'maroon kimono maroon kimono off white background red hooded portrait',
            limit=4,
        )

        self.assertEqual(keywords[0], 'kimono')
        self.assertEqual(keywords[1], 'maroon')
        self.assertIn('kimono', keywords)
        self.assertEqual(len(keywords), 4)

    def test_score_name_pairs_normalizes_trailing_ids(self):
        scores = score_name_pairs(
            ['Azuki #123', 'BEANZ Official #77'],
            ['Azuki', 'beanz official'],
        )

        self.assertEqual(len(scores), 2)
        self.assertGreaterEqual(scores[0], 99.0)
        self.assertGreaterEqual(scores[1], 99.0)

    def test_score_metadata_pairs_detects_close_documents(self):
        seed_doc = json.dumps(
            {
                'name': 'Azuki #1',
                'description': 'A red hooded anime portrait on a pale background.',
                'attributes': [
                    {'trait_type': 'Background', 'value': 'Off White D'},
                    {'trait_type': 'Clothing', 'value': 'Maroon Kimono'},
                ],
            }
        )
        candidate_doc = json.dumps(
            {
                'name': 'Azuki #888',
                'description': 'Anime portrait wearing a maroon kimono on an off white background.',
                'attributes': [
                    {'trait_type': 'Background', 'value': 'Off White D'},
                    {'trait_type': 'Clothing', 'value': 'Maroon Kimono'},
                ],
            }
        )

        scores = score_metadata_pairs([seed_doc], [candidate_doc])

        self.assertEqual(len(scores), 1)
        self.assertGreaterEqual(scores[0], 0.55)

    def test_analyze_transfer_signals_computes_expected_metrics(self):
        transfers = [
            mod.TransferRecord(
                contract_address='0xdup',
                token_id='1',
                tx_hash='0xmint',
                log_index=0,
                block_number=10,
                block_time=100,
                from_address=mod.ZERO_ADDRESS,
                to_address='0xminted',
                event_type='erc721',
                source='alchemy',
            ),
            mod.TransferRecord(
                contract_address='0xdup',
                token_id='1',
                tx_hash='0xsale',
                log_index=1,
                block_number=11,
                block_time=460,
                from_address='0xminted',
                to_address='0xbuyer',
                event_type='erc721',
                source='alchemy',
            ),
            mod.TransferRecord(
                contract_address='0xdup',
                token_id='2',
                tx_hash='0xcycle-a',
                log_index=2,
                block_number=12,
                block_time=470,
                from_address='0xa',
                to_address='0xb',
                event_type='erc721',
                source='alchemy',
            ),
            mod.TransferRecord(
                contract_address='0xdup',
                token_id='3',
                tx_hash='0xcycle-b',
                log_index=3,
                block_number=13,
                block_time=480,
                from_address='0xb',
                to_address='0xa',
                event_type='erc721',
                source='alchemy',
            ),
        ]

        result = rust_bridge.analyze_transfer_signals(transfers)

        self.assertEqual(result['mint_address_count'], 1)
        self.assertEqual(result['mint_count'], 1)
        self.assertEqual(result['unique_receiver_count'], 4)
        self.assertEqual(result['cycle_edge_count'], 1)
        self.assertEqual(result['mint_to_first_transfer_seconds'], 360)
        self.assertTrue(result['fast_spread'])

    def test_analyze_victim_signals_counts_stuck_holders(self):
        transfers = [
            mod.TransferRecord(
                contract_address='0xdup',
                token_id='1',
                tx_hash='0xmint',
                log_index=0,
                block_number=10,
                block_time=100,
                from_address=mod.ZERO_ADDRESS,
                to_address='0xholder',
                event_type='erc721',
                source='alchemy',
            ),
            mod.TransferRecord(
                contract_address='0xdup',
                token_id='1',
                tx_hash='0xsale',
                log_index=1,
                block_number=11,
                block_time=460,
                from_address='0xactive',
                to_address='0xbuyer',
                event_type='erc721',
                source='alchemy',
            ),
        ]
        owners = [
            mod.OwnerBalance(owner_address='0xholder', token_balances={'1': 1}),
            mod.OwnerBalance(owner_address='0xactive', token_balances={'2': 1}),
            mod.OwnerBalance(owner_address='0xzero', token_balances={'3': 0}),
        ]

        result = rust_bridge.analyze_victim_signals(transfers, owners)

        self.assertEqual(result['owner_count'], 2)
        self.assertEqual(result['stuck_holder_count'], 1)
        self.assertEqual(result['victim_wallet_count'], 1)
        self.assertEqual(result['stuck_holder_ratio'], 0.5)


class DuckDBSnapshotTests(unittest.TestCase):
    def test_duckdb_feature_store_loads_candidate_rows_with_metadata(self):
        seed_nfts = [
            mod.SeedNFT(
                chain='ethereum',
                contract_address='0xseed',
                token_id='1',
                name='Azuki #1',
                symbol='AZUKI',
                token_uri='ipfs://seed/meta-1',
                image_uri='ipfs://seed/image-1.png',
                metadata_json=json.dumps(
                    {
                        'name': 'Azuki #1',
                        'description': 'A red hooded anime portrait on a pale background.',
                        'attributes': [
                            {'trait_type': 'Background', 'value': 'Off White D'},
                        ],
                    }
                ),
            )
        ]
        store = DuckDBFeatureStore()
        store.replace_chain_rows(
            'ethereum',
            [
                mod.DatabaseNFTRecord(
                    contract_address='0xdup-strong',
                    token_id='2',
                    token_uri='ipfs://seed/meta-1',
                    image_uri='ipfs://other/image.png',
                    name='Azuki Mirror #2',
                    symbol='AZUKI',
                    metadata_json='',
                ),
                mod.DatabaseNFTRecord(
                    contract_address='0xdup-meta',
                    token_id='3',
                    token_uri='ipfs://other/meta-3',
                    image_uri='ipfs://other/image-3.png',
                    name='Anime Portrait Copy #3',
                    symbol='OTHER',
                    metadata_json=json.dumps(
                        {
                            'name': 'Anime Portrait Copy #3',
                            'description': 'Anime portrait wearing a maroon kimono on an off white background.',
                            'attributes': [
                                {'trait_type': 'Background', 'value': 'Off White D'},
                            ],
                        }
                    ),
                ),
            ],
        )

        snapshot = store.load_snapshot('ethereum', seed_nfts=seed_nfts)
        candidates = mod.find_duplicate_candidates(
            seed_nfts,
            snapshot,
            name_threshold=95.0,
            metadata_threshold=0.55,
        )

        by_contract = {item.contract_address: item for item in candidates}
        self.assertIn('0xdup-strong', by_contract)
        self.assertIn('token_uri_match', by_contract['0xdup-strong'].match_reasons)
        self.assertIn('0xdup-meta', by_contract)
        self.assertIn('metadata_match', by_contract['0xdup-meta'].match_reasons)
        self.assertEqual(by_contract['0xdup-meta'].confidence, 'high')

    def test_duckdb_feature_store_two_step_query_keeps_chain_filter(self):
        seed_nfts = [
            mod.SeedNFT(
                chain='ethereum',
                contract_address='0xseed',
                token_id='1',
                name='Azuki #1',
                symbol='AZUKI',
                token_uri='ipfs://seed/meta-1',
                image_uri='',
                metadata_json='',
            )
        ]
        store = DuckDBFeatureStore()
        store.replace_chain_rows(
            'ethereum',
            [
                mod.DatabaseNFTRecord(
                    contract_address='0xdup',
                    token_id='1',
                    token_uri='ipfs://seed/meta-1',
                    image_uri='',
                    name='Eth Clone #1',
                    symbol='AZUKI',
                    metadata_json='',
                ),
            ],
        )
        store.replace_chain_rows(
            'base',
            [
                mod.DatabaseNFTRecord(
                    contract_address='0xdup',
                    token_id='999',
                    token_uri='ipfs://base/meta-999',
                    image_uri='',
                    name='Base Clone #999',
                    symbol='BASE',
                    metadata_json='',
                ),
            ],
        )

        snapshot = store.load_snapshot(
            'ethereum',
            seed_nfts=seed_nfts,
            max_tokens_per_contract=500,
        )

        self.assertEqual(
            [(row.contract_address, row.token_id, row.name) for row in snapshot.nft_rows],
            [('0xdup', '1', 'Eth Clone #1')],
        )
        self.assertEqual(snapshot.contract_signals['0xdup'].token_count, 1)
        self.assertTrue(snapshot.contract_signals['0xdup'].symbol_match)

    def test_duckdb_feature_store_contract_signals_follow_max_recall_rows_window(self):
        seed_nfts = [
            mod.SeedNFT(
                chain='ethereum',
                contract_address='0xseed',
                token_id='1',
                name='Azuki #1',
                symbol='AZUKI',
                token_uri='ipfs://seed/meta-1',
                image_uri='',
                metadata_json='',
            )
        ]
        store = DuckDBFeatureStore()
        store.replace_chain_rows(
            'ethereum',
            [
                mod.DatabaseNFTRecord(
                    contract_address='0xa',
                    token_id='1',
                    token_uri='ipfs://seed/meta-1',
                    image_uri='',
                    name='Clone A #1',
                    symbol='AZUKI',
                    metadata_json='',
                ),
                mod.DatabaseNFTRecord(
                    contract_address='0xb',
                    token_id='1',
                    token_uri='ipfs://seed/meta-1',
                    image_uri='',
                    name='Clone B #1',
                    symbol='AZUKI',
                    metadata_json='',
                ),
            ],
        )

        snapshot = store.load_snapshot(
            'ethereum',
            seed_nfts=seed_nfts,
            max_tokens_per_contract=500,
            max_recall_rows=1,
        )

        self.assertEqual([(row.contract_address, row.token_id) for row in snapshot.nft_rows], [('0xa', '1')])
        self.assertEqual(sorted(snapshot.contract_signals), ['0xa'])
        self.assertEqual(snapshot.contract_signals['0xa'].token_count, 1)

    def test_duckdb_feature_store_loads_rows_from_parquet(self):
        tmpdir = Path('D:/code/solidity/get-nft-data/output/test-top-contract-analysis-parquet')
        tmpdir.mkdir(parents=True, exist_ok=True)
        parquet_path = tmpdir / 'ethereum.parquet'
        try:
            table = pa.table(
                {
                    'chain': ['ethereum'],
                    'contract_address': ['0xdup'],
                    'token_id': ['1'],
                    'token_uri': ['ipfs://seed/meta-1'],
                    'image_uri': ['ipfs://dup/image.png'],
                    'name': ['Azuki Mirror #1'],
                    'symbol': ['AZUKI'],
                    'metadata_json': ['{"description":"red hooded anime portrait"}'],
                    'token_uri_norm': ['ipfs:seed/meta-1'],
                    'image_uri_norm': ['ipfs:dup/image.png'],
                    'name_norm': ['azuki mirror'],
                    'symbol_norm': ['azuki'],
                    'metadata_doc': ['red hooded anime portrait'],
                }
            )
            pq.write_table(table, parquet_path)

            store = DuckDBFeatureStore()
            store.load_parquet_dataset('ethereum', str(parquet_path))
            rows = store._conn.execute(
                'SELECT contract_address, token_uri_norm FROM nft_features WHERE chain = ?',
                ['ethereum'],
            ).fetchall()

            self.assertEqual(rows, [('0xdup', 'ipfs:seed/meta-1')])
        finally:
            for path in sorted(tmpdir.glob('**/*'), reverse=True):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()
            if tmpdir.exists():
                tmpdir.rmdir()

    def test_duckdb_feature_store_loads_precomputed_feature_parquet_without_metadata_json(self):
        tmpdir = Path('D:/code/solidity/get-nft-data/output/test-top-contract-analysis-feature-parquet')
        tmpdir.mkdir(parents=True, exist_ok=True)
        parquet_path = tmpdir / 'ethereum_features.parquet'
        try:
            table = pa.table(
                {
                    'chain': ['ethereum'],
                    'contract_address': ['0xdup'],
                    'token_id': ['1'],
                    'token_uri': ['ipfs://seed/meta-1'],
                    'image_uri': ['ipfs://dup/image.png'],
                    'name': ['Azuki Mirror #1'],
                    'symbol': ['AZUKI'],
                    'token_uri_norm': ['ipfs:seed/meta-1'],
                    'image_uri_norm': ['ipfs:dup/image.png'],
                    'name_norm': ['azuki mirror'],
                    'symbol_norm': ['azuki'],
                    'metadata_doc': ['red hooded anime portrait'],
                }
            )
            pq.write_table(table, parquet_path)

            store = DuckDBFeatureStore()
            store.load_parquet_dataset('ethereum', str(parquet_path))
            rows = store._conn.execute(
                'SELECT contract_address, metadata_json, metadata_doc FROM nft_features WHERE chain = ?',
                ['ethereum'],
            ).fetchall()

            self.assertEqual(rows, [('0xdup', '', 'red hooded anime portrait')])
        finally:
            for path in sorted(tmpdir.glob('**/*'), reverse=True):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()
            if tmpdir.exists():
                tmpdir.rmdir()

    def test_duckdb_feature_store_can_open_prebuilt_database_without_parquet_reload(self):
        tmpdir = Path('D:/code/solidity/get-nft-data/output/test-top-contract-analysis-feature-db')
        tmpdir.mkdir(parents=True, exist_ok=True)
        db_path = tmpdir / 'features.duckdb'
        try:
            writer_store = DuckDBFeatureStore(database_path=str(db_path))
            writer_store.replace_chain_rows(
                'ethereum',
                [
                    mod.DatabaseNFTRecord(
                        contract_address='0xdup',
                        token_id='1',
                        token_uri='ipfs://seed/meta-1',
                        image_uri='ipfs://dup/image.png',
                        name='Azuki Mirror #1',
                        symbol='AZUKI',
                        metadata_json='{"description":"red hooded anime portrait"}',
                    )
                ],
            )
            writer_store.close()

            reader_store = DuckDBFeatureStore(database_path=str(db_path))
            rows = reader_store._conn.execute(
                'SELECT contract_address, token_uri_norm, metadata_doc FROM nft_features WHERE chain = ?',
                ['ethereum'],
            ).fetchall()
            self.assertEqual(rows, [('0xdup', 'ipfs:seed/meta-1', 'red hooded anime portrait')])
        finally:
            for path in sorted(tmpdir.glob('**/*'), reverse=True):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()
            if tmpdir.exists():
                tmpdir.rmdir()

    def test_analyze_seed_contract_uses_feature_store_snapshot(self):
        seed_meta = mod.ContractMetadata(
            chain='ethereum',
            contract_address='0xseed',
            token_type='ERC721',
            contract_deployer='0xcreator',
            deployed_block_number=123,
            name='Azuki',
            symbol='AZUKI',
        )
        seed_nfts = [
            mod.SeedNFT(
                chain='ethereum',
                contract_address='0xseed',
                token_id='1',
                name='Azuki #1',
                symbol='AZUKI',
                token_uri='ipfs://seed/meta-1',
                image_uri='ipfs://seed/image-1.png',
                metadata_json=json.dumps({'description': 'red hooded anime portrait'}),
            )
        ]
        store = DuckDBFeatureStore()
        store.replace_chain_rows(
            'ethereum',
            [
                mod.DatabaseNFTRecord(
                    contract_address='0xdup',
                    token_id='1',
                    token_uri='ipfs://seed/meta-1',
                    image_uri='ipfs://dup/image.png',
                    name='Azuki Mirror #1',
                    symbol='AZUKI',
                    metadata_json='',
                )
            ],
        )
        transfers = [
            mod.TransferRecord(
                contract_address='0xdup',
                token_id='1',
                tx_hash='0xmint',
                log_index=0,
                block_number=10,
                block_time=100,
                from_address=mod.ZERO_ADDRESS,
                to_address='0xsybil',
                event_type='erc721',
                source='alchemy',
            )
        ]
        owners = [mod.OwnerBalance(owner_address='0xsybil', token_balances={'1': 1})]

        with patch.object(mod, 'fetch_contract_metadata', return_value=seed_meta), \
             patch.object(mod, 'fetch_seed_contract_nfts', return_value=seed_nfts), \
             patch.object(mod, 'fetch_license_sample', return_value={'raw': {'metadata': {'license': 'All rights reserved'}}}), \
             patch.object(mod, 'fetch_contract_transfers', return_value=transfers), \
             patch.object(mod, 'fetch_contract_owners', return_value=owners):
            result = mod.analyze_seed_contract(
                chain='ethereum',
                seed_contract_address='0xseed',
                alchemy_api_key='alchemy',
                alchemy_network='eth-mainnet',
                etherscan_api_key='etherscan',
                conn=object(),
                feature_store=store,
            )

        self.assertEqual(result['report_summary']['high_confidence_contract_count'], 1)
        self.assertEqual(result['duplicate_candidates'][0]['contract_address'], '0xdup')

    def test_analyze_seed_contract_does_not_open_pg_when_feature_store_is_provided(self):
        seed_meta = mod.ContractMetadata(
            chain='ethereum',
            contract_address='0xseed',
            token_type='ERC721',
            contract_deployer='0xcreator',
            deployed_block_number=123,
            name='Azuki',
            symbol='AZUKI',
        )
        seed_nfts = [
            mod.SeedNFT(
                chain='ethereum',
                contract_address='0xseed',
                token_id='1',
                name='Azuki #1',
                symbol='AZUKI',
                token_uri='ipfs://seed/meta-1',
                image_uri='ipfs://seed/image-1.png',
                metadata_json='',
            )
        ]
        store = DuckDBFeatureStore()
        store.replace_chain_rows('ethereum', [])

        with patch.object(mod, 'get_conn', side_effect=AssertionError('unexpected pg connection')), \
             patch.object(mod, 'fetch_contract_metadata', return_value=seed_meta), \
             patch.object(mod, 'fetch_seed_contract_nfts', return_value=seed_nfts), \
             patch.object(mod, 'fetch_license_sample', return_value={}), \
             patch.object(mod, 'fetch_contract_transfers', return_value=[]), \
             patch.object(mod, 'fetch_contract_owners', return_value=[]):
            result = mod.analyze_seed_contract(
                chain='ethereum',
                seed_contract_address='0xseed',
                alchemy_api_key='alchemy',
                alchemy_network='eth-mainnet',
                etherscan_api_key='etherscan',
                feature_store=store,
            )

        self.assertEqual(result['report_summary']['candidate_contract_count'], 0)

    def test_analyze_seed_contract_reuses_one_session_and_parallelizes_high_confidence_fetches(self):
        seed_meta = mod.ContractMetadata(
            chain='ethereum',
            contract_address='0xseed',
            token_type='ERC721',
            contract_deployer='0xcreator',
            deployed_block_number=123,
            name='Azuki',
            symbol='AZUKI',
        )
        seed_nfts = [
            mod.SeedNFT(
                chain='ethereum',
                contract_address='0xseed',
                token_id='1',
                name='Azuki #1',
                symbol='AZUKI',
                token_uri='ipfs://seed/meta-1',
                image_uri='ipfs://seed/image-1.png',
                metadata_json='',
            )
        ]
        store = DuckDBFeatureStore()
        store.replace_chain_rows(
            'ethereum',
            [
                mod.DatabaseNFTRecord(
                    contract_address='0xdup-a',
                    token_id='1',
                    token_uri='ipfs://seed/meta-1',
                    image_uri='ipfs://dup-a/image.png',
                    name='Azuki Mirror #1',
                    symbol='AZUKI',
                    metadata_json='',
                ),
                mod.DatabaseNFTRecord(
                    contract_address='0xdup-b',
                    token_id='2',
                    token_uri='ipfs://seed/meta-1',
                    image_uri='ipfs://dup-b/image.png',
                    name='Azuki Mirror #2',
                    symbol='AZUKI',
                    metadata_json='',
                ),
            ],
        )

        transfers = [
            mod.TransferRecord(
                contract_address='0xdup-a',
                token_id='1',
                tx_hash='0xmint',
                log_index=0,
                block_number=10,
                block_time=100,
                from_address=mod.ZERO_ADDRESS,
                to_address='0xminted',
                event_type='erc721',
                source='alchemy',
            )
        ]
        owners = [mod.OwnerBalance(owner_address='0xbuyer', token_balances={'1': 1})]
        seen_sessions = []
        transfer_threads = []

        def fake_contract_metadata(**kwargs):
            seen_sessions.append(kwargs['session'])
            return seed_meta

        def fake_seed_nfts(**kwargs):
            seen_sessions.append(kwargs['session'])
            return seed_nfts

        def fake_license(**kwargs):
            seen_sessions.append(kwargs['session'])
            return {}

        def fake_transfers(**kwargs):
            seen_sessions.append(kwargs['session'])
            transfer_threads.append(threading.get_ident())
            time.sleep(0.15)
            contract_address = kwargs['contract_address']
            return [
                mod.TransferRecord(
                    contract_address=contract_address,
                    token_id='1',
                    tx_hash='0xmint',
                    log_index=0,
                    block_number=10,
                    block_time=100,
                    from_address=mod.ZERO_ADDRESS,
                    to_address='0xminted',
                    event_type='erc721',
                    source='alchemy',
                )
            ]

        def fake_owners(**kwargs):
            seen_sessions.append(kwargs['session'])
            time.sleep(0.15)
            return owners

        with patch.object(mod, 'fetch_contract_metadata', side_effect=fake_contract_metadata), \
             patch.object(mod, 'fetch_seed_contract_nfts', side_effect=fake_seed_nfts), \
             patch.object(mod, 'fetch_license_sample', side_effect=fake_license), \
             patch.object(mod, 'fetch_contract_transfers', side_effect=fake_transfers), \
             patch.object(mod, 'fetch_contract_owners', side_effect=fake_owners):
            started = time.perf_counter()
            result = mod.analyze_seed_contract(
                chain='ethereum',
                seed_contract_address='0xseed',
                alchemy_api_key='alchemy',
                alchemy_network='eth-mainnet',
                etherscan_api_key='etherscan',
                conn=object(),
                feature_store=store,
                timeout=1,
            )
            elapsed = time.perf_counter() - started

        self.assertEqual(result['report_summary']['high_confidence_contract_count'], 2)
        self.assertTrue(seen_sessions)
        self.assertEqual(len({id(item) for item in seen_sessions}), 1)
        self.assertLess(elapsed, 0.5)
        self.assertGreater(len(set(transfer_threads)), 1)

    def test_find_duplicate_candidates_blocks_irrelevant_metadata_pairs_before_scoring(self):
        seed_nfts = [
            mod.SeedNFT(
                chain='ethereum',
                contract_address='0xseed',
                token_id='1',
                name='Azuki #1',
                symbol='AZUKI',
                token_uri='',
                image_uri='',
                metadata_json='',
                metadata_doc='red hooded anime portrait maroon kimono',
            )
        ]
        snapshot = mod.DatabaseSnapshot(
            nft_rows=[
                mod.DatabaseNFTRecord(
                    contract_address='0xsimilar',
                    token_id='1',
                    token_uri='',
                    image_uri='',
                    name='Similar',
                    symbol='OTHER',
                    metadata_json='',
                    metadata_doc='anime portrait maroon kimono copy',
                ),
                mod.DatabaseNFTRecord(
                    contract_address='0xirrelevant',
                    token_id='2',
                    token_uri='',
                    image_uri='',
                    name='Irrelevant',
                    symbol='OTHER',
                    metadata_json='',
                    metadata_doc='ocean whale blue water reef',
                ),
            ]
        )

        with patch.object(rust_bridge, 'score_metadata_pairs', return_value=[0.8]) as score_mock:
            candidates = mod.find_duplicate_candidates(
                seed_nfts,
                snapshot,
                metadata_threshold=0.55,
            )

        self.assertEqual(score_mock.call_count, 1)
        left_docs, right_docs = score_mock.call_args.args
        self.assertEqual(len(left_docs), 1)
        self.assertEqual(right_docs, ['anime portrait maroon kimono copy'])
        self.assertEqual([item.contract_address for item in candidates], ['0xsimilar'])

    def test_analyze_seed_contract_reuses_signal_cache_across_runs(self):
        seed_meta = mod.ContractMetadata(
            chain='ethereum',
            contract_address='0xseed',
            token_type='ERC721',
            contract_deployer='0xcreator',
            deployed_block_number=123,
            name='Azuki',
            symbol='AZUKI',
        )
        seed_nfts = [
            mod.SeedNFT(
                chain='ethereum',
                contract_address='0xseed',
                token_id='1',
                name='Azuki #1',
                symbol='AZUKI',
                token_uri='ipfs://seed/meta-1',
                image_uri='ipfs://seed/image-1.png',
                metadata_json='',
            )
        ]
        store = DuckDBFeatureStore()
        store.replace_chain_rows(
            'ethereum',
            [
                mod.DatabaseNFTRecord(
                    contract_address='0xdup',
                    token_id='1',
                    token_uri='ipfs://seed/meta-1',
                    image_uri='ipfs://dup/image.png',
                    name='Azuki Mirror #1',
                    symbol='AZUKI',
                    metadata_json='',
                )
            ],
        )
        transfers = [
            mod.TransferRecord(
                contract_address='0xdup',
                token_id='1',
                tx_hash='0xmint',
                log_index=0,
                block_number=10,
                block_time=100,
                from_address=mod.ZERO_ADDRESS,
                to_address='0xbuyer',
                event_type='erc721',
                source='alchemy',
            )
        ]
        owners = [mod.OwnerBalance(owner_address='0xbuyer', token_balances={'1': 1})]
        cache = ContractSignalCache()

        with patch.object(mod, 'fetch_contract_metadata', return_value=seed_meta), \
             patch.object(mod, 'fetch_seed_contract_nfts', return_value=seed_nfts), \
             patch.object(mod, 'fetch_license_sample', return_value={'raw': {'metadata': {'license': 'All rights reserved'}}}), \
             patch.object(mod, 'fetch_contract_transfers', return_value=transfers) as transfer_mock, \
             patch.object(mod, 'fetch_contract_owners', return_value=owners) as owners_mock:
            mod.analyze_seed_contract(
                chain='ethereum',
                seed_contract_address='0xseed',
                alchemy_api_key='alchemy',
                alchemy_network='eth-mainnet',
                etherscan_api_key='etherscan',
                conn=object(),
                feature_store=store,
                signal_cache=cache,
            )
            mod.analyze_seed_contract(
                chain='ethereum',
                seed_contract_address='0xseed',
                alchemy_api_key='alchemy',
                alchemy_network='eth-mainnet',
                etherscan_api_key='etherscan',
                conn=object(),
                feature_store=store,
                signal_cache=cache,
            )

        self.assertEqual(transfer_mock.call_count, 1)
        self.assertEqual(owners_mock.call_count, 1)

    def test_signal_cache_reuses_compact_transfer_cache_and_lazily_fetches_owners(self):
        seed_meta_legit = mod.ContractMetadata(
            chain='ethereum',
            contract_address='0xseed',
            token_type='ERC721',
            contract_deployer='0xcreator',
            deployed_block_number=123,
            name='Azuki',
            symbol='AZUKI',
        )
        seed_meta_non_legit = mod.ContractMetadata(
            chain='ethereum',
            contract_address='0xseed-two',
            token_type='ERC721',
            contract_deployer='0xothercreator',
            deployed_block_number=124,
            name='BEANZ',
            symbol='BEANZ',
        )
        seed_nfts = [
            mod.SeedNFT(
                chain='ethereum',
                contract_address='0xseed',
                token_id='1',
                name='Azuki #1',
                symbol='AZUKI',
                token_uri='ipfs://seed/meta-1',
                image_uri='ipfs://seed/image-1.png',
                metadata_json='',
            )
        ]
        store = DuckDBFeatureStore()
        store.replace_chain_rows(
            'ethereum',
            [
                mod.DatabaseNFTRecord(
                    contract_address='0xdup',
                    token_id='1',
                    token_uri='ipfs://seed/meta-1',
                    image_uri='ipfs://dup/image.png',
                    name='Azuki Mirror #1',
                    symbol='AZUKI',
                    metadata_json='',
                )
            ],
        )
        transfers = [
            mod.TransferRecord(
                contract_address='0xdup',
                token_id='1',
                tx_hash='0xmint',
                log_index=0,
                block_number=10,
                block_time=100,
                from_address=mod.ZERO_ADDRESS,
                to_address='0xcreator',
                event_type='erc721',
                source='alchemy',
            )
        ]
        owners = [mod.OwnerBalance(owner_address='0xbuyer', token_balances={'1': 1})]
        cache = ContractSignalCache()

        with patch.object(mod, 'fetch_contract_metadata', side_effect=[seed_meta_legit, seed_meta_non_legit]), \
             patch.object(mod, 'fetch_seed_contract_nfts', return_value=seed_nfts), \
             patch.object(mod, 'fetch_license_sample', return_value={'raw': {'metadata': {'license': 'All rights reserved'}}}), \
             patch.object(mod, 'fetch_contract_transfers', return_value=transfers) as transfer_mock, \
             patch.object(mod, 'fetch_contract_owners', return_value=owners) as owners_mock:
            legit_result = mod.analyze_seed_contract(
                chain='ethereum',
                seed_contract_address='0xseed',
                alchemy_api_key='alchemy',
                alchemy_network='eth-mainnet',
                etherscan_api_key='etherscan',
                conn=object(),
                feature_store=store,
                signal_cache=cache,
            )
            non_legit_result = mod.analyze_seed_contract(
                chain='ethereum',
                seed_contract_address='0xseed-two',
                alchemy_api_key='alchemy',
                alchemy_network='eth-mainnet',
                etherscan_api_key='etherscan',
                conn=object(),
                feature_store=store,
                signal_cache=cache,
            )

        self.assertEqual(transfer_mock.call_count, 1)
        self.assertEqual(owners_mock.call_count, 1)
        self.assertEqual(legit_result['report_summary']['legit_duplicate_contract_count'], 1)
        self.assertEqual(non_legit_result['report_summary']['high_confidence_contract_count'], 1)


class BatchEntryTests(unittest.TestCase):
    def test_batch_main_reads_seed_file_and_writes_outputs(self):
        tmpdir = Path('D:/code/solidity/get-nft-data/output/test-top-contract-analysis-batch')
        tmpdir.mkdir(parents=True, exist_ok=True)
        seeds_path = tmpdir / 'seeds.txt'
        seeds_path.write_text('0xseed1\n0xseed2\n', encoding='utf-8')
        output_dir = tmpdir / 'results'
        payload = {
            'seed_contract': {
                'chain': 'ethereum',
                'contract_address': '0xseed1',
                'token_type': 'ERC721',
                'contract_deployer': '0xcreator',
                'deployed_block_number': 1,
                'name': 'Azuki',
                'symbol': 'AZUKI',
            },
            'seed_collection_stats': {
                'seed_nft_count': 1,
                'unique_token_uri_count': 1,
                'unique_image_uri_count': 1,
                'unique_name_count': 1,
                'unique_symbol_count': 1,
            },
            'duplicate_candidates': [],
            'legit_duplicates': [],
            'suspected_infringing_duplicates_high_confidence': [],
            'suspected_infringing_duplicates_low_confidence': [],
            'contract_level_summary': {},
            'address_signals': {},
            'victim_signals': {},
            'report_summary': {
                'open_license_detected': False,
                'candidate_contract_count': 0,
                'high_confidence_contract_count': 0,
                'low_confidence_contract_count': 0,
                'legit_duplicate_contract_count': 0,
            },
        }
        side_effect = [
            payload,
            {**payload, 'seed_contract': {**payload['seed_contract'], 'contract_address': '0xseed2', 'name': 'BEANZ'}},
        ]
        try:
            with patch.object(batch_mod, 'analyze_seed_contract', side_effect=side_effect):
                code = batch_mod.main(
                    [
                        '--chain', 'ethereum',
                        '--seed-file', str(seeds_path),
                        '--alchemy-api-key', 'alchemy',
                        '--output-dir', str(output_dir),
                        '--workers', '2',
                    ]
                )

            self.assertEqual(code, 0)
            self.assertTrue((output_dir / 'top_contract_analysis__azuki.json').exists())
            self.assertTrue((output_dir / 'top_contract_analysis__beanz.json').exists())
            summary_json = output_dir / 'top_contract_analysis__summary.json'
            summary_md = output_dir / 'top_contract_analysis__summary.md'
            self.assertTrue(summary_json.exists())
            self.assertTrue(summary_md.exists())
            summary_payload = json.loads(summary_json.read_text(encoding='utf-8'))
            self.assertEqual(summary_payload['batch_summary']['seed_report_count'], 2)
            self.assertEqual(len(summary_payload['seed_reports']), 2)
            self.assertEqual(
                [item['seed_contract']['contract_address'] for item in summary_payload['seed_reports']],
                ['0xseed1', '0xseed2'],
            )
            self.assertNotIn('duplicate_candidates', summary_payload['seed_reports'][0])
        finally:
            for path in sorted(tmpdir.glob('**/*'), reverse=True):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()
            if tmpdir.exists():
                tmpdir.rmdir()

    def test_batch_main_forwards_max_tokens_per_contract(self):
        tmpdir = Path('D:/code/solidity/get-nft-data/output/test-top-contract-analysis-batch-max-tokens')
        tmpdir.mkdir(parents=True, exist_ok=True)
        seeds_path = tmpdir / 'seeds.txt'
        seeds_path.write_text('0xseed1\n', encoding='utf-8')
        payload = {
            'seed_contract': {
                'chain': 'ethereum',
                'contract_address': '0xseed1',
                'token_type': 'ERC721',
                'contract_deployer': '0xcreator',
                'deployed_block_number': 1,
                'name': 'Azuki',
                'symbol': 'AZUKI',
            },
            'seed_collection_stats': {
                'seed_nft_count': 1,
                'unique_token_uri_count': 1,
                'unique_image_uri_count': 1,
                'unique_name_count': 1,
                'unique_symbol_count': 1,
            },
            'duplicate_candidates': [],
            'legit_duplicates': [],
            'suspected_infringing_duplicates_high_confidence': [],
            'suspected_infringing_duplicates_low_confidence': [],
            'contract_level_summary': {},
            'address_signals': {},
            'victim_signals': {},
            'report_summary': {
                'open_license_detected': False,
                'candidate_contract_count': 0,
                'high_confidence_contract_count': 0,
                'low_confidence_contract_count': 0,
                'legit_duplicate_contract_count': 0,
            },
        }
        try:
            with patch.object(batch_mod, 'analyze_seed_contract', return_value=payload) as analyze_mock:
                code = batch_mod.main(
                    [
                        '--chain', 'ethereum',
                        '--seed-file', str(seeds_path),
                        '--alchemy-api-key', 'alchemy',
                        '--output-dir', str(tmpdir / 'results'),
                        '--max-tokens-per-contract', '123',
                    ]
                )

            self.assertEqual(code, 0)
            self.assertEqual(analyze_mock.call_args.kwargs['max_tokens_per_contract'], 123)
        finally:
            for path in sorted(tmpdir.glob('**/*'), reverse=True):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()
            if tmpdir.exists():
                tmpdir.rmdir()

    def test_batch_main_reuses_feature_store_loaded_from_parquet(self):
        tmpdir = Path('D:/code/solidity/get-nft-data/output/test-top-contract-analysis-batch-feature-store')
        tmpdir.mkdir(parents=True, exist_ok=True)
        seeds_path = tmpdir / 'seeds.txt'
        seeds_path.write_text('0xseed1\n0xseed2\n', encoding='utf-8')
        parquet_path = tmpdir / 'ethereum.parquet'
        pq.write_table(
            pa.table(
                {
                    'chain': ['ethereum'],
                    'contract_address': ['0xdup'],
                    'token_id': ['1'],
                    'token_uri': ['ipfs://seed/meta-1'],
                    'image_uri': ['ipfs://dup/image.png'],
                    'name': ['Azuki Mirror #1'],
                    'symbol': ['AZUKI'],
                    'metadata_json': ['{"description":"red hooded anime portrait"}'],
                }
            ),
            parquet_path,
        )
        output_dir = tmpdir / 'results'
        payload = {
            'seed_contract': {
                'chain': 'ethereum',
                'contract_address': '0xseed1',
                'token_type': 'ERC721',
                'contract_deployer': '0xcreator',
                'deployed_block_number': 1,
                'name': 'Azuki',
                'symbol': 'AZUKI',
            },
            'seed_collection_stats': {
                'seed_nft_count': 1,
                'unique_token_uri_count': 1,
                'unique_image_uri_count': 1,
                'unique_name_count': 1,
                'unique_symbol_count': 1,
            },
            'duplicate_candidates': [],
            'legit_duplicates': [],
            'suspected_infringing_duplicates_high_confidence': [],
            'suspected_infringing_duplicates_low_confidence': [],
            'contract_level_summary': {},
            'address_signals': {},
            'victim_signals': {},
            'report_summary': {
                'open_license_detected': False,
                'candidate_contract_count': 0,
                'high_confidence_contract_count': 0,
                'low_confidence_contract_count': 0,
                'legit_duplicate_contract_count': 0,
            },
        }
        payloads = [
            payload,
            {**payload, 'seed_contract': {**payload['seed_contract'], 'contract_address': '0xseed2', 'name': 'BEANZ'}},
        ]
        try:
            with patch.object(batch_mod, 'analyze_seed_contract', side_effect=payloads) as analyze_mock, \
                 patch.object(batch_mod.DuckDBFeatureStore, 'close', autospec=True) as close_mock:
                code = batch_mod.main(
                    [
                        '--chain', 'ethereum',
                        '--seed-file', str(seeds_path),
                        '--alchemy-api-key', 'alchemy',
                        '--output-dir', str(output_dir),
                        '--feature-parquet', str(parquet_path),
                    ]
                )

            self.assertEqual(code, 0)
            self.assertEqual(analyze_mock.call_count, 2)
            first_store = analyze_mock.call_args_list[0].kwargs['feature_store']
            second_store = analyze_mock.call_args_list[1].kwargs['feature_store']
            self.assertIs(first_store, second_store)
            self.assertEqual(close_mock.call_count, 1)
            loaded_rows = first_store._conn.execute(
                'SELECT count(*) FROM nft_features WHERE chain = ?',
                ['ethereum'],
            ).fetchone()[0]
            self.assertEqual(loaded_rows, 1)
        finally:
            for path in sorted(tmpdir.glob('**/*'), reverse=True):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()
            if tmpdir.exists():
                tmpdir.rmdir()


class SnapshotExportTests(unittest.TestCase):
    def test_export_chain_snapshot_writes_expected_parquet_columns(self):
        tmpdir = Path('D:/code/solidity/get-nft-data/output/test-top-contract-analysis-export')
        tmpdir.mkdir(parents=True, exist_ok=True)
        output_path = tmpdir / 'ethereum.parquet'

        class FakeCursor:
            def __init__(self, rows):
                self._rows = rows
                self.itersize = 0

            def execute(self, query, params=None):
                self.query = query

            def fetchone(self):
                return None

            def fetchmany(self, fetch_size):
                if not self._rows:
                    return []
                batch, self._rows = self._rows[:fetch_size], self._rows[fetch_size:]
                return batch

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeConn:
            def __init__(self, rows):
                self.rows = rows

            def cursor(self, name=None):
                return FakeCursor(list(self.rows))

        rows = [
            ('0xdup', '1', 'ipfs://seed/meta-1', 'ipfs://dup/image.png', 'Azuki Mirror #1', 'AZUKI', '{"description":"x"}'),
            ('0xdup', '2', '', '', 'Azuki Mirror #2', 'AZUKI', ''),
        ]

        try:
            export_mod.export_chain_snapshot_to_parquet(
                FakeConn(rows),
                chain='ethereum',
                output_path=output_path,
                fetch_size=1,
            )

            table = pq.read_table(output_path)
            self.assertEqual(
                table.column_names,
                [
                    'chain', 'contract_address', 'token_id', 'token_uri', 'image_uri', 'name', 'symbol',
                    'token_uri_norm', 'image_uri_norm', 'name_norm', 'symbol_norm', 'metadata_doc',
                    'metadata_keywords_arr',
                ],
            )
            self.assertEqual(table.num_rows, 2)
            self.assertEqual(table['contract_address'].to_pylist(), ['0xdup', '0xdup'])
        finally:
            for path in sorted(tmpdir.glob('**/*'), reverse=True):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()
            if tmpdir.exists():
                tmpdir.rmdir()

    def test_export_chain_snapshot_prefers_real_metadata_column_when_available(self):
        tmpdir = Path('D:/code/solidity/get-nft-data/output/test-top-contract-analysis-export-metadata')
        tmpdir.mkdir(parents=True, exist_ok=True)
        output_path = tmpdir / 'ethereum.parquet'

        class FakeCursor:
            def __init__(self, rows):
                self._rows = rows
                self.itersize = 0
                self.executed = []

            def execute(self, query, params=None):
                self.executed.append((query, params))

            def fetchone(self):
                return ('raw_metadata',)

            def fetchmany(self, fetch_size):
                if not self._rows:
                    return []
                batch, self._rows = self._rows[:fetch_size], self._rows[fetch_size:]
                return batch

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeConn:
            def __init__(self, rows):
                self.rows = rows
                self.cursors = []

            def cursor(self, name=None):
                rows = list(self.rows) if name else []
                cursor = FakeCursor(rows)
                self.cursors.append(cursor)
                return cursor

        rows = [
            ('0xdup', '1', 'ipfs://seed/meta-1', 'ipfs://dup/image.png', 'Azuki Mirror #1', 'AZUKI', '{"description":"x"}'),
        ]

        try:
            conn = FakeConn(rows)
            export_mod.export_chain_snapshot_to_parquet(
                conn,
                chain='ethereum',
                output_path=output_path,
                fetch_size=1,
            )

            table = pq.read_table(output_path)
            self.assertEqual(table['metadata_doc'].to_pylist(), ['x'])
            schema_query = conn.cursors[0].executed[0][0]
            data_query = conn.cursors[1].executed[0][0]
            self.assertIn('information_schema.columns', schema_query)
            self.assertIn('raw_metadata', data_query)
        finally:
            for path in sorted(tmpdir.glob('**/*'), reverse=True):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()
            if tmpdir.exists():
                tmpdir.rmdir()

    def test_export_chain_snapshot_can_keep_raw_metadata_json_when_requested(self):
        tmpdir = Path('D:/code/solidity/get-nft-data/output/test-top-contract-analysis-export-keep-metadata')
        tmpdir.mkdir(parents=True, exist_ok=True)
        output_path = tmpdir / 'ethereum.parquet'

        class FakeCursor:
            def __init__(self, rows):
                self._rows = rows
                self.itersize = 0

            def execute(self, query, params=None):
                self.query = query

            def fetchone(self):
                return ('raw_metadata',)

            def fetchmany(self, fetch_size):
                if not self._rows:
                    return []
                batch, self._rows = self._rows[:fetch_size], self._rows[fetch_size:]
                return batch

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeConn:
            def __init__(self, rows):
                self.rows = rows

            def cursor(self, name=None):
                return FakeCursor(list(self.rows) if name else [])

        rows = [
            ('0xdup', '1', 'ipfs://seed/meta-1', 'ipfs://dup/image.png', 'Azuki Mirror #1', 'AZUKI', '{"description":"x"}'),
        ]

        try:
            export_mod.export_chain_snapshot_to_parquet(
                FakeConn(rows),
                chain='ethereum',
                output_path=output_path,
                fetch_size=1,
                keep_metadata_json=True,
            )

            table = pq.read_table(output_path)
            self.assertIn('metadata_json', table.column_names)
            self.assertEqual(table['metadata_json'].to_pylist(), ['{"description":"x"}'])
        finally:
            for path in sorted(tmpdir.glob('**/*'), reverse=True):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()
            if tmpdir.exists():
                tmpdir.rmdir()


class SingleEntryOfflineTests(unittest.TestCase):
    def test_main_accepts_feature_store_and_signal_cache_options(self):
        tmpdir = Path('D:/code/solidity/get-nft-data/output/test-top-contract-analysis-single-entry')
        tmpdir.mkdir(parents=True, exist_ok=True)
        parquet_path = tmpdir / 'ethereum.parquet'
        pq.write_table(
            pa.table(
                {
                    'chain': ['ethereum'],
                    'contract_address': ['0xdup'],
                    'token_id': ['1'],
                    'token_uri': ['ipfs://seed/meta-1'],
                    'image_uri': ['ipfs://dup/image.png'],
                    'name': ['Azuki Mirror #1'],
                    'symbol': ['AZUKI'],
                    'metadata_json': [''],
                }
            ),
            parquet_path,
        )
        signal_cache_path = tmpdir / 'signals.duckdb'
        payload = {
            'seed_contract': {
                'chain': 'ethereum',
                'contract_address': '0xseed',
                'token_type': 'ERC721',
                'contract_deployer': '0xcreator',
                'deployed_block_number': 1,
                'name': 'Azuki',
                'symbol': 'AZUKI',
            },
            'seed_collection_stats': {
                'seed_nft_count': 1,
                'unique_token_uri_count': 1,
                'unique_image_uri_count': 1,
                'unique_name_count': 1,
                'unique_symbol_count': 1,
            },
            'duplicate_candidates': [],
            'legit_duplicates': [],
            'suspected_infringing_duplicates_high_confidence': [],
            'suspected_infringing_duplicates_low_confidence': [],
            'contract_level_summary': {},
            'address_signals': {},
            'victim_signals': {},
            'report_summary': {
                'open_license_detected': False,
                'candidate_contract_count': 0,
                'high_confidence_contract_count': 0,
                'low_confidence_contract_count': 0,
                'legit_duplicate_contract_count': 0,
            },
        }
        output_path = tmpdir / 'result.json'
        try:
            with patch.object(mod, 'analyze_seed_contract', return_value=payload) as analyze_mock:
                code = mod.main(
                    [
                        '--chain', 'ethereum',
                        '--seed-contract-address', '0xseed',
                        '--alchemy-api-key', 'alchemy',
                        '--feature-parquet', str(parquet_path),
                        '--signal-cache-db', str(signal_cache_path),
                        '--output', str(output_path),
                    ]
                )

            self.assertEqual(code, 0)
            self.assertEqual(analyze_mock.call_count, 1)
            self.assertIsNotNone(analyze_mock.call_args.kwargs['feature_store'])
            self.assertIsNotNone(analyze_mock.call_args.kwargs['signal_cache'])
            self.assertTrue(output_path.exists())
        finally:
            for path in sorted(tmpdir.glob('**/*'), reverse=True):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()
            if tmpdir.exists():
                tmpdir.rmdir()


class RustBuildEntryTests(unittest.TestCase):
    def test_build_rust_extension_builds_and_installs_to_runtime_dir(self):
        tmpdir = Path('D:/code/solidity/get-nft-data/output/test-top-contract-analysis-rust-build')
        tmpdir.mkdir(parents=True, exist_ok=True)
        runtime_dir = tmpdir / 'pydeps'
        wheel_path = tmpdir / 'top_contract_analysis_rust-0.1.0-cp312-cp312-win_amd64.whl'
        wheel_path.write_bytes(b'placeholder')

        try:
            with patch.object(build_rust_mod, '_build_wheel', return_value=wheel_path) as build_mock, \
                 patch.object(build_rust_mod, '_install_wheel_to_runtime_dir') as install_mock:
                code = build_rust_mod.main(
                    [
                        '--runtime-dir', str(runtime_dir),
                        '--interpreter', 'C:\\Users\\z1766\\.conda\\envs\\codex\\python.exe',
                    ]
                )

            self.assertEqual(code, 0)
            self.assertEqual(build_mock.call_count, 1)
            self.assertEqual(install_mock.call_count, 1)
            self.assertEqual(install_mock.call_args.args[0], wheel_path)
            self.assertEqual(install_mock.call_args.args[1], runtime_dir)
        finally:
            for path in sorted(tmpdir.glob('**/*'), reverse=True):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()
            if tmpdir.exists():
                tmpdir.rmdir()


if __name__ == '__main__':
    unittest.main()
