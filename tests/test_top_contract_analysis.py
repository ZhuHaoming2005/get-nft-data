import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import top_contract_analysis as mod


class FetchSeedContractNFTsTests(unittest.TestCase):
    def test_fetch_seed_contract_nfts_paginates_until_page_key_exhausted(self):
        first_response = Mock()
        first_response.status_code = 200
        first_response.reason = 'OK'
        first_response.text = ''
        first_response.raise_for_status.return_value = None
        first_response.json.return_value = {
            'nfts': [
                {
                    'contract': {'address': '0xseed'},
                    'id': {'tokenId': '0x1'},
                    'title': 'Azuki #1',
                    'contractMetadata': {'symbol': 'AZUKI'},
                    'tokenUri': {'raw': 'ipfs://seed/1'},
                    'image': {'originalUrl': 'ipfs://image/1.png'},
                }
            ],
            'pageKey': 'next-page',
        }
        second_response = Mock()
        second_response.status_code = 200
        second_response.reason = 'OK'
        second_response.text = ''
        second_response.raise_for_status.return_value = None
        second_response.json.return_value = {
            'nfts': [
                {
                    'contract': {'address': '0xseed'},
                    'id': {'tokenId': '0x2'},
                    'title': 'Azuki #2',
                    'contractMetadata': {'symbol': 'AZUKI'},
                    'tokenUri': {'raw': 'ipfs://seed/2'},
                    'image': {'originalUrl': 'ipfs://image/2.png'},
                }
            ],
        }
        fake_requests = SimpleNamespace(get=Mock(side_effect=[first_response, second_response]))

        with patch.object(mod, 'requests', fake_requests):
            rows = mod.fetch_seed_contract_nfts(
                api_key='key',
                network='eth-mainnet',
                chain='ethereum',
                contract_address='0xseed',
            )

        self.assertEqual([row.token_id for row in rows], ['1', '2'])
        second_url = fake_requests.get.call_args_list[1].args[0]
        self.assertIn('pageKey=next-page', second_url)

    def test_fetch_seed_contract_nfts_accepts_string_token_uri_and_image(self):
        response = Mock()
        response.status_code = 200
        response.reason = 'OK'
        response.text = ''
        response.raise_for_status.return_value = None
        response.json.return_value = {
            'nfts': [
                {
                    'contract': {'address': '0xseed'},
                    'id': {'tokenId': '0x1'},
                    'title': 'Azuki #1',
                    'contractMetadata': {'symbol': 'AZUKI'},
                    'tokenUri': 'ipfs://seed/1',
                    'image': 'ipfs://image/1.png',
                }
            ],
        }
        fake_requests = SimpleNamespace(get=Mock(return_value=response))

        with patch.object(mod, 'requests', fake_requests):
            rows = mod.fetch_seed_contract_nfts(
                api_key='key',
                network='eth-mainnet',
                chain='ethereum',
                contract_address='0xseed',
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].token_uri, 'ipfs://seed/1')
        self.assertEqual(rows[0].image_uri, 'ipfs://image/1.png')


class LicenseDetectionTests(unittest.TestCase):
    def test_pick_license_metadata_calls_api_once(self):
        seed_nfts = [
            mod.SeedNFT(
                chain='ethereum',
                contract_address='0xseed',
                token_id='1',
                name='Azuki #1',
                symbol='AZUKI',
                token_uri='ipfs://seed/1',
                image_uri='ipfs://image/1.png',
            ),
            mod.SeedNFT(
                chain='ethereum',
                contract_address='0xseed',
                token_id='2',
                name='Azuki #2',
                symbol='AZUKI',
                token_uri='ipfs://seed/2',
                image_uri='ipfs://image/2.png',
            ),
        ]

        with patch.object(mod, 'fetch_nft_metadata', return_value={'raw': {'metadata': {'license': 'CC0-1.0'}}}) as mocked:
            payload = mod.fetch_license_sample(
                api_key='key',
                network='eth-mainnet',
                chain='ethereum',
                seed_nfts=seed_nfts,
            )

        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(payload['raw']['metadata']['license'], 'CC0-1.0')
        self.assertTrue(mod.is_open_license_payload(payload))


class TransferFallbackTests(unittest.TestCase):
    def test_fetch_alchemy_contract_transfers_retries_before_succeeding(self):
        first_response = Mock()
        first_response.raise_for_status.side_effect = mod.REQUESTS_REQUEST_ERROR('ssl eof')
        second_response = Mock()
        second_response.raise_for_status.return_value = None
        second_response.json.return_value = {
            'result': {
                'transfers': [
                    {
                        'rawContract': {'address': '0xdup'},
                        'erc721TokenId': '0x1',
                        'hash': '0xabc',
                        'logIndex': 0,
                        'blockNum': '0x10',
                        'metadata': {'blockTimestampUnix': 100},
                        'from': mod.ZERO_ADDRESS,
                        'to': '0xbuyer',
                        'category': 'erc721',
                    }
                ]
            }
        }
        fake_requests = SimpleNamespace(post=Mock(side_effect=[first_response, second_response]))

        with patch.object(mod, 'requests', fake_requests):
            rows = mod.fetch_alchemy_contract_transfers(
                api_key='alchemy',
                network='eth-mainnet',
                contract_address='0xdup',
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(fake_requests.post.call_count, 2)

    def test_fetch_contract_transfers_falls_back_to_etherscan_for_erc721(self):
        with patch.object(mod, 'fetch_alchemy_contract_transfers', side_effect=RuntimeError('rate limited')) as alchemy_mock:
            with patch.object(mod, 'fetch_etherscan_contract_transfers', return_value=[mod.TransferRecord(
                contract_address='0xdup',
                token_id='1',
                tx_hash='0xabc',
                log_index=0,
                block_number=1,
                block_time=10,
                from_address=mod.ZERO_ADDRESS,
                to_address='0xbuyer',
                event_type='erc721',
                source='etherscan',
            )]) as etherscan_mock:
                rows = mod.fetch_contract_transfers(
                    alchemy_api_key='alchemy',
                    alchemy_network='eth-mainnet',
                    etherscan_api_key='etherscan',
                    chain='ethereum',
                    contract_address='0xdup',
                    token_type='ERC721',
                )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].source, 'etherscan')
        self.assertEqual(alchemy_mock.call_count, 1)
        self.assertEqual(etherscan_mock.call_count, 1)


class DuplicateAnalysisTests(unittest.TestCase):
    def test_find_duplicate_candidates_marks_high_and_low_confidence(self):
        seed_nfts = [
            mod.SeedNFT(
                chain='ethereum',
                contract_address='0xseed',
                token_id='1',
                name='Azuki #1',
                symbol='AZUKI',
                token_uri='ipfs://seed/meta-1',
                image_uri='ipfs://seed/image-1.png',
            )
        ]
        snapshot = mod.DatabaseSnapshot(
            nft_rows=[
                mod.DatabaseNFTRecord(
                    contract_address='0xseed',
                    token_id='1',
                    token_uri='ipfs://seed/meta-1',
                    image_uri='ipfs://seed/image-1.png',
                    name='Azuki #1',
                    symbol='AZUKI',
                ),
                mod.DatabaseNFTRecord(
                    contract_address='0xdup-high',
                    token_id='2',
                    token_uri='ipfs://seed/meta-1',
                    image_uri='ipfs://other/image.png',
                    name='Azuki Clone #2',
                    symbol='AZUKI',
                ),
                mod.DatabaseNFTRecord(
                    contract_address='0xdup-low',
                    token_id='3',
                    token_uri='ipfs://other/meta-3',
                    image_uri='ipfs://other/image-3.png',
                    name='Azuki #3',
                    symbol='OTHER',
                ),
            ],
            contract_names=[
                mod.ContractNameRecord(contract_address='0xdup-high', name_norm='azuki clone'),
                mod.ContractNameRecord(contract_address='0xdup-low', name_norm='azuki'),
            ],
            symbol_contracts={
                'azuki': {'0xdup-high'},
                'other': {'0xdup-low'},
            },
        )

        candidates = mod.find_duplicate_candidates(seed_nfts, snapshot, name_threshold=95.0)

        self.assertEqual(len(candidates), 2)
        by_contract = {item.contract_address: item for item in candidates}
        self.assertEqual(by_contract['0xdup-high'].confidence, 'high')
        self.assertIn('token_uri_match', by_contract['0xdup-high'].match_reasons)
        self.assertEqual(by_contract['0xdup-low'].confidence, 'low')
        self.assertEqual(by_contract['0xdup-low'].match_reasons, ('name_match',))


class EndToEndAnalysisTests(unittest.TestCase):
    def test_analyze_seed_contract_runs_without_opensea(self):
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
            )
        ]
        snapshot = mod.DatabaseSnapshot(
            nft_rows=[
                mod.DatabaseNFTRecord(
                    contract_address='0xdup',
                    token_id='1',
                    token_uri='ipfs://seed/meta-1',
                    image_uri='ipfs://dup/image.png',
                    name='Azuki Mirror #1',
                    symbol='AZUKI',
                )
            ],
            contract_names=[mod.ContractNameRecord(contract_address='0xdup', name_norm='azuki mirror')],
            symbol_contracts={'azuki': {'0xdup'}},
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
            ),
            mod.TransferRecord(
                contract_address='0xdup',
                token_id='1',
                tx_hash='0xwash1',
                log_index=1,
                block_number=11,
                block_time=110,
                from_address='0xsybil',
                to_address='0xbuyer',
                event_type='erc721',
                source='alchemy',
            ),
        ]
        owners = [
            mod.OwnerBalance(owner_address='0xbuyer', token_balances={'1': 1}),
        ]

        with patch.object(mod, 'fetch_contract_metadata', return_value=seed_meta), \
             patch.object(mod, 'fetch_seed_contract_nfts', return_value=seed_nfts), \
             patch.object(mod, 'fetch_license_sample', return_value={'raw': {'metadata': {'license': 'All rights reserved'}}}), \
             patch.object(mod, 'load_database_snapshot', return_value=snapshot), \
             patch.object(mod, 'fetch_contract_transfers', return_value=transfers), \
             patch.object(mod, 'fetch_contract_owners', return_value=owners):
            result = mod.analyze_seed_contract(
                chain='ethereum',
                seed_contract_address='0xseed',
                alchemy_api_key='alchemy',
                alchemy_network='eth-mainnet',
                etherscan_api_key='etherscan',
                conn=object(),
            )

        self.assertEqual(result['seed_contract']['contract_address'], '0xseed')
        self.assertEqual(result['report_summary']['high_confidence_contract_count'], 1)
        self.assertFalse(result['report_summary']['open_license_detected'])
        self.assertEqual(result['address_signals']['0xdup']['mint_address_count'], 1)


class ParserNetworkDefaultTests(unittest.TestCase):
    def test_parser_does_not_default_alchemy_network_from_env(self):
        with patch.dict('os.environ', {'ALCHEMY_NETWORK': 'polygon-mainnet'}, clear=False):
            parser = mod.build_parser()
            args = parser.parse_args(['--chain', 'ethereum', '--seed-contract-address', '0xseed'])

        self.assertEqual(args.alchemy_network, '')


class HumanReadableReportTests(unittest.TestCase):
    def test_render_human_readable_report_contains_key_sections(self):
        payload = {
            'seed_contract': {
                'chain': 'ethereum',
                'contract_address': '0xseed',
                'token_type': 'ERC721',
                'contract_deployer': '0xcreator',
                'deployed_block_number': 123,
                'name': 'Azuki',
                'symbol': 'AZUKI',
            },
            'seed_collection_stats': {
                'seed_nft_count': 10000,
                'unique_token_uri_count': 10000,
                'unique_image_uri_count': 10000,
                'unique_name_count': 10000,
                'unique_symbol_count': 1,
            },
            'report_summary': {
                'open_license_detected': False,
                'candidate_contract_count': 3,
                'high_confidence_contract_count': 1,
                'low_confidence_contract_count': 1,
                'legit_duplicate_contract_count': 1,
            },
            'suspected_infringing_duplicates_high_confidence': [
                {
                    'contract_address': '0xdup',
                    'candidate_count': 12,
                    'match_reasons': ['token_uri_match', 'symbol_match'],
                }
            ],
            'suspected_infringing_duplicates_low_confidence': [
                {
                    'contract_address': '0xlow',
                    'candidate_count': 3,
                    'match_reasons': ['name_match'],
                }
            ],
            'legit_duplicates': [
                {
                    'contract_address': '0xlegit',
                    'candidate_count': 5,
                    'mint_recipients': ['0xcreator'],
                }
            ],
            'address_signals': {
                '0xdup': {
                    'mint_address_count': 2,
                    'mint_count': 10,
                    'unique_receiver_count': 8,
                    'cycle_edge_count': 1,
                    'star_distributor_count': 1,
                    'mint_to_first_transfer_seconds': 600,
                    'fast_spread': True,
                }
            },
            'victim_signals': {
                '0xdup': {
                    'owner_count': 8,
                    'stuck_holder_count': 5,
                    'stuck_holder_ratio': 0.625,
                    'victim_wallet_count': 5,
                }
            },
        }

        text = mod.render_human_readable_report(payload)

        self.assertIn('# Top NFT 合约重复样本分析报告', text)
        self.assertIn('## 种子合约', text)
        self.assertIn('Azuki', text)
        self.assertIn('## 摘要', text)
        self.assertIn('高置信疑似侵权合约数: 1', text)
        self.assertIn('## 高置信疑似侵权合约', text)
        self.assertIn('0xdup', text)
        self.assertIn('## 地址行为信号', text)
        self.assertIn('快速扩散: 是', text)
        self.assertIn('## 受害者信号', text)
        self.assertIn('套牢地址数: 5', text)


if __name__ == '__main__':
    unittest.main()
