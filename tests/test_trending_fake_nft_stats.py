import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import trend_stats
from trend_stats import (
    ContractNameRecord,
    DatabaseSnapshot,
    DEFAULT_OPENSEA_ENDPOINT,
    TargetNFT,
    analyze_targets,
    build_parser,
    build_trending_request_url,
    extract_items_and_cursor,
    fetch_trending_targets,
    normalize_page_size,
    normalize_url,
)


class NormalizeUrlTests(unittest.TestCase):
    def test_normalize_url_collapses_ipfs_gateways(self):
        self.assertEqual(
            normalize_url('https://ipfs.io/ipfs/QmExampleCID/1.png?foo=1#bar'),
            'ipfs:QmExampleCID/1.png',
        )
        self.assertEqual(
            normalize_url('ipfs://ipfs/QmExampleCID/1.png'),
            'ipfs:QmExampleCID/1.png',
        )

    def test_normalize_url_collapses_arweave_gateway(self):
        tx = 'a' * 43
        self.assertEqual(
            normalize_url(f'https://arweave.net/{tx}/asset.png?foo=1'),
            f'ar:{tx}/asset.png',
        )


class ExtractItemsTests(unittest.TestCase):
    def test_extract_items_and_cursor_supports_doc_response_shape(self):
        payload = {
            'tokens': [
                {
                    'address': '0xabc',
                    'chain': 'ethereum',
                    'name': 'USD Coin',
                    'symbol': 'USDC',
                    'image_url': 'https://ipfs.io/ipfs/QmUsdc/logo.png',
                }
            ],
            'next': 'cursor-0',
        }

        items, cursor = extract_items_and_cursor(payload)

        self.assertEqual(cursor, 'cursor-0')
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].contract_address, '0xabc')
        self.assertEqual(items[0].token_id, '')
        self.assertEqual(items[0].name, 'USD Coin')
        self.assertEqual(items[0].symbol, 'USDC')


class ParserTests(unittest.TestCase):
    def test_build_parser_reads_page_size_argument(self):
        parser = build_parser()

        args = parser.parse_args(['--page-size', '37'])

        self.assertEqual(args.page_size, 37)


class AnalyzeTargetsTests(unittest.TestCase):
    def test_analyze_targets_uses_requested_counting_semantics(self):
        snapshot = DatabaseSnapshot(
            image_total_counts={'ipfs:shared.png': 4},
            image_contract_counts={
                ('ipfs:shared.png', '0xaaa'): 1,
                ('ipfs:shared.png', '0xbbb'): 2,
                ('ipfs:shared.png', '0xccc'): 1,
            },
            symbol_contracts={
                'azuki': {'0xaaa', '0xbbb', '0xccc'},
            },
            contract_names=[
                ContractNameRecord(contract_address='0xaaa', name_norm='azuki'),
                ContractNameRecord(contract_address='0xbbb', name_norm='azuki'),
                ContractNameRecord(contract_address='0xddd', name_norm='azuk1'),
                ContractNameRecord(contract_address='0xeee', name_norm='beanz'),
            ],
        )
        targets = [
            TargetNFT(
                chain='ethereum',
                contract_address='0xaaa',
                token_id='1',
                name='Azuki #1',
                symbol='AZUKI',
                image_url='https://gateway.pinata.cloud/ipfs/shared.png',
            )
        ]

        rows = analyze_targets(targets, snapshot, name_threshold=80.0)

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.image_duplicate_nft_count, 3)
        self.assertEqual(row.symbol_duplicate_contract_count, 2)
        self.assertEqual(row.name_duplicate_contract_count, 2)

    def test_analyze_targets_normalizes_api_name_before_matching(self):
        snapshot = DatabaseSnapshot(
            contract_names=[
                ContractNameRecord(contract_address='0xbbb', name_norm='azuki'),
                ContractNameRecord(contract_address='0xccc', name_norm='azuki'),
            ],
        )
        targets = [
            TargetNFT(
                chain='ethereum',
                contract_address='0xaaa',
                token_id='',
                name='Azuki #123',
                symbol='AZUKI',
                image_url='',
            )
        ]

        rows = analyze_targets(targets, snapshot, name_threshold=95.0)

        self.assertEqual(rows[0].name_norm, 'azuki')
        self.assertEqual(rows[0].name_duplicate_contract_count, 2)


class FetchTrendingTargetsTests(unittest.TestCase):
    def test_normalize_page_size_clamps_to_opensea_limit(self):
        self.assertEqual(normalize_page_size(0), 1)
        self.assertEqual(normalize_page_size(37), 37)
        self.assertEqual(normalize_page_size(100), 100)
        self.assertEqual(normalize_page_size(120), 100)

    def test_build_trending_request_url_uses_doc_parameter_names(self):
        url = build_trending_request_url(
            endpoint=DEFAULT_OPENSEA_ENDPOINT,
            chain='ethereum',
            page_limit=50,
            next_cursor='bz0xJnA9MjAyMi0wMi0wMiswMiUzQTQ1JTNBMTIuNjQ3MDM2%3D',
        )

        self.assertEqual(
            url,
            f'{DEFAULT_OPENSEA_ENDPOINT}?chains=ethereum&limit=50&cursor=bz0xJnA9MjAyMi0wMi0wMiswMiUzQTQ1JTNBMTIuNjQ3MDM2%3D',
        )

    def test_fetch_trending_targets_surfaces_http_403_with_hint(self):
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.reason = 'Forbidden'
        mock_response.text = 'blocked'
        mock_response.raise_for_status.side_effect = Exception('403')
        fake_requests = SimpleNamespace(
            get=Mock(return_value=mock_response),
            HTTPError=Exception,
            RequestException=Exception,
        )

        with patch.object(trend_stats, 'requests', fake_requests):
            with self.assertRaisesRegex(RuntimeError, 'OpenSea API returned 403'):
                fetch_trending_targets(api_key='bad-key', chain='ethereum', limit=1)

    def test_fetch_trending_targets_uses_cursor_for_second_page(self):
        first_response = Mock()
        first_response.status_code = 200
        first_response.reason = 'OK'
        first_response.text = ''
        first_response.raise_for_status.return_value = None
        first_response.json.return_value = {
            'tokens': [
                {'address': '0xaaa', 'chain': 'ethereum', 'name': 'A', 'symbol': 'A', 'image_url': 'x'},
            ],
            'next': 'abc%3Ddef',
        }
        second_response = Mock()
        second_response.status_code = 200
        second_response.reason = 'OK'
        second_response.text = ''
        second_response.raise_for_status.return_value = None
        second_response.json.return_value = {
            'tokens': [
                {'address': '0xbbb', 'chain': 'ethereum', 'name': 'B', 'symbol': 'B', 'image_url': 'y'},
            ],
            'next': '',
        }
        fake_get = Mock(side_effect=[first_response, second_response])
        fake_requests = SimpleNamespace(
            get=fake_get,
            HTTPError=Exception,
            RequestException=Exception,
        )

        with patch.object(trend_stats, 'requests', fake_requests):
            items = fetch_trending_targets(api_key='key', chain='ethereum', limit=2)

        self.assertEqual(len(items), 2)
        first_call_url = fake_get.call_args_list[0].args[0]
        second_call_url = fake_get.call_args_list[1].args[0]
        self.assertEqual(
            first_call_url,
            f'{DEFAULT_OPENSEA_ENDPOINT}?chains=ethereum&limit=2',
        )
        self.assertEqual(
            second_call_url,
            f'{DEFAULT_OPENSEA_ENDPOINT}?chains=ethereum&limit=1&cursor=abc%3Ddef',
        )

    def test_fetch_trending_targets_caps_requested_page_size_at_50(self):
        response = Mock()
        response.status_code = 200
        response.reason = 'OK'
        response.text = ''
        response.raise_for_status.return_value = None
        response.json.return_value = {
            'tokens': [
                {'address': '0xaaa', 'chain': 'ethereum', 'name': 'A', 'symbol': 'A', 'image_url': 'x'},
            ],
            'next': '',
        }
        fake_get = Mock(return_value=response)
        fake_requests = SimpleNamespace(
            get=fake_get,
            HTTPError=Exception,
            RequestException=Exception,
        )

        with patch.object(trend_stats, 'requests', fake_requests):
            fetch_trending_targets(api_key='key', chain='ethereum', limit=1000, page_size=100)

        first_call_url = fake_get.call_args_list[0].args[0]
        self.assertEqual(
            first_call_url,
            f'{DEFAULT_OPENSEA_ENDPOINT}?chains=ethereum&limit=100',
        )


if __name__ == '__main__':
    unittest.main()
