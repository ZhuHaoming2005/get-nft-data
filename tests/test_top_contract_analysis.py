import os
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import top_contract_analysis as mod
import top_contract_analysis.alchemy_api as alchemy_mod
import top_contract_analysis.analysis as analysis_mod
import top_contract_analysis.cli as cli_mod
import top_contract_analysis.constants as constants_mod
import top_contract_analysis.sales as sales_mod


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

        with patch.object(constants_mod, 'requests', fake_requests):
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

        with patch.object(constants_mod, 'requests', fake_requests):
            rows = mod.fetch_seed_contract_nfts(
                api_key='key',
                network='eth-mainnet',
                chain='ethereum',
                contract_address='0xseed',
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].token_uri, 'ipfs://seed/1')
        self.assertEqual(rows[0].image_uri, 'ipfs://image/1.png')


class FetchContractOwnersTests(unittest.TestCase):
    def test_fetch_contract_owners_skips_zero_address_rows(self):
        response = Mock()
        response.status_code = 200
        response.reason = 'OK'
        response.text = ''
        response.raise_for_status.return_value = None
        response.json.return_value = {
            'owners': [
                {
                    'ownerAddress': mod.ZERO_ADDRESS,
                    'tokenBalances': [{'tokenId': '0x1', 'balance': '1'}],
                },
                {
                    'ownerAddress': '0xholder',
                    'tokenBalances': [{'tokenId': '0x2', 'balance': '1'}],
                },
            ],
        }
        fake_requests = SimpleNamespace(get=Mock(return_value=response))

        with patch.object(constants_mod, 'requests', fake_requests):
            rows = mod.fetch_contract_owners(
                api_key='key',
                network='eth-mainnet',
                contract_address='0xdup',
            )

        self.assertEqual(
            rows,
            [mod.OwnerBalance(owner_address='0xholder', token_balances={'2': 1})],
        )


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

        with patch.object(alchemy_mod, 'fetch_nft_metadata', return_value={'raw': {'metadata': {'license': 'CC0-1.0'}}}) as mocked:
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
    def test_fetch_alchemy_contract_transfers_uses_metadata_block_timestamp(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            'result': {
                'transfers': [
                    {
                        'rawContract': {'address': '0xdup'},
                        'erc721TokenId': '0x1',
                        'hash': '0xabc',
                        'logIndex': 0,
                        'blockNum': '0x10',
                        'metadata': {'blockTimestamp': '2024-01-01T00:00:00.000Z'},
                        'from': mod.ZERO_ADDRESS,
                        'to': '0xbuyer',
                        'category': 'erc721',
                    }
                ]
            }
        }
        fake_requests = SimpleNamespace(post=Mock(return_value=response))

        with patch.object(constants_mod, 'requests', fake_requests):
            rows = mod.fetch_alchemy_contract_transfers(
                api_key='alchemy',
                network='eth-mainnet',
                contract_address='0xdup',
            )

        self.assertEqual(len(rows), 1)
        self.assertGreater(rows[0].block_time, 0)

    def test_fetch_alchemy_contract_transfers_falls_back_to_block_lookup_for_timestamp(self):
        transfer_response = Mock()
        transfer_response.raise_for_status.return_value = None
        transfer_response.json.return_value = {
            'result': {
                'transfers': [
                    {
                        'rawContract': {'address': '0xdup'},
                        'erc721TokenId': '0x1',
                        'hash': '0xabc',
                        'logIndex': 0,
                        'blockNum': '0x10',
                        'metadata': {},
                        'from': mod.ZERO_ADDRESS,
                        'to': '0xbuyer',
                        'category': 'erc721',
                    }
                ]
            }
        }
        block_response = Mock()
        block_response.raise_for_status.return_value = None
        block_response.json.return_value = {
            'result': {
                'timestamp': '0x65920080',
            }
        }
        fake_requests = SimpleNamespace(post=Mock(side_effect=[transfer_response, block_response]))

        with patch.object(constants_mod, 'requests', fake_requests):
            rows = mod.fetch_alchemy_contract_transfers(
                api_key='alchemy',
                network='eth-mainnet',
                contract_address='0xdup',
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].block_time, int('65920080', 16))

    def test_fetch_alchemy_contract_transfers_retries_before_succeeding(self):
        first_response = Mock()
        first_response.raise_for_status.side_effect = constants_mod.REQUESTS_REQUEST_ERROR('ssl eof')
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
                        'metadata': {'blockTimestamp': '2024-01-01T00:00:00.000Z'},
                        'from': mod.ZERO_ADDRESS,
                        'to': '0xbuyer',
                        'category': 'erc721',
                    }
                ]
            }
        }
        fake_requests = SimpleNamespace(post=Mock(side_effect=[first_response, second_response]))

        with patch.object(constants_mod, 'requests', fake_requests):
            rows = mod.fetch_alchemy_contract_transfers(
                api_key='alchemy',
                network='eth-mainnet',
                contract_address='0xdup',
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(fake_requests.post.call_count, 2)

    def test_fetch_contract_transfers_falls_back_to_etherscan_for_erc721(self):
        with patch.object(alchemy_mod, 'fetch_alchemy_contract_transfers', side_effect=RuntimeError('rate limited')) as alchemy_mock:
            with patch.object(alchemy_mod, 'fetch_etherscan_contract_transfers', return_value=[mod.TransferRecord(
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


class SalesAnalysisTests(unittest.TestCase):
    def test_fetch_alchemy_nft_sales_parses_native_eth_and_non_native_sales(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            'nftSales': [
                {
                    'marketplace': 'seaport',
                    'contractAddress': '0xdup',
                    'tokenId': '1',
                    'buyerAddress': '0xbuyer',
                    'sellerAddress': '0xseller',
                    'taker': 'BUYER',
                    'sellerFee': {'amount': '2000000000000000000', 'symbol': 'ETH', 'decimals': 18},
                    'protocolFee': {'amount': '100000000000000000', 'symbol': 'ETH', 'decimals': 18},
                    'royaltyFee': {'amount': '200000000000000000', 'symbol': 'ETH', 'decimals': 18},
                    'blockNumber': 11,
                    'logIndex': 7,
                    'bundleIndex': 0,
                    'transactionHash': '0xsale-eth',
                },
                {
                    'marketplace': 'blur',
                    'contractAddress': '0xdup',
                    'tokenId': '2',
                    'buyerAddress': '0xbuyer2',
                    'sellerAddress': '0xseller2',
                    'taker': 'BUYER',
                    'sellerFee': {'amount': '1500000000000000000', 'symbol': 'WETH', 'decimals': 18},
                    'protocolFee': {'amount': '0', 'symbol': 'WETH', 'decimals': 18},
                    'royaltyFee': {'amount': '0', 'symbol': 'WETH', 'decimals': 18},
                    'blockNumber': 12,
                    'logIndex': 8,
                    'bundleIndex': 0,
                    'transactionHash': '0xsale-weth',
                },
            ],
        }
        fake_requests = SimpleNamespace(get=Mock(return_value=response))

        with patch.object(constants_mod, 'requests', fake_requests):
            rows = mod.fetch_alchemy_nft_sales(
                api_key='alchemy',
                network='eth-mainnet',
                contract_address='0xdup',
            )

        self.assertEqual(len(rows), 2)
        self.assertTrue(rows[0].is_native_eth)
        self.assertAlmostEqual(rows[0].price_eth, 2.3)
        self.assertEqual(rows[0].payment_token_symbol, 'ETH')
        self.assertFalse(rows[1].is_native_eth)
        self.assertAlmostEqual(rows[1].price_eth, 1.5)
        self.assertEqual(rows[1].payment_token_symbol, 'WETH')

    def test_fetch_contract_sales_falls_back_to_opensea(self):
        opensea_sale = mod.NFTSaleRecord(
            contract_address='0xdup',
            token_id='1',
            tx_hash='0xopensea',
            block_number=11,
            log_index=1,
            bundle_index=0,
            buyer_address='0xbuyer',
            seller_address='0xseller',
            marketplace='opensea',
            taker='BUYER',
            payment_token_symbol='ETH',
            payment_token_address='',
            price_eth=1.25,
            seller_fee_eth=1.0,
            protocol_fee_eth=0.1,
            royalty_fee_eth=0.15,
            source='opensea',
            is_native_eth=True,
        )

        with patch.object(sales_mod, 'fetch_alchemy_nft_sales', return_value=[]), \
             patch.object(sales_mod, 'fetch_opensea_nft_events', return_value=[opensea_sale]):
            rows = mod.fetch_contract_sales(
                alchemy_api_key='alchemy',
                alchemy_network='eth-mainnet',
                contract_address='0xdup',
                opensea_api_key='opensea',
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].source, 'opensea')
        self.assertTrue(rows[0].is_native_eth)

    def test_calculate_sale_eth_metrics_uses_buyer_pre_balance_and_gas(self):
        sale = mod.NFTSaleRecord(
            contract_address='0xdup',
            token_id='1',
            tx_hash='0xsale',
            block_number=11,
            log_index=7,
            bundle_index=0,
            buyer_address='0xbuyer',
            seller_address='0xseller',
            marketplace='seaport',
            taker='BUYER',
            payment_token_symbol='ETH',
            payment_token_address='',
            price_eth=2.0,
            seller_fee_eth=1.7,
            protocol_fee_eth=0.1,
            royalty_fee_eth=0.2,
            source='alchemy',
            is_native_eth=True,
        )
        purchase_receipt = mod.TransactionReceiptRecord(
            tx_hash='0xsale',
            block_number=11,
            transaction_index=3,
            from_address='0xbuyer',
            gas_used=21_000,
            effective_gas_price_wei=10_000_000_000,
        )
        same_block_transfers = [
            mod.EthTransferRecord(
                tx_hash='0xin',
                block_number=11,
                from_address='0xother',
                to_address='0xbuyer',
                value_eth=1.0,
                category='external',
            ),
            mod.EthTransferRecord(
                tx_hash='0xout',
                block_number=11,
                from_address='0xbuyer',
                to_address='0xelse',
                value_eth=0.5,
                category='external',
            ),
            mod.EthTransferRecord(
                tx_hash='0xafter',
                block_number=11,
                from_address='0xother2',
                to_address='0xbuyer',
                value_eth=9.0,
                category='external',
            ),
        ]
        receipts_by_hash = {
            '0xin': mod.TransactionReceiptRecord(
                tx_hash='0xin',
                block_number=11,
                transaction_index=1,
                from_address='0xother',
                gas_used=0,
                effective_gas_price_wei=0,
            ),
            '0xout': mod.TransactionReceiptRecord(
                tx_hash='0xout',
                block_number=11,
                transaction_index=2,
                from_address='0xbuyer',
                gas_used=0,
                effective_gas_price_wei=0,
            ),
            '0xafter': mod.TransactionReceiptRecord(
                tx_hash='0xafter',
                block_number=11,
                transaction_index=5,
                from_address='0xother2',
                gas_used=0,
                effective_gas_price_wei=0,
            ),
        }

        metrics = mod.calculate_sale_eth_metrics(
            sale=sale,
            purchase_receipt=purchase_receipt,
            base_balance_eth=3.0,
            same_block_transfers=same_block_transfers,
            receipts_by_hash=receipts_by_hash,
        )

        self.assertEqual(metrics['ratio_status'], 'ok')
        self.assertAlmostEqual(metrics['buy_before_eth_balance'], 3.5)
        self.assertAlmostEqual(metrics['buy_amount_eth'], 2.0)
        self.assertAlmostEqual(metrics['buy_total_eth_out'], 2.00021)
        self.assertAlmostEqual(metrics['buy_asset_ratio'], 2.0 / 3.5)
        self.assertAlmostEqual(metrics['buy_asset_ratio_with_gas'], 2.00021 / 3.5)
        self.assertFalse(metrics['gas_not_attributed'])

    def test_calculate_sale_eth_metrics_skips_gas_when_buyer_is_not_tx_sender(self):
        sale = mod.NFTSaleRecord(
            contract_address='0xdup',
            token_id='1',
            tx_hash='0xsale',
            block_number=11,
            log_index=7,
            bundle_index=0,
            buyer_address='0xbuyer',
            seller_address='0xseller',
            marketplace='seaport',
            taker='BUYER',
            payment_token_symbol='ETH',
            payment_token_address='',
            price_eth=2.0,
            seller_fee_eth=1.7,
            protocol_fee_eth=0.1,
            royalty_fee_eth=0.2,
            source='alchemy',
            is_native_eth=True,
        )
        purchase_receipt = mod.TransactionReceiptRecord(
            tx_hash='0xsale',
            block_number=11,
            transaction_index=3,
            from_address='0xaggregator',
            gas_used=21_000,
            effective_gas_price_wei=10_000_000_000,
        )

        metrics = mod.calculate_sale_eth_metrics(
            sale=sale,
            purchase_receipt=purchase_receipt,
            base_balance_eth=4.0,
            same_block_transfers=[],
            receipts_by_hash={},
        )

        self.assertEqual(metrics['ratio_status'], 'ok')
        self.assertTrue(metrics['gas_not_attributed'])
        self.assertAlmostEqual(metrics['buy_total_eth_out'], 2.0)
        self.assertAlmostEqual(metrics['buy_asset_ratio_with_gas'], 0.5)

    def test_build_victim_address_records_tracks_last_stuck_purchase_amount_only(self):
        sales = [
            mod.NFTSaleRecord(
                contract_address='0xdup',
                token_id='1',
                tx_hash='0xfirst-buy',
                block_number=11,
                log_index=1,
                bundle_index=0,
                buyer_address='0xbuyer',
                seller_address='0xsybil',
                marketplace='seaport',
                taker='BUYER',
                payment_token_symbol='ETH',
                payment_token_address='',
                price_eth=1.0,
                seller_fee_eth=1.0,
                protocol_fee_eth=0.0,
                royalty_fee_eth=0.0,
                source='alchemy',
                is_native_eth=True,
            ),
            mod.NFTSaleRecord(
                contract_address='0xdup',
                token_id='2',
                tx_hash='0xlast-buy',
                block_number=12,
                log_index=1,
                bundle_index=0,
                buyer_address='0xbuyer',
                seller_address='0xsybil',
                marketplace='blur',
                taker='BUYER',
                payment_token_symbol='WETH',
                payment_token_address='0xweth',
                price_eth=3.0,
                seller_fee_eth=3.0,
                protocol_fee_eth=0.0,
                royalty_fee_eth=0.0,
                source='alchemy',
                is_native_eth=False,
            ),
        ]
        transfers = [
            mod.TransferRecord(
                contract_address='0xdup',
                token_id='1',
                tx_hash='0xresale',
                log_index=2,
                block_number=13,
                block_time=130,
                from_address='0xbuyer',
                to_address='0xother',
                event_type='erc721',
                source='alchemy',
            ),
        ]
        owners = [mod.OwnerBalance(owner_address='0xbuyer', token_balances={'2': 1})]
        sale_metrics_by_tx = {
            '0xfirst-buy': {
                'buy_before_eth_balance': 4.0,
                'buy_asset_ratio': 0.25,
                'buy_asset_ratio_with_gas': 0.2501,
                'ratio_status': 'ok',
            },
            '0xlast-buy': {
                'buy_before_eth_balance': None,
                'buy_asset_ratio': None,
                'buy_asset_ratio_with_gas': None,
                'ratio_status': 'unavailable',
            },
        }

        victim_addresses = analysis_mod.build_victim_address_records(
            contract_address='0xdup',
            sales=sales,
            transfers=transfers,
            owners=owners,
            sale_metrics_by_tx=sale_metrics_by_tx,
        )

        self.assertEqual(len(victim_addresses), 1)
        self.assertAlmostEqual(victim_addresses[0]['buy_amount_eth'], 4.0)
        self.assertTrue(victim_addresses[0]['is_stuck'])
        self.assertEqual(victim_addresses[0]['last_buy_tx_hash'], '0xlast-buy')
        self.assertAlmostEqual(victim_addresses[0]['last_buy_amount_eth'], 3.0)

    def test_build_fraud_trade_stats_counts_weth_and_only_last_stuck_buy(self):
        sales = [
            mod.NFTSaleRecord(
                contract_address='0xdup',
                token_id='1',
                tx_hash='0xeth-buy',
                block_number=11,
                log_index=1,
                bundle_index=0,
                buyer_address='0xbuyer1',
                seller_address='0xsybil',
                marketplace='seaport',
                taker='BUYER',
                payment_token_symbol='ETH',
                payment_token_address='',
                price_eth=2.0,
                seller_fee_eth=2.0,
                protocol_fee_eth=0.0,
                royalty_fee_eth=0.0,
                source='alchemy',
                is_native_eth=True,
            ),
            mod.NFTSaleRecord(
                contract_address='0xdup',
                token_id='2',
                tx_hash='0xweth-buy',
                block_number=12,
                log_index=1,
                bundle_index=0,
                buyer_address='0xbuyer2',
                seller_address='0xsybil',
                marketplace='blur',
                taker='BUYER',
                payment_token_symbol='WETH',
                payment_token_address='0xweth',
                price_eth=1.5,
                seller_fee_eth=1.5,
                protocol_fee_eth=0.0,
                royalty_fee_eth=0.0,
                source='alchemy',
                is_native_eth=False,
            ),
            mod.NFTSaleRecord(
                contract_address='0xdup',
                token_id='3',
                tx_hash='0xusdc-buy',
                block_number=13,
                log_index=1,
                bundle_index=0,
                buyer_address='0xbuyer3',
                seller_address='0xsybil',
                marketplace='opensea',
                taker='BUYER',
                payment_token_symbol='USDC',
                payment_token_address='0xusdc',
                price_eth=None,
                seller_fee_eth=0.0,
                protocol_fee_eth=0.0,
                royalty_fee_eth=0.0,
                source='opensea',
                is_native_eth=False,
            ),
        ]
        victim_addresses = [
            {
                'address': '0xbuyer1',
                'buy_amount_eth': 5.0,
                'last_buy_amount_eth': 2.0,
                'is_stuck': True,
            },
            {
                'address': '0xbuyer2',
                'buy_amount_eth': 1.5,
                'last_buy_amount_eth': 1.5,
                'is_stuck': False,
            },
        ]

        stats = analysis_mod.build_fraud_trade_stats(
            contract_address='0xdup',
            sales=sales,
            victim_addresses=victim_addresses,
        )['0xdup']

        self.assertEqual(stats['unique_buyers'], 3)
        self.assertEqual(stats['eth_priced_sale_count'], 2)
        self.assertAlmostEqual(stats['eth_priced_volume'], 3.5)
        self.assertEqual(stats['stuck_wallet_count'], 1)
        self.assertAlmostEqual(stats['stuck_cost_eth'], 2.0)

    def test_build_honest_address_records_tracks_corrupted_addresses_and_hold_times(self):
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
                tx_hash='0xvictim-a',
                log_index=1,
                block_number=11,
                block_time=150,
                from_address='0xsybil',
                to_address='0xhonest1',
                event_type='erc721',
                source='alchemy',
            ),
            mod.TransferRecord(
                contract_address='0xdup',
                token_id='1',
                tx_hash='0xvictim-b',
                log_index=2,
                block_number=12,
                block_time=200,
                from_address='0xhonest1',
                to_address='0xhonest2',
                event_type='erc721',
                source='alchemy',
            ),
        ]
        owners = [
            mod.OwnerBalance(owner_address='0xhonest2', token_balances={'1': 1}),
            mod.OwnerBalance(owner_address=mod.ZERO_ADDRESS, token_balances={'1': 1}),
        ]
        infringing_tokens = [
            {
                'contract_address': '0xdup',
                'token_id': '1',
            }
        ]
        malicious_addresses = [
            {
                'address': '0xsybil',
            }
        ]

        honest_addresses = analysis_mod.build_honest_address_records(
            contract_address='0xdup',
            transfers=transfers,
            sales=[],
            owners=owners,
            infringing_tokens=infringing_tokens,
            malicious_addresses=malicious_addresses,
            analysis_timestamp=300,
        )
        honest_stats = analysis_mod.build_honest_address_stats(
            contract_address='0xdup',
            honest_addresses=honest_addresses,
        )['0xdup']

        self.assertEqual([item['address'] for item in honest_addresses], ['0xhonest1', '0xhonest2'])
        self.assertTrue(honest_addresses[0]['is_corrupted_address'])
        self.assertFalse(honest_addresses[1]['is_corrupted_address'])
        self.assertEqual(honest_addresses[0]['hold_duration_median_seconds'], 50)
        self.assertEqual(honest_addresses[1]['hold_duration_median_seconds'], 100)
        self.assertEqual(honest_stats['honest_address_count'], 2)
        self.assertEqual(honest_stats['corrupted_address_count'], 1)
        self.assertEqual(honest_stats['honest_to_honest_transfer_count'], 1)
        self.assertEqual(honest_stats['median_holding_seconds'], 75.0)
        self.assertEqual(honest_stats['avg_seconds_to_honest_holder'], 50.0)

    def test_analyze_contract_victims_skips_zero_address_owner_balances(self):
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
            ),
        ]
        owners = [
            mod.OwnerBalance(owner_address='0xbuyer', token_balances={'1': 1}),
            mod.OwnerBalance(owner_address=mod.ZERO_ADDRESS, token_balances={'2': 1}),
        ]

        signals = analysis_mod.analyze_contract_victims(transfers, owners)

        self.assertEqual(signals['owner_count'], 1)
        self.assertEqual(signals['stuck_holder_count'], 1)
        self.assertEqual(signals['victim_wallet_count'], 1)


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
    def test_analyze_seed_contract_logs_stage_timings(self):
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
                tx_hash='0xvictim-a',
                log_index=1,
                block_number=11,
                block_time=150,
                from_address='0xsybil',
                to_address='0xhonest1',
                event_type='erc721',
                source='alchemy',
            ),
        ]
        owners = [mod.OwnerBalance(owner_address='0xhonest1', token_balances={'1': 1})]
        sales = [
            mod.NFTSaleRecord(
                contract_address='0xdup',
                token_id='1',
                tx_hash='0xvictim-a',
                block_number=11,
                log_index=1,
                bundle_index=0,
                buyer_address='0xhonest1',
                seller_address='0xsybil',
                marketplace='seaport',
                taker='BUYER',
                payment_token_symbol='ETH',
                payment_token_address='',
                price_eth=1.0,
                seller_fee_eth=1.0,
                protocol_fee_eth=0.0,
                royalty_fee_eth=0.0,
                source='alchemy',
                is_native_eth=True,
            )
        ]
        purchase_receipt = mod.TransactionReceiptRecord(
            tx_hash='0xvictim-a',
            block_number=11,
            transaction_index=3,
            from_address='0xhonest1',
            gas_used=21_000,
            effective_gas_price_wei=10_000_000_000,
        )

        with patch.object(analysis_mod, 'fetch_contract_metadata', return_value=seed_meta), \
             patch.object(analysis_mod, 'fetch_seed_contract_nfts', return_value=seed_nfts), \
             patch.object(analysis_mod, 'fetch_license_sample', return_value={'raw': {'metadata': {'license': 'All rights reserved'}}}), \
             patch.object(analysis_mod, 'load_database_snapshot', return_value=snapshot), \
             patch.object(analysis_mod, 'fetch_contract_transfers', return_value=transfers), \
             patch.object(analysis_mod, 'fetch_contract_owners', return_value=owners), \
             patch.object(analysis_mod, 'fetch_contract_sales', return_value=sales), \
             patch.object(analysis_mod, 'fetch_transaction_receipt', return_value=purchase_receipt), \
             patch.object(analysis_mod, 'fetch_eth_balance', return_value=4.0), \
             patch.object(analysis_mod, 'fetch_same_block_eth_transfers_for_address', return_value=[]), \
             patch.object(analysis_mod.logger, 'info') as logger_info:
            mod.analyze_seed_contract(
                chain='ethereum',
                seed_contract_address='0xseed',
                alchemy_api_key='fake-alchemy-key-123456',
                alchemy_network='eth-mainnet',
                etherscan_api_key='etherscan',
                conn=object(),
            )

        info_messages = [call.args[0] for call in logger_info.call_args_list if call.args]
        self.assertTrue(any('seed timing' in message for message in info_messages))
        self.assertTrue(any('contract timing' in message for message in info_messages))

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

        with patch.object(analysis_mod, 'fetch_contract_metadata', return_value=seed_meta), \
             patch.object(analysis_mod, 'fetch_seed_contract_nfts', return_value=seed_nfts), \
             patch.object(analysis_mod, 'fetch_license_sample', return_value={'raw': {'metadata': {'license': 'All rights reserved'}}}), \
             patch.object(analysis_mod, 'load_database_snapshot', return_value=snapshot), \
             patch.object(analysis_mod, 'fetch_contract_transfers', return_value=transfers), \
             patch.object(analysis_mod, 'fetch_contract_owners', return_value=owners):
            result = mod.analyze_seed_contract(
                chain='ethereum',
                seed_contract_address='0xseed',
                alchemy_api_key='fake-alchemy-key-123456',
                alchemy_network='eth-mainnet',
                etherscan_api_key='etherscan',
                conn=object(),
            )

        self.assertEqual(result['seed_contract']['contract_address'], '0xseed')
        self.assertEqual(result['report_summary']['high_confidence_contract_count'], 1)
        self.assertFalse(result['report_summary']['open_license_detected'])
        self.assertEqual(result['address_signals']['0xdup']['mint_address_count'], 1)

    def test_analyze_seed_contract_emits_infringing_tokens_victims_and_fraud_stats(self):
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
                tx_hash='0xsale',
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
        sales = [
            mod.NFTSaleRecord(
                contract_address='0xdup',
                token_id='1',
                tx_hash='0xsale',
                block_number=11,
                log_index=1,
                bundle_index=0,
                buyer_address='0xbuyer',
                seller_address='0xsybil',
                marketplace='seaport',
                taker='BUYER',
                payment_token_symbol='ETH',
                payment_token_address='',
                price_eth=2.0,
                seller_fee_eth=1.7,
                protocol_fee_eth=0.1,
                royalty_fee_eth=0.2,
                source='alchemy',
                is_native_eth=True,
            )
        ]
        purchase_receipt = mod.TransactionReceiptRecord(
            tx_hash='0xsale',
            block_number=11,
            transaction_index=3,
            from_address='0xbuyer',
            gas_used=21_000,
            effective_gas_price_wei=10_000_000_000,
        )

        with patch.object(analysis_mod, 'fetch_contract_metadata', return_value=seed_meta), \
             patch.object(analysis_mod, 'fetch_seed_contract_nfts', return_value=seed_nfts), \
             patch.object(analysis_mod, 'fetch_license_sample', return_value={'raw': {'metadata': {'license': 'All rights reserved'}}}), \
             patch.object(analysis_mod, 'load_database_snapshot', return_value=snapshot), \
             patch.object(analysis_mod, 'fetch_contract_transfers', return_value=transfers), \
             patch.object(analysis_mod, 'fetch_contract_owners', return_value=owners), \
             patch.object(analysis_mod, 'fetch_contract_sales', return_value=sales), \
             patch.object(analysis_mod, 'fetch_transaction_receipt', return_value=purchase_receipt), \
             patch.object(analysis_mod, 'fetch_eth_balance', return_value=4.0), \
             patch.object(analysis_mod, 'fetch_same_block_eth_transfers_for_address', return_value=[]):
            result = mod.analyze_seed_contract(
                chain='ethereum',
                seed_contract_address='0xseed',
                alchemy_api_key='fake-alchemy-key-123456',
                alchemy_network='eth-mainnet',
                etherscan_api_key='etherscan',
                conn=object(),
            )

        self.assertIn('infringing_tokens', result)
        self.assertIn('malicious_addresses', result)
        self.assertIn('victim_addresses', result)
        self.assertIn('fraud_trade_stats', result)
        self.assertEqual(result['infringing_tokens'][0]['minter_address'], '0xsybil')
        self.assertEqual(result['victim_addresses'][0]['address'], '0xbuyer')
        self.assertTrue(result['victim_addresses'][0]['is_stuck'])
        self.assertAlmostEqual(result['victim_addresses'][0]['buy_asset_ratio'], 0.5)
        self.assertEqual(result['fraud_trade_stats']['0xdup']['native_eth_sale_count'], 1)
        self.assertAlmostEqual(result['fraud_trade_stats']['0xdup']['stuck_cost_eth'], 2.0)

    def test_analyze_seed_contract_emits_honest_address_stats_and_candidate_license_counts(self):
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
                    metadata_json='{"license":"CC0-1.0"}',
                    metadata_doc='license cc0-1.0',
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
                tx_hash='0xvictim-a',
                log_index=1,
                block_number=11,
                block_time=150,
                from_address='0xsybil',
                to_address='0xhonest1',
                event_type='erc721',
                source='alchemy',
            ),
            mod.TransferRecord(
                contract_address='0xdup',
                token_id='1',
                tx_hash='0xvictim-b',
                log_index=2,
                block_number=12,
                block_time=200,
                from_address='0xhonest1',
                to_address='0xhonest2',
                event_type='erc721',
                source='alchemy',
            ),
        ]
        owners = [
            mod.OwnerBalance(owner_address='0xhonest2', token_balances={'1': 1}),
        ]
        sales = [
            mod.NFTSaleRecord(
                contract_address='0xdup',
                token_id='1',
                tx_hash='0xvictim-a',
                block_number=11,
                log_index=1,
                bundle_index=0,
                buyer_address='0xhonest1',
                seller_address='0xsybil',
                marketplace='seaport',
                taker='BUYER',
                payment_token_symbol='ETH',
                payment_token_address='',
                price_eth=2.0,
                seller_fee_eth=1.7,
                protocol_fee_eth=0.1,
                royalty_fee_eth=0.2,
                source='alchemy',
                is_native_eth=True,
            )
        ]
        purchase_receipt = mod.TransactionReceiptRecord(
            tx_hash='0xvictim-a',
            block_number=11,
            transaction_index=3,
            from_address='0xhonest1',
            gas_used=21_000,
            effective_gas_price_wei=10_000_000_000,
        )

        with patch.object(analysis_mod, 'fetch_contract_metadata', return_value=seed_meta), \
             patch.object(analysis_mod, 'fetch_seed_contract_nfts', return_value=seed_nfts), \
             patch.object(analysis_mod, 'fetch_license_sample', return_value={'raw': {'metadata': {'license': 'All rights reserved'}}}), \
             patch.object(analysis_mod, 'load_database_snapshot', return_value=snapshot), \
             patch.object(analysis_mod, 'fetch_contract_transfers', return_value=transfers), \
             patch.object(analysis_mod, 'fetch_contract_owners', return_value=owners), \
             patch.object(analysis_mod, 'fetch_contract_sales', return_value=sales), \
             patch.object(analysis_mod, 'fetch_transaction_receipt', return_value=purchase_receipt), \
             patch.object(analysis_mod, 'fetch_eth_balance', return_value=4.0), \
             patch.object(analysis_mod, 'fetch_same_block_eth_transfers_for_address', return_value=[]), \
             patch.object(analysis_mod.time, 'time', return_value=300):
            result = mod.analyze_seed_contract(
                chain='ethereum',
                seed_contract_address='0xseed',
                alchemy_api_key='fake-alchemy-key-123456',
                alchemy_network='eth-mainnet',
                etherscan_api_key='etherscan',
                conn=object(),
            )

        self.assertEqual(result['infringing_tokens'][0]['candidate_open_license'], True)
        self.assertEqual(result['report_summary']['candidate_open_license_token_count'], 1)
        self.assertEqual(result['report_summary']['candidate_open_license_contract_count'], 1)
        self.assertAlmostEqual(result['report_summary']['honest_purchase_total_eth'], 2.0)
        self.assertEqual(result['report_summary']['buy_asset_ratio_known_address_count'], 1)
        self.assertEqual(result['report_summary']['ratio_over_60_address_count'], 0)
        self.assertEqual(result['report_summary']['ratio_over_80_address_count'], 0)
        self.assertEqual(result['report_summary']['ratio_over_60_address_ratio'], 0.0)
        self.assertEqual(result['report_summary']['ratio_over_80_address_ratio'], 0.0)
        self.assertEqual(result['report_summary']['stuck_honest_address_count'], 0)
        self.assertEqual(result['report_summary']['stuck_honest_address_ratio'], 0.0)
        self.assertAlmostEqual(result['report_summary']['avg_seconds_to_honest_holder'], 50.0)
        self.assertAlmostEqual(result['report_summary']['avg_mint_to_first_transfer_seconds'], 50.0)
        self.assertAlmostEqual(result['report_summary']['avg_unique_receiver_count'], 3.0)
        self.assertEqual(result['honest_address_stats']['0xdup']['honest_address_count'], 2)
        self.assertEqual(result['honest_address_stats']['0xdup']['corrupted_address_count'], 1)
        self.assertEqual(result['honest_address_stats']['0xdup']['honest_to_honest_transfer_count'], 1)
        self.assertAlmostEqual(result['honest_address_stats']['0xdup']['avg_seconds_to_honest_holder'], 50.0)
        self.assertTrue(result['honest_addresses'][0]['is_corrupted_address'])

    def test_analyze_contract_transfers_computes_non_zero_first_transfer_delay(self):
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
        ]

        result = mod.analyze_contract_transfers(transfers)

        self.assertEqual(result['mint_to_first_transfer_seconds'], 360)
        self.assertTrue(result['fast_spread'])


class ParserNetworkDefaultTests(unittest.TestCase):
    def test_parser_does_not_default_alchemy_network_from_env(self):
        with patch.dict('os.environ', {'ALCHEMY_NETWORK': 'polygon-mainnet'}, clear=False):
            parser = mod.build_parser()
            args = parser.parse_args(['--chain', 'ethereum', '--seed-contract-address', '0xseed'])

        self.assertEqual(args.alchemy_network, '')

    def test_parser_exposes_async_concurrency_limits(self):
        parser = mod.build_parser()
        args = parser.parse_args(['--chain', 'ethereum', '--seed-contract-address', '0xseed'])

        self.assertEqual(args.api_max_concurrency, constants_mod.DEFAULT_API_MAX_CONCURRENCY)
        self.assertEqual(args.contract_max_concurrency, constants_mod.DEFAULT_CONTRACT_MAX_CONCURRENCY)
        self.assertEqual(args.sale_metric_max_concurrency, constants_mod.DEFAULT_SALE_METRIC_MAX_CONCURRENCY)

    def test_public_api_is_reexported_from_split_modules(self):
        self.assertEqual(mod.fetch_seed_contract_nfts.__module__, 'top_contract_analysis.alchemy_api')
        self.assertEqual(mod.fetch_contract_sales.__module__, 'top_contract_analysis.sales')
        self.assertEqual(mod.analyze_seed_contract.__module__, 'top_contract_analysis.analysis')
        self.assertEqual(mod.render_human_readable_report.__module__, 'top_contract_analysis.reporting')
        self.assertEqual(mod.build_parser.__module__, 'top_contract_analysis.cli')
        self.assertFalse(hasattr(mod, '_require_requests'))
        self.assertFalse(hasattr(mod, '_alchemy_get_json'))
        self.assertFalse(hasattr(mod, '_transfer_sort_key'))
        self.assertFalse(hasattr(mod, 'requests'))

    def test_internal_scripts_import_split_modules_directly(self):
        package_dir = Path(mod.__file__).resolve().parent
        targets = [
            'duckdb_store.py',
            'export_snapshot.py',
            'signal_cache.py',
            'batch.py',
            'render_report.py',
        ]
        for filename in targets:
            text = (package_dir / filename).read_text(encoding='utf-8')
            self.assertNotIn('from . import ', text, filename)

    def test_sales_implementation_is_not_defined_in_alchemy_api(self):
        package_dir = Path(mod.__file__).resolve().parent
        text = (package_dir / 'alchemy_api.py').read_text(encoding='utf-8')
        for marker in [
            'def _decode_fee_eth(',
            'def _looks_like_real_api_key(',
            'def fetch_alchemy_nft_sales(',
            'def fetch_opensea_nft_events(',
            'def fetch_contract_sales(',
        ]:
            self.assertNotIn(marker, text, marker)

    def test_analysis_cli_and_sales_do_not_use_package_importlib_routing(self):
        package_dir = Path(mod.__file__).resolve().parent
        for filename in ['analysis.py', 'cli.py', 'sales.py', 'alchemy_api.py']:
            text = (package_dir / filename).read_text(encoding='utf-8')
            self.assertNotIn('importlib', text, filename)


class AsyncApiExportsTests(unittest.TestCase):
    def test_public_api_reexports_async_entrypoints(self):
        self.assertTrue(hasattr(mod, 'fetch_contract_metadata_async'))
        self.assertTrue(hasattr(mod, 'fetch_contract_sales_async'))
        self.assertTrue(hasattr(mod, 'fetch_contract_transfers_async'))
        self.assertTrue(hasattr(mod, 'fetch_contract_owners_async'))
        self.assertTrue(hasattr(mod, 'async_analyze_seed_contract'))


class SyncWrapperTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_fetch_contract_transfers_uses_async_client(self):
        class FakeClient:
            async def post_json(self, url, payload):
                self.last_url = url
                self.last_payload = payload
                return {'result': {'transfers': []}}

        client = FakeClient()
        rows = await mod.fetch_contract_transfers_async(
            client=client,
            alchemy_api_key='alchemy',
            alchemy_network='eth-mainnet',
            etherscan_api_key='etherscan',
            chain='ethereum',
            contract_address='0xdup',
            token_type='ERC721',
            timeout=1,
        )

        self.assertEqual(rows, [])
        self.assertIn('eth-mainnet.g.alchemy.com', client.last_url)
        self.assertEqual(client.last_payload['method'], 'alchemy_getAssetTransfers')


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
                'candidate_open_license_token_count': 1,
                'candidate_open_license_contract_count': 1,
                'honest_purchase_total_eth': 2.0,
                'buy_asset_ratio_known_address_count': 1,
                'ratio_over_60_address_count': 0,
                'ratio_over_60_address_ratio': 0.0,
                'ratio_over_80_address_count': 0,
                'ratio_over_80_address_ratio': 0.0,
                'stuck_honest_address_count': 0,
                'stuck_honest_address_ratio': 0.0,
                'avg_seconds_to_honest_holder': 50.0,
                'avg_mint_to_first_transfer_seconds': 600.0,
                'avg_unique_receiver_count': 8.0,
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
            'infringing_tokens': [
                {
                    'contract_address': '0xdup',
                    'token_id': '1',
                    'mint_tx_hash': '0xmint',
                    'mint_block': 10,
                    'minter_address': '0xsybil',
                    'candidate_open_license': True,
                    'first_transfer_time': 100,
                    'history_window': 'full',
                    'match_reasons': ['token_uri_match'],
                    'official_or_legit_reissue': False,
                }
            ],
            'honest_address_stats': {
                '0xdup': {
                    'honest_address_count': 2,
                    'corrupted_address_count': 1,
                    'honest_to_honest_transfer_count': 1,
                    'median_holding_seconds': 75.0,
                    'avg_seconds_to_honest_holder': 50.0,
                    'corrupted_addresses': ['0xhonest1'],
                }
            },
            'honest_addresses': [
                {
                    'address': '0xhonest1',
                    'interacted_token_count': 1,
                    'currently_holding_token_count': 0,
                    'hold_duration_median_seconds': 50,
                    'hold_duration_count': 1,
                    'is_corrupted_address': True,
                    'honest_sale_to_honest_count': 1,
                },
                {
                    'address': '0xhonest2',
                    'interacted_token_count': 1,
                    'currently_holding_token_count': 1,
                    'hold_duration_median_seconds': 100,
                    'hold_duration_count': 1,
                    'is_corrupted_address': False,
                    'honest_sale_to_honest_count': 0,
                },
            ],
            'malicious_addresses': [
                {
                    'address': '0xsybil',
                    'mint_role': True,
                    'wash_cycle_count': 1,
                    'star_out_degree': 1,
                    'rapid_spread_contracts': ['0xdup'],
                    'evidence_contracts': ['0xdup'],
                }
            ],
            'victim_addresses': [
                {
                    'address': '0xbuyer',
                    'buy_tx_hashes': ['0xsale'],
                    'buy_amount_eth': 2.0,
                    'last_buy_amount_eth': 2.0,
                    'buy_before_eth_balance': 4.0,
                    'buy_asset_ratio': 0.5,
                    'buy_asset_ratio_with_gas': 0.5000525,
                    'is_stuck': True,
                    'last_buy_tx_hash': '0xsale',
                    'ratio_status': 'ok',
                }
            ],
            'fraud_trade_stats': {
                '0xdup': {
                    'unique_buyers': 1,
                    'native_eth_sale_count': 1,
                    'native_eth_volume': 2.0,
                    'eth_priced_sale_count': 1,
                    'eth_priced_volume': 2.0,
                    'stuck_wallet_count': 1,
                    'stuck_cost_eth': 2.0,
                }
            },
        }

        text = mod.render_human_readable_report(payload)

        self.assertIn('# Top NFT 合约重复样本分析报告', text)
        self.assertIn('## 种子合约', text)
        self.assertIn('Azuki', text)
        self.assertIn('## 摘要', text)
        self.assertIn('高置信疑似侵权合约数: 1', text)
        self.assertIn('候选侧开放许可 token 数: 1', text)
        self.assertIn('诚实地址购买总金额(ETH/WETH): 2.0', text)
        self.assertIn('可计算买入占比的诚实地址数: 1', text)
        self.assertIn('买入金额占钱包总额 >60% 的地址数/占比: 0 / 0.00%', text)
        self.assertIn('买入金额占钱包总额 >80% 的地址数/占比: 0 / 0.00%', text)
        self.assertIn('购买后无法再次售出的诚实节点数/占比: 0 / 0.00%', text)
        self.assertIn('Mint 到被诚实节点购买平均时间: 50.0 秒', text)
        self.assertIn('Mint 到首次转手平均时间: 600.0 秒', text)
        self.assertIn('候选合约平均唯一接收钱包数: 8.0', text)
        self.assertIn('## 高置信疑似侵权合约', text)
        self.assertIn('0xdup', text)
        self.assertIn('归为官方参与型重复的合约数: 1', text)
        self.assertIn('## 被算法归为官方参与型重复的合约', text)
        self.assertIn('该分组仅表示 mint 接收地址与官方地址集合存在交集', text)
        self.assertIn('## 地址行为信号', text)
        self.assertIn('快速扩散: 是', text)
        self.assertIn('## 受害者信号', text)
        self.assertIn('套牢地址数: 5', text)
        self.assertNotIn('## 侵权 NFT 历史记录', text)
        self.assertNotIn('## 恶意地址画像', text)
        self.assertIn('## 诚实地址画像', text)
        self.assertIn('被腐化地址数: 1', text)
        self.assertIn('Mint 到诚实地址平均时间: 50.0 秒', text)
        self.assertIn('## 被骗地址画像', text)
        self.assertIn('## 被骗交易与套牢资金', text)
        self.assertIn('买入前 ETH 余额: 4.0', text)
        self.assertIn('最后一次买入金额(ETH/WETH)=2.0', text)
        self.assertIn('eth_priced_sale_count=1', text)
        self.assertNotIn('## 合法重复合约', text)
        self.assertNotIn('合法重复合约数', text)


class BatchSummaryReportTests(unittest.TestCase):
    def test_build_batch_summary_payload_aggregates_extended_summary_metrics(self):
        payloads = [
            {
                'seed_contract': {'chain': 'ethereum', 'contract_address': '0xseed1', 'name': 'Alpha'},
                'report_summary': {
                    'open_license_detected': False,
                    'candidate_contract_count': 3,
                    'high_confidence_contract_count': 2,
                    'low_confidence_contract_count': 1,
                    'legit_duplicate_contract_count': 0,
                    'honest_purchase_total_eth': 2.5,
                    'buy_asset_ratio_known_address_count': 2,
                    'ratio_over_60_address_count': 1,
                    'ratio_over_60_address_ratio': 0.5,
                    'ratio_over_80_address_count': 0,
                    'ratio_over_80_address_ratio': 0.0,
                    'stuck_honest_address_count': 1,
                    'stuck_honest_address_ratio': 0.5,
                    'avg_seconds_to_honest_holder': 100.0,
                    'avg_mint_to_first_transfer_seconds': 200.0,
                    'avg_unique_receiver_count': 5.0,
                },
            },
            {
                'seed_contract': {'chain': 'ethereum', 'contract_address': '0xseed2', 'name': 'Beta'},
                'report_summary': {
                    'open_license_detected': True,
                    'candidate_contract_count': 4,
                    'high_confidence_contract_count': 1,
                    'low_confidence_contract_count': 2,
                    'legit_duplicate_contract_count': 1,
                    'honest_purchase_total_eth': 1.5,
                    'buy_asset_ratio_known_address_count': 1,
                    'ratio_over_60_address_count': 1,
                    'ratio_over_60_address_ratio': 1.0,
                    'ratio_over_80_address_count': 1,
                    'ratio_over_80_address_ratio': 1.0,
                    'stuck_honest_address_count': 1,
                    'stuck_honest_address_ratio': 1.0,
                    'avg_seconds_to_honest_holder': 300.0,
                    'avg_mint_to_first_transfer_seconds': 600.0,
                    'avg_unique_receiver_count': 7.0,
                },
            },
        ]

        batch_payload = mod.build_batch_summary_payload(payloads)
        summary = batch_payload['batch_summary']

        self.assertEqual(summary['seed_report_count'], 2)
        self.assertEqual(summary['open_license_detected_count'], 1)
        self.assertEqual(summary['candidate_contract_count_total'], 7)
        self.assertAlmostEqual(summary['honest_purchase_total_eth_total'], 4.0)
        self.assertEqual(summary['buy_asset_ratio_known_address_count_total'], 3)
        self.assertEqual(summary['ratio_over_60_address_count_total'], 2)
        self.assertAlmostEqual(summary['ratio_over_60_address_ratio_overall'], 2 / 3)
        self.assertEqual(summary['ratio_over_80_address_count_total'], 1)
        self.assertAlmostEqual(summary['ratio_over_80_address_ratio_overall'], 1 / 3)
        self.assertEqual(summary['stuck_honest_address_count_total'], 2)
        self.assertAlmostEqual(summary['stuck_honest_address_ratio_overall'], 2 / 3)
        self.assertAlmostEqual(summary['avg_seconds_to_honest_holder_mean'], 200.0)
        self.assertAlmostEqual(summary['avg_mint_to_first_transfer_seconds_mean'], 400.0)
        self.assertAlmostEqual(summary['avg_unique_receiver_count_mean'], 6.0)

    def test_render_batch_human_readable_report_includes_extended_summary_metrics(self):
        payload = {
            'batch_summary': {
                'seed_report_count': 2,
                'chain': 'ethereum',
                'chains': ['ethereum'],
                'open_license_detected_count': 1,
                'candidate_contract_count_total': 7,
                'high_confidence_contract_count_total': 3,
                'low_confidence_contract_count_total': 3,
                'legit_duplicate_contract_count_total': 1,
                'honest_purchase_total_eth_total': 4.0,
                'buy_asset_ratio_known_address_count_total': 3,
                'ratio_over_60_address_count_total': 2,
                'ratio_over_60_address_ratio_overall': 2 / 3,
                'ratio_over_80_address_count_total': 1,
                'ratio_over_80_address_ratio_overall': 1 / 3,
                'stuck_honest_address_count_total': 2,
                'stuck_honest_address_ratio_overall': 2 / 3,
                'avg_seconds_to_honest_holder_mean': 200.0,
                'avg_mint_to_first_transfer_seconds_mean': 400.0,
                'avg_unique_receiver_count_mean': 6.0,
                'generated_at': '2026-04-13T00:00:00+00:00',
            },
            'seed_reports': [
                {
                    'seed_contract': {'name': 'Alpha', 'contract_address': '0xseed1'},
                    'report_summary': {
                        'high_confidence_contract_count': 2,
                        'low_confidence_contract_count': 1,
                        'legit_duplicate_contract_count': 0,
                        'honest_purchase_total_eth': 2.5,
                        'ratio_over_60_address_count': 1,
                        'ratio_over_60_address_ratio': 0.5,
                        'stuck_honest_address_count': 1,
                        'stuck_honest_address_ratio': 0.5,
                        'avg_seconds_to_honest_holder': 100.0,
                    },
                    'output_files': {'json': 'alpha.json', 'markdown': 'alpha.md'},
                }
            ],
        }

        text = mod.render_batch_human_readable_report(payload)

        self.assertIn('诚实地址购买总金额(ETH/WETH)汇总: 4.0', text)
        self.assertIn('可计算买入占比的诚实地址总数: 3', text)
        self.assertIn('买入金额占钱包总额 >60% 的地址数/总体占比: 2 / 66.67%', text)
        self.assertIn('买入金额占钱包总额 >80% 的地址数/总体占比: 1 / 33.33%', text)
        self.assertIn('购买后无法再次售出的诚实节点数/总体占比: 2 / 66.67%', text)
        self.assertIn('Mint 到被诚实节点购买平均时间(跨 seed 均值): 200.0 秒', text)
        self.assertIn('Mint 到首次转手平均时间(跨 seed 均值): 400.0 秒', text)
        self.assertIn('候选合约平均唯一接收钱包数(跨 seed 均值): 6.0', text)
        self.assertIn('Alpha (0xseed1)', text)
        self.assertIn('诚实购买额=2.5', text)
        self.assertIn('>60%=1/50.00%', text)
        self.assertIn('套牢=1/50.00%', text)
        self.assertIn('诚实购买时长=100.0秒', text)


class OutputNamingTests(unittest.TestCase):
    def test_default_output_basename_uses_seed_collection_name(self):
        payload = {
            'seed_contract': {
                'name': 'Bored Ape Yacht Club',
                'contract_address': '0xseed',
            }
        }

        basename = mod.default_output_basename(payload)

        self.assertEqual(basename, 'top_contract_analysis__bored_ape_yacht_club')

    def test_main_writes_default_json_and_markdown_outputs(self):
        payload = {
            'seed_contract': {
                'chain': 'ethereum',
                'contract_address': '0xseed',
                'token_type': 'ERC721',
                'contract_deployer': '0xcreator',
                'deployed_block_number': 123,
                'name': 'Bored Ape Yacht Club',
                'symbol': 'BAYC',
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
        tmpdir = Path('D:/code/solidity/get-nft-data/output/test-top-contract-analysis') / str(uuid.uuid4())
        tmpdir.mkdir(parents=True, exist_ok=True)
        cwd = Path.cwd()
        try:
            with patch.object(cli_mod, 'analyze_seed_contract', return_value=payload):
                with patch('sys.argv', [
                    'top_contract_analysis',
                    '--chain', 'ethereum',
                    '--seed-contract-address', '0xseed',
                    '--alchemy-api-key', 'alchemy',
                ]):
                    os.chdir(tmpdir)
                    result = mod.main()

            self.assertEqual(result, 0)
            json_path = tmpdir / 'result' / 'top_contract_analysis__bored_ape_yacht_club.json'
            md_path = tmpdir / 'result' / 'top_contract_analysis__bored_ape_yacht_club.md'
            self.assertTrue(json_path.exists())
            self.assertTrue(md_path.exists())
            self.assertIn('Bored Ape Yacht Club', json_path.read_text(encoding='utf-8'))
            self.assertIn('# Top NFT 合约重复样本分析报告', md_path.read_text(encoding='utf-8'))
        finally:
            os.chdir(cwd)
            for path in sorted(tmpdir.glob('**/*'), reverse=True):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()
            if tmpdir.exists():
                tmpdir.rmdir()

    def test_main_passes_async_concurrency_args(self):
        payload = {'seed_contract': {'name': 'Bored Ape Yacht Club', 'contract_address': '0xseed'}}

        with patch.object(cli_mod, 'analyze_seed_contract', return_value=payload) as mocked:
            with patch.object(cli_mod, 'write_default_outputs'):
                with patch('sys.argv', [
                    'top_contract_analysis',
                    '--chain', 'ethereum',
                    '--seed-contract-address', '0xseed',
                    '--alchemy-api-key', 'alchemy',
                    '--api-max-concurrency', '21',
                    '--contract-max-concurrency', '5',
                    '--sale-metric-max-concurrency', '7',
                ]):
                    result = mod.main()

        self.assertEqual(result, 0)
        self.assertEqual(mocked.call_args.kwargs['api_max_concurrency'], 21)
        self.assertEqual(mocked.call_args.kwargs['contract_max_concurrency'], 5)
        self.assertEqual(mocked.call_args.kwargs['sale_metric_max_concurrency'], 7)


if __name__ == '__main__':
    unittest.main()
