import asyncio
from types import SimpleNamespace
from unittest.mock import patch

import top_contract_analysis as mod
import top_contract_analysis.analysis as analysis_mod


class StubProgressReporter:
    def __init__(self):
        self.contract_totals = []
        self.contract_completions = []

    def on_seed_stage(self, stage, **kwargs):
        del stage, kwargs

    def on_high_confidence_contracts_started(self, *, total):
        self.contract_totals.append(total)

    def on_high_confidence_contract_completed(self, *, contract_address, completed, total):
        self.contract_completions.append((contract_address, completed, total))

    def on_seed_completed(self, **kwargs):
        del kwargs


def test_async_analyze_seed_contract_includes_low_confidence_contracts_in_stats():
    async def _run():
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
                token_uri='ipfs://seed/1',
                image_uri='ipfs://seed/1.png',
            )
        ]
        candidates = [
            mod.DuplicateCandidate(
                contract_address='0xdup-high',
                token_id='1',
                match_reasons=('token_uri_match',),
                confidence='high',
                token_uri='ipfs://seed/1',
                image_uri='',
                name='Azuki Mirror #1',
                symbol='AZUKI',
            ),
            mod.DuplicateCandidate(
                contract_address='0xdup-low',
                token_id='2',
                match_reasons=('name_match',),
                confidence='low',
                token_uri='ipfs://seed/2',
                image_uri='',
                name='Azuki Styled #2',
                symbol='AZUKI',
            ),
        ]
        feature_store = SimpleNamespace(load_snapshot=lambda *args, **kwargs: mod.DatabaseSnapshot())
        reporter = StubProgressReporter()

        async def fake_contract_metadata_async(**kwargs):
            del kwargs
            return seed_meta

        async def fake_seed_nfts_async(**kwargs):
            del kwargs
            return seed_nfts

        async def fake_license_sample_async(**kwargs):
            del kwargs
            return {}

        async def fake_contract_analysis(**kwargs):
            confidence = kwargs['contract_confidence']
            contract_address = kwargs['contract_address']
            suffix = 'high' if confidence == 'high' else 'low'
            sample_seconds = 30 if confidence == 'high' else 90
            return {
                'contract_address': contract_address,
                'status': confidence,
                'candidate_count': len(kwargs['contract_candidates']),
                'match_reasons': ['token_uri_match'] if confidence == 'high' else ['name_match'],
                'address_signals': {
                    'mint_to_first_transfer_seconds': sample_seconds,
                    'unique_receiver_count': 1,
                },
                'victim_signals': {},
                'infringing_tokens': [
                    {
                        'contract_address': contract_address,
                        'token_id': suffix,
                        'minter_address': f'0xminter-{suffix}',
                        'candidate_open_license': False,
                        'official_or_legit_reissue': False,
                    }
                ],
                'malicious_addresses': [
                    {
                        'address': f'0xmalicious-{suffix}',
                        'mint_role': True,
                        'wash_cycle_count': 0,
                        'star_out_degree': 0,
                        'rapid_spread_contracts': [],
                        'evidence_contracts': [contract_address],
                    }
                ],
                'honest_addresses': [
                    {
                        'address': f'0xhonest-{suffix}',
                        'mint_to_honest_seconds_samples': [sample_seconds],
                    }
                ],
                'honest_address_stats': {},
                'victim_addresses': [
                    {
                        'address': f'0xbuyer-{suffix}',
                        'buy_amount_eth': 1.0,
                        'last_buy_amount_eth': 1.0,
                        'buy_asset_ratio': 0.5,
                        'is_stuck': confidence == 'low',
                    }
                ],
                'fraud_trade_stats': {},
            }

        with patch.object(analysis_mod, 'fetch_contract_metadata_async', side_effect=fake_contract_metadata_async), \
             patch.object(analysis_mod, 'fetch_seed_contract_nfts_async', side_effect=fake_seed_nfts_async), \
             patch.object(analysis_mod, 'fetch_license_sample_async', side_effect=fake_license_sample_async), \
             patch.object(analysis_mod, 'find_duplicate_candidates', return_value=candidates), \
             patch.object(analysis_mod, '_analyze_high_confidence_contract_async', side_effect=fake_contract_analysis):
            payload = await analysis_mod.async_analyze_seed_contract(
                chain='ethereum',
                seed_contract_address='0xseed',
                alchemy_api_key='realistic-api-key-123',
                feature_store=feature_store,
                progress_reporter=reporter,
            )

        return payload, reporter

    payload, reporter = asyncio.run(_run())
    summary = payload['report_summary']

    assert summary['candidate_contract_count'] == 2
    assert summary['high_confidence_contract_count'] == 1
    assert summary['low_confidence_contract_count'] == 1
    assert summary['infringing_nft_count'] == 2
    assert summary['malicious_address_count'] == 2
    assert summary['honest_address_count'] == 2
    assert summary['buy_asset_ratio_known_address_count'] == 2
    assert summary['stuck_honest_address_count'] == 1
    assert set(payload['address_signals']) == {'0xdup-high', '0xdup-low'}
    assert {item['contract_address'] for item in payload['infringing_tokens']} == {'0xdup-high', '0xdup-low'}
    assert payload['suspected_infringing_duplicates_high_confidence'] == [
        {
            'contract_address': '0xdup-high',
            'candidate_count': 1,
            'match_reasons': ['token_uri_match'],
        }
    ]
    assert payload['suspected_infringing_duplicates_low_confidence'] == [
        {
            'contract_address': '0xdup-low',
            'candidate_count': 1,
            'match_reasons': ['name_match'],
        }
    ]
    assert reporter.contract_totals == [2]
    assert reporter.contract_completions == [
        ('0xdup-high', 1, 2),
        ('0xdup-low', 2, 2),
    ]
