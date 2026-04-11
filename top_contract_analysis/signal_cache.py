from __future__ import annotations

import json
from threading import RLock
from typing import Any, Sequence

import duckdb

from . import OwnerBalance, TransferRecord
from .rust_bridge import analyze_transfer_signals, analyze_victim_signals_from_active_sellers


def _expected_columns() -> list[str]:
    return [
        'chain',
        'contract_address',
        'token_type',
        'mint_recipients_json',
        'active_sellers_json',
        'address_signals_json',
        'victim_signals_json',
    ]


class ContractSignalCache:
    def __init__(self, database_path: str = ':memory:') -> None:
        self._lock = RLock()
        self._conn = duckdb.connect(database=database_path)
        with self._lock:
            existing = self._conn.execute(
                '''
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'contract_signal_cache'
                ORDER BY ordinal_position
                '''
            ).fetchall()
            if existing and [row[0] for row in existing] != _expected_columns():
                self._conn.execute('DROP TABLE contract_signal_cache')
            self._conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS contract_signal_cache (
                    chain VARCHAR NOT NULL,
                    contract_address VARCHAR NOT NULL,
                    token_type VARCHAR NOT NULL,
                    mint_recipients_json VARCHAR NOT NULL,
                    active_sellers_json VARCHAR NOT NULL,
                    address_signals_json VARCHAR NOT NULL,
                    victim_signals_json VARCHAR NOT NULL,
                    PRIMARY KEY (chain, contract_address, token_type)
                )
                '''
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def get(self, *, chain: str, contract_address: str, token_type: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                '''
                SELECT mint_recipients_json, active_sellers_json, address_signals_json, victim_signals_json
                FROM contract_signal_cache
                WHERE chain = ? AND contract_address = ? AND token_type = ?
                ''',
                [chain, contract_address.lower(), token_type],
            ).fetchone()
        if row is None:
            return None
        mint_recipients_json, active_sellers_json, address_signals_json, victim_signals_json = row
        return {
            'mint_recipients': json.loads(mint_recipients_json or '[]'),
            'active_sellers': json.loads(active_sellers_json or '[]'),
            'address_signals': json.loads(address_signals_json or '{}'),
            'victim_signals': json.loads(victim_signals_json) if victim_signals_json else None,
        }

    def put(
        self,
        *,
        chain: str,
        contract_address: str,
        token_type: str,
        transfers: Sequence[TransferRecord],
        owners: Sequence[OwnerBalance],
    ) -> None:
        mint_recipients = sorted(
            {
                item.to_address
                for item in transfers
                if item.from_address == '0x0000000000000000000000000000000000000000' and item.to_address
            }
        )
        active_sellers = sorted(
            {
                item.from_address
                for item in transfers
                if item.from_address and item.from_address != '0x0000000000000000000000000000000000000000'
            }
        )
        address_signals = analyze_transfer_signals(transfers)
        victim_signals = analyze_victim_signals_from_active_sellers(active_sellers, owners) if owners else None
        with self._lock:
            self._conn.execute(
                '''
                INSERT OR REPLACE INTO contract_signal_cache (
                    chain, contract_address, token_type, mint_recipients_json, active_sellers_json,
                    address_signals_json, victim_signals_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ''',
                [
                    chain,
                    contract_address.lower(),
                    token_type,
                    json.dumps(mint_recipients, ensure_ascii=False),
                    json.dumps(active_sellers, ensure_ascii=False),
                    json.dumps(address_signals, ensure_ascii=False),
                    json.dumps(victim_signals, ensure_ascii=False) if victim_signals is not None else '',
                ],
            )
