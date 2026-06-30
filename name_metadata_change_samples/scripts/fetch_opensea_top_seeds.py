#!/usr/bin/env python3
"""Fetch trending OpenSea collection addresses into chain-specific seed files."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_TRENDING_COLLECTIONS_URL = (
    "https://api.opensea.io/api/v2/collections/trending"
)
DEFAULT_TIMEFRAME = "thirty_days"
DEFAULT_CHAINS = ("ethereum", "base", "polygon", "solana")
EVM_CHAINS = frozenset({"ethereum", "base", "polygon"})
ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
BASE58_INDEX = {char: index for index, char in enumerate(BASE58_ALPHABET)}
COLLECTION_KEYS = ("collections", "top_collections", "data", "results")
CONTRACT_LIST_KEYS = ("contracts", "primary_asset_contracts", "asset_contracts")


def parse_json_response(raw: bytes) -> dict[str, Any]:
    value = json.loads(raw.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("OpenSea response must be a JSON object")
    return value


def extract_trending_collection_addresses(
    payload: dict[str, Any], chain: str
) -> list[str]:
    addresses: list[str] = []
    seen: set[str] = set()
    for collection in collection_items(payload):
        for address in collection_contract_addresses(collection, chain):
            if address not in seen:
                seen.add(address)
                addresses.append(address)
    return addresses


def collection_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for key in COLLECTION_KEYS:
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def collection_contract_addresses(collection: dict[str, Any], chain: str) -> list[str]:
    addresses: list[str] = []
    for key in CONTRACT_LIST_KEYS:
        contracts = collection.get(key)
        if isinstance(contracts, list):
            for contract in contracts:
                address = contract_address_from_value(contract, chain)
                if address:
                    addresses.append(address)
    direct_address = contract_address_from_value(collection, chain)
    if direct_address:
        addresses.append(direct_address)
    return addresses


def contract_address_from_value(value: Any, chain: str) -> str | None:
    if isinstance(value, str):
        return normalize_contract_address(value, chain)
    if not isinstance(value, dict):
        return None
    item_chain = value.get("chain") or value.get("chain_identifier")
    if item_chain and str(item_chain).lower() != chain.lower():
        return None
    for key in ("address", "contract_address", "contractAddress"):
        address = normalize_contract_address(str(value.get(key, "")), chain)
        if address:
            return address
    return None


def decode_base58(value: str) -> bytes:
    number = 0
    for char in value:
        if char not in BASE58_INDEX:
            raise ValueError("invalid Base58 character")
        number = number * 58 + BASE58_INDEX[char]
    body = number.to_bytes((number.bit_length() + 7) // 8, "big")
    leading_zeroes = len(value) - len(value.lstrip("1"))
    return b"\x00" * leading_zeroes + body


def normalize_contract_address(value: str, chain: str) -> str | None:
    address = value.strip()
    normalized_chain = chain.strip().lower()
    if normalized_chain in EVM_CHAINS:
        lowered = address.lower()
        return lowered if ADDRESS_RE.fullmatch(lowered) else None
    if normalized_chain == "solana":
        try:
            return address if len(decode_base58(address)) == 32 else None
        except ValueError:
            return None
    return None


def next_cursor(payload: dict[str, Any]) -> str | None:
    for key in ("cursor", "next", "next_cursor"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def build_trending_collections_url(
    base_url: str,
    *,
    chain: str,
    page_size: int,
    timeframe: str,
    cursor: str | None,
) -> str:
    query: dict[str, str | int] = {
        "chains": chain,
        "limit": page_size,
        "timeframe": timeframe,
    }
    if cursor:
        query["cursor"] = cursor
    return f"{base_url}?{urlencode(query)}"


def fetch_bytes(url: str, api_key: str, timeout: float) -> bytes:
    request = Request(
        url,
        headers={
            "accept": "application/json",
            "user-agent": "name-metadata-change-samples/1.0",
            "x-api-key": api_key,
        },
    )
    with urlopen(request, timeout=timeout) as response:
        return response.read()


def collect_seed_addresses(
    *,
    api_key: str,
    chain: str,
    limit: int,
    page_size: int,
    trending_collections_url: str,
    timeframe: str,
    timeout: float,
) -> list[str]:
    addresses: list[str] = []
    seen: set[str] = set()
    cursor: str | None = None

    while len(addresses) < limit:
        url = build_trending_collections_url(
            trending_collections_url,
            chain=chain,
            page_size=min(page_size, limit - len(addresses)),
            timeframe=timeframe,
            cursor=cursor,
        )
        payload = parse_json_response(fetch_bytes(url, api_key=api_key, timeout=timeout))
        page_addresses = extract_trending_collection_addresses(payload, chain)
        for address in page_addresses:
            if address not in seen:
                seen.add(address)
                addresses.append(address)
                if len(addresses) >= limit:
                    break

        cursor = next_cursor(payload)
        if not cursor or not page_addresses:
            break

    return addresses


def collect_seed_addresses_by_chain(
    *,
    chains: list[str],
    collector=collect_seed_addresses,
    **collector_kwargs: Any,
) -> dict[str, list[str]]:
    return {
        chain: collector(chain=chain, **collector_kwargs)
        for chain in chains
    }


def format_seeds(addresses: list[str], chain: str) -> str:
    normalized = []
    for address in addresses:
        canonical = normalize_contract_address(address, chain)
        if canonical is None:
            raise ValueError(f"invalid {chain} contract address: {address!r}")
        normalized.append(canonical)
    return "".join(f"{address}\n" for address in normalized)


def write_seeds(path: Path, addresses: list[str], chain: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(format_seeds(addresses, chain), encoding="utf-8", newline="\n")


def seed_output_path(
    *,
    chain: str,
    selected_chains: list[str],
    output: Path | None,
    output_dir: Path,
) -> Path:
    if len(selected_chains) == 1 and output is not None:
        return output
    return output_dir / f"{chain}.seeds.txt"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch trending OpenSea collection addresses into per-chain seed files"
    )
    chain_group = parser.add_mutually_exclusive_group()
    chain_group.add_argument("--chain")
    chain_group.add_argument("--chains", nargs="+")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--output-dir", type=Path, default=Path("../seeds"))
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    parser.add_argument(
        "--trending-collections-url",
        "--top-collections-url",
        dest="trending_collections_url",
        default=DEFAULT_TRENDING_COLLECTIONS_URL,
        help="OpenSea trending collections API URL",
    )
    parser.add_argument("--api-key", default="2d17a25e68714720883ac996f5459b17")
    parser.add_argument("--api-key-env", default="OPENSEA_API_KEY")
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args(argv)
    selected_chains = [args.chain] if args.chain else (args.chains or list(DEFAULT_CHAINS))
    args.chains = list(dict.fromkeys(chain.strip().lower() for chain in selected_chains))
    unsupported = [chain for chain in args.chains if chain not in DEFAULT_CHAINS]
    if unsupported:
        parser.error(f"unsupported chain(s): {', '.join(unsupported)}")
    if args.output is not None and len(args.chains) != 1:
        parser.error("--output requires exactly one selected chain")
    if args.chain and args.output is None:
        args.output = Path("../seeds.txt")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.limit <= 0:
        raise SystemExit("--limit must be positive")
    if not 1 <= args.page_size <= 100:
        raise SystemExit("--page-size must be between 1 and 100")
    api_key = args.api_key or os.getenv(args.api_key_env)
    if not api_key:
        raise SystemExit(
            f"missing OpenSea API key; pass --api-key or set {args.api_key_env}"
        )
    try:
        addresses_by_chain = collect_seed_addresses_by_chain(
            chains=args.chains,
            api_key=api_key,
            limit=args.limit,
            page_size=args.page_size,
            trending_collections_url=args.trending_collections_url,
            timeframe=args.timeframe,
            timeout=args.timeout,
        )
    except HTTPError as exc:
        print(f"OpenSea HTTP error {exc.code}: {exc.reason}", file=sys.stderr)
        return 1
    except (URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
        print(f"OpenSea request failed: {exc}", file=sys.stderr)
        return 1

    for chain, addresses in addresses_by_chain.items():
        if not addresses:
            raise SystemExit(f"no contract addresses collected for {chain}")
        output_path = seed_output_path(
            chain=chain,
            selected_chains=args.chains,
            output=args.output,
            output_dir=args.output_dir,
        )
        write_seeds(output_path, addresses, chain)
        print(f"wrote {len(addresses)} {chain} addresses to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
