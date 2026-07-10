#!/usr/bin/env python3
"""Fetch one global OpenSea Top collection ranking as chain/address pairs."""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_TOP_COLLECTIONS_URL = "https://api.opensea.io/api/v2/collections/top"
DEFAULT_SORT_BY = "thirty_days_volume"
DEFAULT_CHAINS = ("ethereum", "base", "polygon", "solana")
EVM_CHAINS = frozenset({"ethereum", "base", "polygon"})
ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
BASE58_INDEX = {char: index for index, char in enumerate(BASE58_ALPHABET)}
COLLECTION_KEYS = ("collections", "top_collections", "data", "results")
CONTRACT_LIST_KEYS = ("contracts", "primary_asset_contracts", "asset_contracts")


@dataclass(frozen=True)
class ContractPair:
    chain: str
    address: str
    raw_chain: str | None = field(default=None, compare=False, hash=False)


@dataclass(frozen=True)
class RankedCollection:
    global_rank: int
    slug: str
    name: str | None
    ranking_value: Any
    contract_pairs: tuple[ContractPair, ...]


def parse_json_response(raw: bytes) -> dict[str, Any]:
    value = json.loads(raw.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("OpenSea response must be a JSON object")
    return value


def collection_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for key in COLLECTION_KEYS:
        if key not in payload:
            continue
        value = payload[key]
        if not isinstance(value, list):
            raise ValueError(f"OpenSea response field {key!r} must be a list")
        return [item for item in value if isinstance(item, dict)]
    raise ValueError("OpenSea response does not contain a collection list")


def contract_pair_from_value(
    value: Any, selected_chains: set[str]
) -> ContractPair | None:
    if not isinstance(value, dict):
        return None
    raw_chain_value = value.get("chain") or value.get("chain_identifier")
    if not isinstance(raw_chain_value, str):
        return None
    raw_chain_value = raw_chain_value.strip()
    chain = raw_chain_value.lower()
    if chain not in selected_chains:
        return None
    for key in ("address", "contract_address", "contractAddress"):
        address = normalize_contract_address(str(value.get(key, "")), chain)
        if address is not None:
            return ContractPair(
                chain=chain,
                address=address,
                raw_chain=raw_chain_value if raw_chain_value != chain else None,
            )
    return None


def collection_contract_pairs(
    collection: dict[str, Any], selected_chains: set[str]
) -> tuple[ContractPair, ...]:
    pairs: list[ContractPair] = []
    seen: set[ContractPair] = set()
    for key in CONTRACT_LIST_KEYS:
        values = collection.get(key)
        if not isinstance(values, list):
            continue
        for value in values:
            pair = contract_pair_from_value(value, selected_chains)
            if pair is not None and pair not in seen:
                seen.add(pair)
                pairs.append(pair)
    direct = contract_pair_from_value(collection, selected_chains)
    if direct is not None and direct not in seen:
        pairs.append(direct)
    return tuple(pairs)


def collection_ranking_value(collection: dict[str, Any]) -> Any:
    for container in (collection, collection.get("stats")):
        if isinstance(container, dict):
            for key in ("thirty_days_volume", "thirty_day_volume"):
                if key in container:
                    return container[key]
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


def build_top_collections_url(
    base_url: str,
    *,
    chains: list[str],
    page_size: int,
    cursor: str | None,
) -> str:
    query: dict[str, str | int] = {
        "chains": ",".join(chains),
        "limit": page_size,
        "sort_by": DEFAULT_SORT_BY,
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


def collect_ranked_collections(
    *,
    api_key: str,
    chains: list[str],
    limit: int,
    page_size: int,
    top_collections_url: str,
    timeout: float,
    fetcher=fetch_bytes,
) -> list[RankedCollection]:
    ranked: list[RankedCollection] = []
    selected_chains = set(chains)
    cursor: str | None = None
    seen_cursors: set[str] = set()

    while len(ranked) < limit:
        url = build_top_collections_url(
            top_collections_url,
            chains=chains,
            page_size=min(page_size, limit - len(ranked)),
            cursor=cursor,
        )
        payload = parse_json_response(
            fetcher(url, api_key=api_key, timeout=timeout)
        )
        for collection in collection_items(payload):
            pairs = collection_contract_pairs(collection, selected_chains)
            if not pairs:
                continue
            ranked.append(
                RankedCollection(
                    global_rank=len(ranked) + 1,
                    slug=str(collection.get("collection") or ""),
                    name=(
                        str(collection["name"])
                        if collection.get("name") is not None
                        else None
                    ),
                    ranking_value=collection_ranking_value(collection),
                    contract_pairs=pairs,
                )
            )
            if len(ranked) == limit:
                break

        next_value = next_cursor(payload)
        if len(ranked) == limit:
            break
        if not next_value:
            break
        if next_value in seen_cursors:
            raise ValueError(f"repeated pagination cursor: {next_value}")
        seen_cursors.add(next_value)
        cursor = next_value

    if len(ranked) != limit:
        raise ValueError(
            f"requested {limit} analyzable collections but collected {len(ranked)}"
        )
    return ranked


def manifest_pairs(ranked: list[RankedCollection]) -> list[ContractPair]:
    pairs: list[ContractPair] = []
    seen: set[ContractPair] = set()
    for collection in ranked:
        for pair in collection.contract_pairs:
            if pair not in seen:
                seen.add(pair)
                pairs.append(pair)
    return pairs


def render_manifest_csv(ranked: list[RankedCollection]) -> str:
    handle = io.StringIO(newline="")
    writer = csv.writer(handle, lineterminator="\n")
    writer.writerow(["chain", "address"])
    writer.writerows(
        (pair.chain, pair.address) for pair in manifest_pairs(ranked)
    )
    return handle.getvalue()


def audit_payload(
    ranked: list[RankedCollection],
    chains: list[str],
    requested_limit: int,
) -> dict[str, Any]:
    collections = []
    for item in ranked:
        contract_pairs = []
        for pair in item.contract_pairs:
            value = {"chain": pair.chain, "address": pair.address}
            if pair.raw_chain is not None:
                value["raw_chain"] = pair.raw_chain
            contract_pairs.append(value)
        collections.append(
            {
                "global_rank": item.global_rank,
                "slug": item.slug,
                "name": item.name,
                "ranking_criterion": DEFAULT_SORT_BY,
                "ranking_value": item.ranking_value,
                "contract_pairs": contract_pairs,
            }
        )
    return {
        "ranking_criterion": DEFAULT_SORT_BY,
        "chains": chains,
        "requested_collection_limit": requested_limit,
        "collections": collections,
    }


def write_rank_outputs(
    *,
    csv_path: Path,
    audit_path: Path,
    ranked: list[RankedCollection],
    chains: list[str],
    requested_limit: int,
) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    csv_tmp = csv_path.with_name(f".{csv_path.name}.{os.getpid()}.tmp")
    audit_tmp = audit_path.with_name(f".{audit_path.name}.{os.getpid()}.tmp")
    try:
        csv_tmp.write_text(
            render_manifest_csv(ranked),
            encoding="utf-8",
            newline="\n",
        )
        audit_tmp.write_text(
            json.dumps(
                audit_payload(ranked, chains, requested_limit),
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
            newline="\n",
        )
        os.replace(csv_tmp, csv_path)
        os.replace(audit_tmp, audit_path)
    finally:
        csv_tmp.unlink(missing_ok=True)
        audit_tmp.unlink(missing_ok=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch one globally ranked OpenSea Top collection population "
            "and export chain/address contract pairs"
        )
    )
    parser.add_argument("--chains", nargs="+", default=list(DEFAULT_CHAINS))
    parser.add_argument("--output-dir", type=Path, default=Path("../seeds"))
    parser.add_argument("--contracts-output", type=Path)
    parser.add_argument("--audit-output", type=Path)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument(
        "--top-collections-url",
        default=DEFAULT_TOP_COLLECTIONS_URL,
        help="OpenSea globally ranked collections API URL",
    )
    parser.add_argument("--api-key", default="2d17a25e68714720883ac996f5459b17")
    parser.add_argument("--api-key-env", default="OPENSEA_API_KEY")
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args(argv)
    args.chains = list(
        dict.fromkeys(chain.strip().lower() for chain in args.chains)
    )
    unsupported = [chain for chain in args.chains if chain not in DEFAULT_CHAINS]
    if unsupported:
        parser.error(f"unsupported chain(s): {', '.join(unsupported)}")
    args.contracts_output = (
        args.contracts_output or args.output_dir / "top_contracts.csv"
    )
    args.audit_output = args.audit_output or args.output_dir / "top_collections.json"
    contracts_path = os.path.normcase(os.path.abspath(args.contracts_output))
    audit_path = os.path.normcase(os.path.abspath(args.audit_output))
    if contracts_path == audit_path:
        parser.error(
            "--contracts-output and --audit-output must identify different files"
        )
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
        ranked = collect_ranked_collections(
            api_key=api_key,
            chains=args.chains,
            limit=args.limit,
            page_size=args.page_size,
            top_collections_url=args.top_collections_url,
            timeout=args.timeout,
        )
    except HTTPError as exc:
        print(f"OpenSea HTTP error {exc.code}: {exc.reason}", file=sys.stderr)
        return 1
    except (URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
        print(f"OpenSea request failed: {exc}", file=sys.stderr)
        return 1

    write_rank_outputs(
        csv_path=args.contracts_output,
        audit_path=args.audit_output,
        ranked=ranked,
        chains=args.chains,
        requested_limit=args.limit,
    )
    print(
        f"wrote {len(manifest_pairs(ranked))} contract pairs from "
        f"{len(ranked)} ranked collections to {args.contracts_output}"
    )
    print(f"wrote ranking audit to {args.audit_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
