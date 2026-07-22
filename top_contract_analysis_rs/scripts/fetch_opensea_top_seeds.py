#!/usr/bin/env python3
"""Fetch independent Top-100 contract rankings for the supported chains.

EVM rankings come from OpenSea. Solana uses Magic Eden because OpenSea's
collections API does not currently serve Solana.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import shutil
import re
import secrets
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


DEFAULT_TOP_COLLECTIONS_URL = "https://api.opensea.io/api/v2/collections/top"
DEFAULT_SOLANA_TOP_COLLECTIONS_URL = (
    "https://api-mainnet.magiceden.dev/v2/marketplace/popular_collections"
)
DEFAULT_MAGIC_EDEN_API_URL = "https://api-mainnet.magiceden.dev/v2"
DEFAULT_HELIUS_RPC_URL = "https://mainnet.helius-rpc.com/"
DEFAULT_SORT_BY = "thirty_days_volume"
SOLANA_SORT_BY = "magic_eden_30d_popularity"
DEFAULT_CHAINS = ("ethereum", "base", "polygon", "solana")
EVM_CHAINS = frozenset({"ethereum", "base", "polygon"})
OPENSEA_CHAIN_NAMES = {"polygon": "matic"}
INTERNAL_CHAIN_NAMES = {"matic": "polygon"}
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
class RankedContract:
    chain: str
    address: str
    chain_contract_rank: int
    collection_rank: int
    slug: str
    name: str | None
    ranking_value: Any
    raw_chain: str | None = None
    provider: str = "opensea"
    ranking_criterion: str = DEFAULT_SORT_BY


def parse_json_response(raw: bytes) -> dict[str, Any]:
    value = json.loads(raw.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("OpenSea response must be a JSON object")
    return value


def parse_magic_eden_response(raw: bytes) -> list[dict[str, Any]]:
    value = json.loads(raw.decode("utf-8"))
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        collections = value.get("collections")
        if isinstance(collections, list):
            return [item for item in collections if isinstance(item, dict)]
    raise ValueError("Magic Eden response does not contain a collection list")


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
    chain = INTERNAL_CHAIN_NAMES.get(raw_chain_value.lower(), raw_chain_value.lower())
    if chain not in selected_chains:
        return None
    for key in ("address", "contract_address", "contractAddress"):
        address = normalize_contract_address(str(value.get(key, "")), chain)
        if address is not None:
            return ContractPair(
                chain=chain,
                address=address,
                raw_chain=raw_chain_value,
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
        "chains": ",".join(OPENSEA_CHAIN_NAMES.get(chain, chain) for chain in chains),
        "limit": page_size,
        "sort_by": DEFAULT_SORT_BY,
    }
    if cursor:
        query["cursor"] = cursor
    return f"{base_url}?{urlencode(query)}"


def build_magic_eden_top_collections_url(base_url: str) -> str:
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{urlencode({'timeRange': '30d'})}"


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


def fetch_public_bytes(url: str, timeout: float) -> bytes:
    request = Request(
        url,
        headers={
            "accept": "application/json",
            "user-agent": "name-metadata-change-samples/1.0",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        return response.read()


def fetch_json_rpc_bytes(url: str, payload: dict[str, Any], timeout: float) -> bytes:
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "accept": "application/json",
            "content-type": "application/json",
            "user-agent": "name-metadata-change-samples/1.0",
        },
        method="POST",
    )
    with urlopen(request, timeout=timeout) as response:
        return response.read()


def magic_eden_collection_address(collection: dict[str, Any]) -> str | None:
    for key in (
        "onChainCollectionAddress",
        "on_chain_collection_address",
        "collectionAddress",
        "collection_address",
    ):
        address = normalize_contract_address(str(collection.get(key, "")), "solana")
        if address is not None:
            return address
    return None


def magic_eden_sample_mint(raw: bytes) -> str | None:
    value = json.loads(raw.decode("utf-8"))
    items = value if isinstance(value, list) else None
    if isinstance(value, dict):
        for key in ("items", "listings", "results"):
            if isinstance(value.get(key), list):
                items = value[key]
                break
    if not isinstance(items, list):
        raise ValueError("Magic Eden listings response must contain a list")
    for item in items:
        if not isinstance(item, dict):
            continue
        token = item.get("token") if isinstance(item.get("token"), dict) else {}
        for candidate in (
            item.get("tokenMint"),
            item.get("mintAddress"),
            item.get("mint"),
            token.get("mintAddress"),
        ):
            mint = normalize_contract_address(str(candidate or ""), "solana")
            if mint is not None:
                return mint
    return None


def helius_collection_address_for_mint(
    mint: str,
    *,
    rpc_url: str,
    timeout: float,
    fetcher=fetch_json_rpc_bytes,
) -> str | None:
    payload = parse_json_response(
        fetcher(
            rpc_url,
            {
                "jsonrpc": "2.0",
                "id": f"seed-collection-{mint}",
                "method": "getAsset",
                "params": {"id": mint},
            },
            timeout=timeout,
        )
    )
    if payload.get("error") is not None:
        raise ValueError(f"Helius getAsset failed for {mint}: {payload['error']}")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError(f"Helius getAsset response omitted result for {mint}")
    grouping = result.get("grouping")
    if not isinstance(grouping, list):
        return None
    for group in grouping:
        if not isinstance(group, dict) or group.get("group_key") != "collection":
            continue
        address = normalize_contract_address(str(group.get("group_value", "")), "solana")
        if address is not None:
            return address
    return None


def resolve_magic_eden_collection_address(
    collection: dict[str, Any],
    *,
    magic_eden_api_url: str,
    helius_rpc_url: str,
    timeout: float,
    magic_eden_fetcher=fetch_public_bytes,
    helius_fetcher=fetch_json_rpc_bytes,
) -> str | None:
    direct = magic_eden_collection_address(collection)
    if direct is not None:
        return direct
    symbol = str(collection.get("symbol") or "").strip()
    if not symbol:
        return None
    collection_url = (
        f"{magic_eden_api_url.rstrip('/')}/collections/{quote(symbol, safe='')}"
    )
    mint = None
    for resource in ("listings", "activities"):
        sample_url = (
            f"{collection_url}/{resource}?{urlencode({'offset': 0, 'limit': 1})}"
        )
        mint = magic_eden_sample_mint(
            magic_eden_fetcher(sample_url, timeout=timeout)
        )
        if mint is not None:
            break
    if mint is None:
        return None
    return helius_collection_address_for_mint(
        mint,
        rpc_url=helius_rpc_url,
        timeout=timeout,
        fetcher=helius_fetcher,
    )


def magic_eden_ranking_value(collection: dict[str, Any]) -> Any:
    for key in ("volume", "volumeAll", "volume_all", "totalVolume"):
        if key in collection:
            return collection[key]
    return None


def collect_ranked_solana_contracts(
    *,
    limit: int,
    top_collections_url: str,
    magic_eden_api_url: str,
    helius_rpc_url: str,
    timeout: float,
    magic_eden_fetcher=fetch_public_bytes,
    helius_fetcher=fetch_json_rpc_bytes,
) -> list[RankedContract]:
    url = build_magic_eden_top_collections_url(top_collections_url)
    collections = parse_magic_eden_response(
        magic_eden_fetcher(url, timeout=timeout)
    )
    ranked: list[RankedContract] = []
    seen_addresses: set[str] = set()

    for collection_rank, collection in enumerate(collections, start=1):
        address = resolve_magic_eden_collection_address(
            collection,
            magic_eden_api_url=magic_eden_api_url,
            helius_rpc_url=helius_rpc_url,
            timeout=timeout,
            magic_eden_fetcher=magic_eden_fetcher,
            helius_fetcher=helius_fetcher,
        )
        if address is None or address in seen_addresses:
            continue
        seen_addresses.add(address)
        ranked.append(
            RankedContract(
                chain="solana",
                address=address,
                chain_contract_rank=len(ranked) + 1,
                collection_rank=collection_rank,
                slug=str(collection.get("symbol") or collection.get("collection") or ""),
                name=(
                    str(collection["name"])
                    if collection.get("name") is not None
                    else None
                ),
                ranking_value=magic_eden_ranking_value(collection),
                raw_chain="solana",
                provider="magic_eden",
                ranking_criterion=SOLANA_SORT_BY,
            )
        )
        if len(ranked) == limit:
            break

    if len(ranked) != limit:
        raise ValueError(
            f"requested {limit} Solana collection addresses from Magic Eden but "
            f"collected {len(ranked)}"
        )
    return ranked


def collect_ranked_contracts_for_chain(
    *,
    api_key: str,
    chain: str,
    limit: int,
    page_size: int,
    top_collections_url: str,
    timeout: float,
    fetcher=fetch_bytes,
) -> list[RankedContract]:
    ranked: list[RankedContract] = []
    seen_addresses: set[str] = set()
    cursor: str | None = None
    seen_cursors: set[str] = set()
    collection_rank = 0

    while len(ranked) < limit:
        url = build_top_collections_url(
            top_collections_url,
            chains=[chain],
            page_size=page_size,
            cursor=cursor,
        )
        payload = parse_json_response(
            fetcher(url, api_key=api_key, timeout=timeout)
        )
        for collection in collection_items(payload):
            collection_rank += 1
            for pair in collection_contract_pairs(collection, {chain}):
                if pair.address in seen_addresses:
                    continue
                seen_addresses.add(pair.address)
                ranked.append(
                    RankedContract(
                        chain=chain,
                        address=pair.address,
                        chain_contract_rank=len(ranked) + 1,
                        collection_rank=collection_rank,
                        slug=str(collection.get("collection") or ""),
                        name=(
                            str(collection["name"])
                            if collection.get("name") is not None
                            else None
                        ),
                        ranking_value=collection_ranking_value(collection),
                        raw_chain=pair.raw_chain,
                    )
                )
                if len(ranked) == limit:
                    break
            if len(ranked) == limit:
                break

        if len(ranked) == limit:
            break
        next_value = next_cursor(payload)
        if not next_value:
            break
        if next_value in seen_cursors:
            raise ValueError(f"repeated pagination cursor for {chain}: {next_value}")
        seen_cursors.add(next_value)
        cursor = next_value

    if len(ranked) != limit:
        raise ValueError(
            f"requested {limit} contract addresses for {chain} but collected "
            f"{len(ranked)}"
        )
    return ranked


def render_contract_manifest_csv(ranked: list[RankedContract]) -> str:
    handle = io.StringIO(newline="")
    writer = csv.writer(handle, lineterminator="\n")
    writer.writerow(["chain", "address"])
    writer.writerows((item.chain, item.address) for item in ranked)
    return handle.getvalue()


def contract_audit_payload(
    ranked: list[RankedContract],
    chains: list[str],
    requested_limit_per_chain: int,
    *,
    generation_id: str = "",
    contracts_csv_sha256: str = "",
) -> dict[str, Any]:
    contracts = []
    for item in ranked:
        value = {
            "chain": item.chain,
            "address": item.address,
            "chain_contract_rank": item.chain_contract_rank,
            "collection_rank": item.collection_rank,
            "slug": item.slug,
            "name": item.name,
            "ranking_criterion": item.ranking_criterion,
            "ranking_value": item.ranking_value,
            "provider": item.provider,
        }
        value["raw_chain"] = item.raw_chain or item.chain
        contracts.append(value)
    payload = {
        "ranking_criterion_by_chain": {
            chain: (SOLANA_SORT_BY if chain == "solana" else DEFAULT_SORT_BY)
            for chain in chains
        },
        "provider_by_chain": {
            chain: ("magic_eden" if chain == "solana" else "opensea")
            for chain in chains
        },
        "chains": chains,
        "requested_contract_limit_per_chain": requested_limit_per_chain,
        "contracts": contracts,
    }
    if generation_id:
        payload["generation_id"] = generation_id
    if contracts_csv_sha256:
        payload["contracts_csv_sha256"] = contracts_csv_sha256
    return payload


@contextmanager
def exclusive_output_lock(lock_path: Path):
    """Serialize writers; readers can validate the committed CSV hash in the audit JSON."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as handle:
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def write_contract_rank_outputs(
    *,
    csv_path: Path,
    audit_path: Path,
    ranked: list[RankedContract],
    chains: list[str],
    requested_limit_per_chain: int,
) -> None:
    lock_path = csv_path.with_name(f".{csv_path.name}.output.lock")
    with exclusive_output_lock(lock_path):
        _write_contract_rank_outputs_locked(
            csv_path=csv_path,
            audit_path=audit_path,
            ranked=ranked,
            chains=chains,
            requested_limit_per_chain=requested_limit_per_chain,
        )


def _write_contract_rank_outputs_locked(
    *,
    csv_path: Path,
    audit_path: Path,
    ranked: list[RankedContract],
    chains: list[str],
    requested_limit_per_chain: int,
) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    recover_output_transaction(csv_path, audit_path)
    csv_tmp = csv_path.with_name(f".{csv_path.name}.{os.getpid()}.tmp")
    audit_tmp = audit_path.with_name(f".{audit_path.name}.{os.getpid()}.tmp")
    csv_backup = csv_path.with_name(f".{csv_path.name}.txn.bak")
    audit_backup = audit_path.with_name(f".{audit_path.name}.txn.bak")
    journal = csv_path.with_name(f".{csv_path.name}.output-transaction.json")
    journal_tmp = journal.with_name(f".{journal.name}.{os.getpid()}.tmp")
    csv_existed = csv_path.exists()
    audit_existed = audit_path.exists()
    try:
        csv_contents = render_contract_manifest_csv(ranked)
        generation_id = secrets.token_hex(16)
        csv_tmp.write_text(
            csv_contents,
            encoding="utf-8",
            newline="\n",
        )
        audit_tmp.write_text(
            json.dumps(
                contract_audit_payload(
                    ranked,
                    chains,
                    requested_limit_per_chain,
                    generation_id=generation_id,
                    contracts_csv_sha256=hashlib.sha256(
                        csv_contents.encode("utf-8")
                    ).hexdigest(),
                ),
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
            newline="\n",
        )
        if csv_existed:
            shutil.copy2(csv_path, csv_backup)
        if audit_existed:
            shutil.copy2(audit_path, audit_backup)
        journal_tmp.write_text(
            json.dumps(
                {
                    "csv_path": str(csv_path.resolve()),
                    "audit_path": str(audit_path.resolve()),
                    "csv_backup": str(csv_backup.resolve()),
                    "audit_backup": str(audit_backup.resolve()),
                    "csv_existed": csv_existed,
                    "audit_existed": audit_existed,
                }
            ),
            encoding="utf-8",
        )
        os.replace(journal_tmp, journal)
        try:
            os.replace(csv_tmp, csv_path)
            os.replace(audit_tmp, audit_path)
            validate_output_pair(csv_path, audit_path)
        except BaseException:
            if csv_existed and csv_backup.exists():
                os.replace(csv_backup, csv_path)
            elif not csv_existed:
                csv_path.unlink(missing_ok=True)
            if audit_existed and audit_backup.exists():
                os.replace(audit_backup, audit_path)
            elif not audit_existed:
                audit_path.unlink(missing_ok=True)
            journal.unlink(missing_ok=True)
            raise
        journal.unlink(missing_ok=True)
    finally:
        csv_tmp.unlink(missing_ok=True)
        audit_tmp.unlink(missing_ok=True)
        journal_tmp.unlink(missing_ok=True)
        csv_backup.unlink(missing_ok=True)
        audit_backup.unlink(missing_ok=True)


def validate_output_pair(csv_path: Path, audit_path: Path) -> None:
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    expected = payload.get("contracts_csv_sha256", "")
    if not expected:
        return
    actual = hashlib.sha256(csv_path.read_bytes()).hexdigest()
    if actual != expected:
        raise OSError(
            f"OpenSea seed CSV/audit generation mismatch: expected {expected}, got {actual}"
        )


def recover_output_transaction(csv_path: Path, audit_path: Path) -> None:
    journal = csv_path.with_name(f".{csv_path.name}.output-transaction.json")
    if not journal.exists():
        return
    payload = json.loads(journal.read_text(encoding="utf-8"))
    recorded_csv = Path(payload["csv_path"]).resolve()
    recorded_audit = Path(payload["audit_path"]).resolve()
    if recorded_csv != csv_path.resolve() or recorded_audit != audit_path.resolve():
        raise ValueError(f"output transaction journal does not match requested outputs: {journal}")
    for path, backup_key, existed_key in (
        (csv_path, "csv_backup", "csv_existed"),
        (audit_path, "audit_backup", "audit_existed"),
    ):
        backup = Path(payload[backup_key])
        if payload[existed_key]:
            if not backup.exists():
                raise OSError(f"cannot recover interrupted output transaction; missing {backup}")
            os.replace(backup, path)
        else:
            path.unlink(missing_ok=True)
    journal.unlink(missing_ok=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch independent Top contract rankings (OpenSea for EVM, "
            "Magic Eden for Solana) and export chain/address pairs"
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
        help="OpenSea Top collections API URL (called once per chain/cursor)",
    )
    parser.add_argument(
        "--solana-top-collections-url",
        default=DEFAULT_SOLANA_TOP_COLLECTIONS_URL,
        help="Magic Eden Solana popular collections API URL",
    )
    parser.add_argument(
        "--magic-eden-api-url",
        default=DEFAULT_MAGIC_EDEN_API_URL,
        help="Magic Eden Solana API base URL used to sample one NFT per collection",
    )
    parser.add_argument("--api-key", default="")
    parser.add_argument("--api-key-env", default="OPENSEA_API_KEY")
    parser.add_argument("--helius-api-key", default="")
    parser.add_argument("--helius-api-key-env", default="HELIUS_API_KEY")
    parser.add_argument("--helius-rpc-url", default=DEFAULT_HELIUS_RPC_URL)
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
    api_key = args.api_key or os.getenv(args.api_key_env) or ""
    if any(chain in EVM_CHAINS for chain in args.chains) and not api_key:
        raise SystemExit(
            f"missing OpenSea API key; pass --api-key or set {args.api_key_env}"
        )
    helius_api_key = (
        args.helius_api_key or os.getenv(args.helius_api_key_env) or ""
    )
    if "solana" in args.chains and not helius_api_key:
        raise SystemExit(
            "missing Helius API key for Magic Eden symbol resolution; pass "
            f"--helius-api-key or set {args.helius_api_key_env}"
        )
    helius_rpc_url = args.helius_rpc_url
    if "solana" in args.chains:
        separator = "&" if "?" in helius_rpc_url else "?"
        helius_rpc_url = (
            f"{helius_rpc_url}{separator}{urlencode({'api-key': helius_api_key})}"
        )
    try:
        ranked = []
        for chain in args.chains:
            if chain == "solana":
                ranked.extend(
                    collect_ranked_solana_contracts(
                        limit=args.limit,
                        top_collections_url=args.solana_top_collections_url,
                        magic_eden_api_url=args.magic_eden_api_url,
                        helius_rpc_url=helius_rpc_url,
                        timeout=args.timeout,
                    )
                )
            else:
                ranked.extend(
                    collect_ranked_contracts_for_chain(
                        api_key=api_key,
                        chain=chain,
                        limit=args.limit,
                        page_size=args.page_size,
                        top_collections_url=args.top_collections_url,
                        timeout=args.timeout,
                    )
                )
    except HTTPError as exc:
        print(f"seed provider HTTP error {exc.code}: {exc.reason}", file=sys.stderr)
        return 1
    except (URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
        print(f"seed provider request failed: {exc}", file=sys.stderr)
        return 1

    write_contract_rank_outputs(
        csv_path=args.contracts_output,
        audit_path=args.audit_output,
        ranked=ranked,
        chains=args.chains,
        requested_limit_per_chain=args.limit,
    )
    print(
        f"wrote {len(ranked)} ranked contract pairs across "
        f"{len(args.chains)} chains to {args.contracts_output}"
    )
    print(f"wrote ranking audit to {args.audit_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
