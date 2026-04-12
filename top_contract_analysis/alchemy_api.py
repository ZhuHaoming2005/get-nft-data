from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple
from urllib.parse import urlencode
import requests

from . import constants as package_constants
from .constants import (
    DEFAULT_ALCHEMY_RETRIES,
    DEFAULT_TIMEOUT,
    ETHERSCAN_CHAIN_IDS,
    logger,
)
from .models import (
    EthTransferRecord,
    OwnerBalance,
    SeedNFT,
    TransactionReceiptRecord,
    TransferRecord,
)
from .normalize import build_nft_metadata_json
from .constants import ZERO_ADDRESS


def _requests_module():
    module = package_constants.requests
    if module is None:
        raise RuntimeError('requests is required for HTTP API access; pip install requests')
    return module


def _require_requests() -> None:
    _requests_module()


def _build_requests_session():
    requests = _requests_module()
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session


def _alchemy_nft_base(network: str) -> str:
    return f'https://{network}.g.alchemy.com'


def _alchemy_rpc_base(network: str, api_key: str) -> str:
    return f'https://{network}.g.alchemy.com/v2/{api_key}'


def _normalize_token_id(raw: Any) -> str:
    if raw is None:
        return ''
    text = str(raw).strip()
    if text.startswith(('0x', '0X')):
        return str(int(text, 16))
    return text


def _alchemy_get_json(*, url: str, timeout: int = DEFAULT_TIMEOUT, session=None) -> Dict[str, Any]:
    client = session or _requests_module()
    last_exc: Exception | None = None
    for attempt in range(DEFAULT_ALCHEMY_RETRIES):
        try:
            response = client.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except package_constants.REQUESTS_REQUEST_ERROR as exc:
            last_exc = exc
            if attempt < DEFAULT_ALCHEMY_RETRIES - 1:
                logger.warning('alchemy GET retry %d/%d failed for %s: %s', attempt + 1, DEFAULT_ALCHEMY_RETRIES, url, exc)
                continue
            raise
    raise RuntimeError(last_exc or 'alchemy GET failed')


def _alchemy_post_json(*, url: str, payload: Dict[str, Any], timeout: int = DEFAULT_TIMEOUT, session=None) -> Dict[str, Any]:
    client = session or _requests_module()
    last_exc: Exception | None = None
    for attempt in range(DEFAULT_ALCHEMY_RETRIES):
        try:
            response = client.post(url, json=payload, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except package_constants.REQUESTS_REQUEST_ERROR as exc:
            last_exc = exc
            if attempt < DEFAULT_ALCHEMY_RETRIES - 1:
                logger.warning('alchemy POST retry %d/%d failed for %s: %s', attempt + 1, DEFAULT_ALCHEMY_RETRIES, url, exc)
                continue
            raise
    raise RuntimeError(last_exc or 'alchemy POST failed')


def _parse_alchemy_block_timestamp(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return 0
    if text.startswith(('0x', '0X')):
        return int(text, 16)
    if text.isdigit():
        return int(text)
    try:
        from datetime import datetime

        return int(datetime.fromisoformat(text.replace('Z', '+00:00')).timestamp())
    except ValueError:
        return 0


def _fetch_block_timestamp(
    *,
    api_key: str,
    network: str,
    block_num: str,
    timeout: int = DEFAULT_TIMEOUT,
    session=None,
) -> int:
    body = _alchemy_post_json(
        url=_alchemy_rpc_base(network, api_key),
        payload={
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'eth_getBlockByNumber',
            'params': [block_num, False],
        },
        timeout=timeout,
        session=session,
    )
    result = body.get('result') or {}
    return _parse_alchemy_block_timestamp(result.get('timestamp'))


def _extract_seed_nfts(payload: Dict[str, Any], *, chain: str, contract_address: str) -> Tuple[List[SeedNFT], str]:
    from .rust_bridge import metadata_document_from_json

    raw_items = payload.get('nfts')
    if not isinstance(raw_items, list):
        return [], ''
    rows: List[SeedNFT] = []
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        raw_contract = raw.get('contract') or {}
        raw_id = raw.get('id') or {}
        raw_token_uri = raw.get('tokenUri') or {}
        raw_image = raw.get('image') or {}
        if isinstance(raw_token_uri, dict):
            token_uri = str(raw_token_uri.get('raw') or raw_token_uri.get('gateway') or '')
        else:
            token_uri = str(raw_token_uri or '')
        if isinstance(raw_image, dict):
            image_uri = str(
                raw_image.get('originalUrl')
                or raw_image.get('cachedUrl')
                or raw_image.get('pngUrl')
                or raw.get('image_url')
                or ''
            )
        else:
            image_uri = str(raw_image or raw.get('image_url') or '')
        metadata_json = build_nft_metadata_json(raw, token_uri=token_uri, image_uri=image_uri)
        rows.append(
            SeedNFT(
                chain=chain,
                contract_address=str(raw_contract.get('address') or contract_address).lower(),
                token_id=_normalize_token_id(raw_id.get('tokenId')),
                name=str(raw.get('title') or raw.get('name') or ''),
                symbol=str((raw.get('contractMetadata') or {}).get('symbol') or ''),
                token_uri=token_uri,
                image_uri=image_uri,
                metadata_json=metadata_json,
                metadata_doc=metadata_document_from_json(metadata_json),
            )
        )
    return rows, str(payload.get('pageKey') or '')


def fetch_seed_contract_nfts(*, api_key: str, network: str, chain: str, contract_address: str, timeout: int = DEFAULT_TIMEOUT, session=None) -> List[SeedNFT]:
    _require_requests()
    endpoint = f'{_alchemy_nft_base(network)}/nft/v3/{api_key}/getNFTsForContract'
    rows: List[SeedNFT] = []
    page_key = ''
    while True:
        params = {'contractAddress': contract_address, 'withMetadata': 'true'}
        if page_key:
            params['pageKey'] = page_key
        url = f'{endpoint}?{urlencode(params)}'
        payload = _alchemy_get_json(url=url, timeout=timeout, session=session)
        batch, page_key = _extract_seed_nfts(payload, chain=chain, contract_address=contract_address)
        rows.extend(batch)
        if not page_key:
            break
    return rows


def fetch_nft_metadata(*, api_key: str, network: str, contract_address: str, token_id: str, timeout: int = DEFAULT_TIMEOUT, session=None) -> Dict[str, Any]:
    _require_requests()
    params = {'contractAddress': contract_address, 'tokenId': token_id, 'refreshCache': 'false'}
    url = f'{_alchemy_nft_base(network)}/nft/v3/{api_key}/getNFTMetadata?{urlencode(params)}'
    return _alchemy_get_json(url=url, timeout=timeout, session=session)


def fetch_license_sample(*, api_key: str, network: str, chain: str, seed_nfts: Sequence[SeedNFT], timeout: int = DEFAULT_TIMEOUT, session=None) -> Dict[str, Any]:
    for nft in seed_nfts:
        if nft.token_id:
            return fetch_nft_metadata(
                api_key=api_key,
                network=network,
                contract_address=nft.contract_address,
                token_id=nft.token_id,
                timeout=timeout,
                session=session,
            )
    return {}


def is_open_license_payload(payload: Dict[str, Any]) -> bool:
    texts: List[str] = []

    def _walk(value: Any) -> None:
        if isinstance(value, dict):
            for item in value.values():
                _walk(item)
        elif isinstance(value, list):
            for item in value:
                _walk(item)
        elif isinstance(value, str):
            texts.append(value)

    _walk(payload)
    haystack = ' '.join(texts).casefold()
    needles = ['cc0-1.0', 'license: cc0', 'creative commons zero', 'public domain', 'cc zero']
    return any(needle in haystack for needle in needles)


def fetch_contract_metadata(*, api_key: str, network: str, chain: str, contract_address: str, timeout: int = DEFAULT_TIMEOUT, session=None):
    from .models import ContractMetadata

    _require_requests()
    params = {'contractAddress': contract_address}
    url = f'{_alchemy_nft_base(network)}/nft/v2/{api_key}/getContractMetadata?{urlencode(params)}'
    payload = _alchemy_get_json(url=url, timeout=timeout, session=session)
    meta = payload.get('contractMetadata') or {}
    return ContractMetadata(
        chain=chain,
        contract_address=str(payload.get('address') or contract_address).lower(),
        token_type=str(meta.get('tokenType') or ''),
        contract_deployer=str(meta.get('contractDeployer') or '').lower(),
        deployed_block_number=int(meta.get('deployedBlockNumber') or 0),
        name=str(meta.get('name') or ''),
        symbol=str(meta.get('symbol') or ''),
    )


def fetch_alchemy_contract_transfers(*, api_key: str, network: str, contract_address: str, timeout: int = DEFAULT_TIMEOUT, session=None) -> List[TransferRecord]:
    _require_requests()
    url = _alchemy_rpc_base(network, api_key)
    params: Dict[str, Any] = {
        'fromBlock': '0x0',
        'toBlock': 'latest',
        'category': ['erc721', 'erc1155'],
        'contractAddresses': [contract_address],
        'withMetadata': True,
        'excludeZeroValue': False,
        'maxCount': '0x3e8',
        'order': 'asc',
    }
    transfers: List[TransferRecord] = []
    page_key = None
    block_time_cache: Dict[str, int] = {}
    while True:
        if page_key:
            params['pageKey'] = page_key
        payload = {'jsonrpc': '2.0', 'id': 1, 'method': 'alchemy_getAssetTransfers', 'params': [params]}
        body = _alchemy_post_json(url=url, payload=payload, timeout=timeout, session=session)
        if body.get('error'):
            raise RuntimeError(body['error'])
        result = body.get('result') or {}
        for item in result.get('transfers') or []:
            block_num = str(item.get('blockNum') or '0')
            block_time = _parse_alchemy_block_timestamp(item.get('metadata', {}).get('blockTimestamp'))
            if not block_time and block_num not in {'', '0'}:
                cached = block_time_cache.get(block_num)
                if cached is None:
                    cached = _fetch_block_timestamp(api_key=api_key, network=network, block_num=block_num, timeout=timeout, session=session)
                    block_time_cache[block_num] = cached
                block_time = cached
            transfers.append(
                TransferRecord(
                    contract_address=str(item.get('rawContract', {}).get('address') or contract_address).lower(),
                    token_id=_normalize_token_id(item.get('erc721TokenId') or item.get('tokenId') or ''),
                    tx_hash=str(item.get('hash') or ''),
                    log_index=int(item.get('logIndex') or 0),
                    block_number=int(block_num, 16) if block_num.startswith('0x') else int(block_num or 0),
                    block_time=block_time,
                    from_address=str(item.get('from') or '').lower(),
                    to_address=str(item.get('to') or '').lower(),
                    event_type=str(item.get('category') or ''),
                    source='alchemy',
                )
            )
        page_key = result.get('pageKey')
        if not page_key:
            break
    return transfers


def fetch_etherscan_contract_transfers(*, api_key: str, chain: str, contract_address: str, token_type: str, timeout: int = DEFAULT_TIMEOUT, session=None) -> List[TransferRecord]:
    _require_requests()
    chain_id = ETHERSCAN_CHAIN_IDS.get(chain.lower())
    if not chain_id:
        raise ValueError(f'unsupported chain for etherscan fallback: {chain}')
    action = 'token1155tx' if token_type.upper() == 'ERC1155' else 'tokennfttx'
    base_url = 'https://api.etherscan.io/v2/api'
    page = 1
    transfers: List[TransferRecord] = []
    client = session or requests
    while True:
        params = {
            'chainid': chain_id,
            'module': 'account',
            'action': action,
            'contractaddress': contract_address,
            'page': page,
            'offset': 1000,
            'startblock': 0,
            'endblock': 9999999999,
            'sort': 'asc',
            'apikey': api_key,
        }
        url = f'{base_url}?{urlencode(params)}'
        response = client.get(url, timeout=timeout)
        response.raise_for_status()
        body = response.json()
        items = body.get('result') or []
        if not isinstance(items, list):
            break
        for item in items:
            transfers.append(
                TransferRecord(
                    contract_address=str(item.get('contractAddress') or contract_address).lower(),
                    token_id=_normalize_token_id(item.get('tokenID') or ''),
                    tx_hash=str(item.get('hash') or ''),
                    log_index=int(item.get('transactionIndex') or 0),
                    block_number=int(item.get('blockNumber') or 0),
                    block_time=int(item.get('timeStamp') or 0),
                    from_address=str(item.get('from') or '').lower(),
                    to_address=str(item.get('to') or '').lower(),
                    event_type='erc1155' if action == 'token1155tx' else 'erc721',
                    source='etherscan',
                )
            )
        if len(items) < 1000:
            break
        page += 1
    return transfers


def fetch_contract_transfers(*, alchemy_api_key: str, alchemy_network: str, etherscan_api_key: str, chain: str, contract_address: str, token_type: str, timeout: int = DEFAULT_TIMEOUT, session=None) -> List[TransferRecord]:
    try:
        return fetch_alchemy_contract_transfers(
            api_key=alchemy_api_key,
            network=alchemy_network,
            contract_address=contract_address,
            timeout=timeout,
            session=session,
        )
    except Exception as exc:
        logger.warning('alchemy transfer fetch failed for %s: %s', contract_address, exc)
        return fetch_etherscan_contract_transfers(
            api_key=etherscan_api_key,
            chain=chain,
            contract_address=contract_address,
            token_type=token_type,
            timeout=timeout,
            session=session,
        )


def fetch_contract_owners(*, api_key: str, network: str, contract_address: str, timeout: int = DEFAULT_TIMEOUT, session=None) -> List[OwnerBalance]:
    _require_requests()
    endpoint = f'{_alchemy_nft_base(network)}/nft/v3/{api_key}/getOwnersForContract'
    page_key = ''
    owners: List[OwnerBalance] = []
    while True:
        params = {'contractAddress': contract_address, 'withTokenBalances': 'true'}
        if page_key:
            params['pageKey'] = page_key
        url = f'{endpoint}?{urlencode(params)}'
        payload = _alchemy_get_json(url=url, timeout=timeout, session=session)
        for row in payload.get('owners') or []:
            balances: Dict[str, int] = {}
            for balance in row.get('tokenBalances') or []:
                balances[_normalize_token_id(balance.get('tokenId'))] = int(balance.get('balance') or 0)
            owners.append(OwnerBalance(owner_address=str(row.get('ownerAddress') or '').lower(), token_balances=balances))
        page_key = str(payload.get('pageKey') or '')
        if not page_key:
            break
    return owners


def fetch_transaction_receipt(*, api_key: str, network: str, tx_hash: str, timeout: int = DEFAULT_TIMEOUT, session=None) -> TransactionReceiptRecord:
    body = _alchemy_post_json(
        url=_alchemy_rpc_base(network, api_key),
        payload={'jsonrpc': '2.0', 'id': 1, 'method': 'eth_getTransactionReceipt', 'params': [tx_hash]},
        timeout=timeout,
        session=session,
    )
    result = body.get('result') or {}
    return TransactionReceiptRecord(
        tx_hash=str(result.get('transactionHash') or tx_hash).lower(),
        block_number=int(str(result.get('blockNumber') or '0'), 16) if str(result.get('blockNumber') or '').startswith('0x') else int(result.get('blockNumber') or 0),
        transaction_index=int(str(result.get('transactionIndex') or '0'), 16) if str(result.get('transactionIndex') or '').startswith('0x') else int(result.get('transactionIndex') or 0),
        from_address=str(result.get('from') or '').lower(),
        gas_used=int(str(result.get('gasUsed') or '0'), 16) if str(result.get('gasUsed') or '').startswith('0x') else int(result.get('gasUsed') or 0),
        effective_gas_price_wei=int(str(result.get('effectiveGasPrice') or '0'), 16) if str(result.get('effectiveGasPrice') or '').startswith('0x') else int(result.get('effectiveGasPrice') or 0),
    )


def fetch_transaction_receipts_for_block(*, api_key: str, network: str, block_number: int, timeout: int = DEFAULT_TIMEOUT, session=None) -> Dict[str, TransactionReceiptRecord]:
    body = _alchemy_post_json(
        url=_alchemy_rpc_base(network, api_key),
        payload={'jsonrpc': '2.0', 'id': 1, 'method': 'alchemy_getTransactionReceipts', 'params': [{'blockNumber': hex(block_number)}]},
        timeout=timeout,
        session=session,
    )
    result = body.get('result') or {}
    rows: Dict[str, TransactionReceiptRecord] = {}
    for item in result.get('receipts') or []:
        tx_hash = str(item.get('transactionHash') or '').lower()
        if not tx_hash:
            continue
        rows[tx_hash] = TransactionReceiptRecord(
            tx_hash=tx_hash,
            block_number=block_number,
            transaction_index=int(str(item.get('transactionIndex') or '0'), 16) if str(item.get('transactionIndex') or '').startswith('0x') else int(item.get('transactionIndex') or 0),
            from_address=str(item.get('from') or '').lower(),
            gas_used=int(str(item.get('gasUsed') or '0'), 16) if str(item.get('gasUsed') or '').startswith('0x') else int(item.get('gasUsed') or 0),
            effective_gas_price_wei=int(str(item.get('effectiveGasPrice') or '0'), 16) if str(item.get('effectiveGasPrice') or '').startswith('0x') else int(item.get('effectiveGasPrice') or 0),
        )
    return rows


def fetch_eth_balance(*, api_key: str, network: str, address: str, block_number: int, timeout: int = DEFAULT_TIMEOUT, session=None) -> float:
    if block_number < 0:
        return 0.0
    body = _alchemy_post_json(
        url=_alchemy_rpc_base(network, api_key),
        payload={'jsonrpc': '2.0', 'id': 1, 'method': 'eth_getBalance', 'params': [address, hex(block_number)]},
        timeout=timeout,
        session=session,
    )
    value = str(body.get('result') or '0x0')
    return int(value, 16) / float(10 ** 18)


def _fetch_address_eth_transfers(*, api_key: str, network: str, block_number: int, address: str, direction: str, timeout: int = DEFAULT_TIMEOUT, session=None) -> List[EthTransferRecord]:
    params: Dict[str, Any] = {
        'fromBlock': hex(block_number),
        'toBlock': hex(block_number),
        'category': ['external', 'internal'],
        'withMetadata': False,
        'excludeZeroValue': True,
        'maxCount': '0x3e8',
        'order': 'asc',
    }
    if direction == 'from':
        params['fromAddress'] = address
    else:
        params['toAddress'] = address
    body = _alchemy_post_json(
        url=_alchemy_rpc_base(network, api_key),
        payload={'jsonrpc': '2.0', 'id': 1, 'method': 'alchemy_getAssetTransfers', 'params': [params]},
        timeout=timeout,
        session=session,
    )
    result = body.get('result') or {}
    rows: List[EthTransferRecord] = []
    for item in result.get('transfers') or []:
        value_raw = item.get('value')
        if value_raw is None:
            value_raw = (item.get('rawContract') or {}).get('value')
        if isinstance(value_raw, str) and value_raw.startswith('0x'):
            value_eth = int(value_raw, 16) / float(10 ** 18)
        else:
            try:
                value_eth = float(value_raw or 0)
            except (TypeError, ValueError):
                value_eth = 0.0
        rows.append(
            EthTransferRecord(
                tx_hash=str(item.get('hash') or '').lower(),
                block_number=block_number,
                from_address=str(item.get('from') or '').lower(),
                to_address=str(item.get('to') or '').lower(),
                value_eth=value_eth,
                category=str(item.get('category') or ''),
            )
        )
    return rows


def fetch_same_block_eth_transfers_for_address(*, api_key: str, network: str, block_number: int, address: str, timeout: int = DEFAULT_TIMEOUT, session=None) -> List[EthTransferRecord]:
    rows = _fetch_address_eth_transfers(api_key=api_key, network=network, block_number=block_number, address=address, direction='from', timeout=timeout, session=session)
    rows.extend(_fetch_address_eth_transfers(api_key=api_key, network=network, block_number=block_number, address=address, direction='to', timeout=timeout, session=session))
    deduped: Dict[tuple[str, str, str, float], EthTransferRecord] = {}
    for row in rows:
        deduped[(row.tx_hash, row.from_address, row.to_address, row.value_eth)] = row
    return list(deduped.values())
