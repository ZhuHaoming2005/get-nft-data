from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urlencode

from .async_http import AsyncApiClient
from .alchemy_api import (
    _alchemy_get_json,
    _alchemy_nft_base,
    _normalize_token_id,
    _requests_module,
    _require_requests,
)
from .constants import DEFAULT_TIMEOUT
from .models import NFTSaleRecord

ETH_PRICED_SYMBOLS = frozenset({'ETH', 'WETH'})


def _decode_fee_eth(payload: Dict[str, Any]) -> tuple[float, str, str]:
    amount_raw = str(payload.get('amount') or '0').strip()
    symbol = str(payload.get('symbol') or '').strip().upper()
    token_address = str(payload.get('contractAddress') or payload.get('tokenAddress') or '').lower()
    try:
        decimals = int(payload.get('decimals') or 18)
    except (TypeError, ValueError):
        decimals = 18
    try:
        amount = int(amount_raw or '0')
    except ValueError:
        amount = 0
    return amount / float(10 ** max(decimals, 0)), symbol, token_address


def _alchemy_sales_base(network: str, api_key: str) -> str:
    return f'{_alchemy_nft_base(network)}/nft/v2/{api_key}/getNFTSales'


def _looks_like_real_api_key(value: str) -> bool:
    text = (value or '').strip()
    if len(text) < 12:
        return False
    return text.casefold() not in {'alchemy', 'etherscan', 'opensea', 'key'}


def fetch_alchemy_nft_sales(*, api_key: str, network: str, contract_address: str, token_id: str = '', timeout: int = DEFAULT_TIMEOUT, session=None) -> List[NFTSaleRecord]:
    _require_requests()
    endpoint = _alchemy_sales_base(network, api_key)
    page_key = ''
    rows: List[NFTSaleRecord] = []
    while True:
        params = {'fromBlock': '0', 'toBlock': 'latest', 'order': 'asc', 'contractAddress': contract_address}
        if token_id:
            params['tokenId'] = token_id
        if page_key:
            params['pageKey'] = page_key
        url = f'{endpoint}?{urlencode(params)}'
        payload = _alchemy_get_json(url=url, timeout=timeout, session=session)
        for item in payload.get('nftSales') or []:
            seller_fee_eth, fee_symbol, fee_token_address = _decode_fee_eth(item.get('sellerFee') or {})
            protocol_fee_eth, protocol_symbol, protocol_token_address = _decode_fee_eth(item.get('protocolFee') or {})
            royalty_fee_eth, royalty_symbol, royalty_token_address = _decode_fee_eth(item.get('royaltyFee') or {})
            symbols = {value for value in [fee_symbol, protocol_symbol, royalty_symbol] if value}
            native_eth = bool(symbols) and symbols == {'ETH'}
            payment_symbol = fee_symbol or protocol_symbol or royalty_symbol
            payment_token_address = fee_token_address or protocol_token_address or royalty_token_address
            eth_priced = bool(symbols) and symbols.issubset(ETH_PRICED_SYMBOLS)
            rows.append(
                NFTSaleRecord(
                    contract_address=str(item.get('contractAddress') or contract_address).lower(),
                    token_id=_normalize_token_id(item.get('tokenId') or ''),
                    tx_hash=str(item.get('transactionHash') or '').lower(),
                    block_number=int(item.get('blockNumber') or 0),
                    log_index=int(item.get('logIndex') or 0),
                    bundle_index=int(item.get('bundleIndex') or 0),
                    buyer_address=str(item.get('buyerAddress') or '').lower(),
                    seller_address=str(item.get('sellerAddress') or '').lower(),
                    marketplace=str(item.get('marketplace') or ''),
                    taker=str(item.get('taker') or ''),
                    payment_token_symbol=payment_symbol,
                    payment_token_address=payment_token_address,
                    price_eth=(seller_fee_eth + protocol_fee_eth + royalty_fee_eth) if eth_priced else None,
                    seller_fee_eth=seller_fee_eth,
                    protocol_fee_eth=protocol_fee_eth,
                    royalty_fee_eth=royalty_fee_eth,
                    source='alchemy',
                    is_native_eth=native_eth,
                )
            )
        page_key = str(payload.get('pageKey') or '')
        if not page_key:
            break
    return rows


async def fetch_alchemy_nft_sales_async(
    *,
    client: AsyncApiClient,
    api_key: str,
    network: str,
    contract_address: str,
    token_id: str = '',
    timeout: int = DEFAULT_TIMEOUT,
) -> List[NFTSaleRecord]:
    del timeout
    endpoint = _alchemy_sales_base(network, api_key)
    page_key = ''
    rows: List[NFTSaleRecord] = []
    while True:
        params = {'fromBlock': '0', 'toBlock': 'latest', 'order': 'asc', 'contractAddress': contract_address}
        if token_id:
            params['tokenId'] = token_id
        if page_key:
            params['pageKey'] = page_key
        url = f'{endpoint}?{urlencode(params)}'
        payload = await client.get_json(url)
        for item in payload.get('nftSales') or []:
            seller_fee_eth, fee_symbol, fee_token_address = _decode_fee_eth(item.get('sellerFee') or {})
            protocol_fee_eth, protocol_symbol, protocol_token_address = _decode_fee_eth(item.get('protocolFee') or {})
            royalty_fee_eth, royalty_symbol, royalty_token_address = _decode_fee_eth(item.get('royaltyFee') or {})
            symbols = {value for value in [fee_symbol, protocol_symbol, royalty_symbol] if value}
            native_eth = bool(symbols) and symbols == {'ETH'}
            payment_symbol = fee_symbol or protocol_symbol or royalty_symbol
            payment_token_address = fee_token_address or protocol_token_address or royalty_token_address
            eth_priced = bool(symbols) and symbols.issubset(ETH_PRICED_SYMBOLS)
            rows.append(
                NFTSaleRecord(
                    contract_address=str(item.get('contractAddress') or contract_address).lower(),
                    token_id=_normalize_token_id(item.get('tokenId') or ''),
                    tx_hash=str(item.get('transactionHash') or '').lower(),
                    block_number=int(item.get('blockNumber') or 0),
                    log_index=int(item.get('logIndex') or 0),
                    bundle_index=int(item.get('bundleIndex') or 0),
                    buyer_address=str(item.get('buyerAddress') or '').lower(),
                    seller_address=str(item.get('sellerAddress') or '').lower(),
                    marketplace=str(item.get('marketplace') or ''),
                    taker=str(item.get('taker') or ''),
                    payment_token_symbol=payment_symbol,
                    payment_token_address=payment_token_address,
                    price_eth=(seller_fee_eth + protocol_fee_eth + royalty_fee_eth) if eth_priced else None,
                    seller_fee_eth=seller_fee_eth,
                    protocol_fee_eth=protocol_fee_eth,
                    royalty_fee_eth=royalty_fee_eth,
                    source='alchemy',
                    is_native_eth=native_eth,
                )
            )
        page_key = str(payload.get('pageKey') or '')
        if not page_key:
            return rows


def fetch_opensea_nft_events(*, contract_address: str, token_id: str = '', opensea_api_key: str, timeout: int = DEFAULT_TIMEOUT, session=None) -> List[NFTSaleRecord]:
    _require_requests()
    client = session or _requests_module()
    headers = {'accept': 'application/json', 'x-api-key': opensea_api_key}
    params = {'event_type': 'sale'}
    if token_id:
        url = f'https://api.opensea.io/api/v2/events/chain/ethereum/contract/{contract_address}/nfts/{token_id}'
    else:
        url = 'https://api.opensea.io/api/v2/events'
        params['asset_contract_address'] = contract_address
        params['chain'] = 'ethereum'
    response = client.get(url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    events = payload.get('asset_events') or payload.get('events') or []
    rows: List[NFTSaleRecord] = []
    for item in events:
        event_type = str(item.get('event_type') or item.get('eventType') or '').casefold()
        if event_type and event_type != 'sale':
            continue
        nft = item.get('nft') or item.get('asset') or {}
        payment = item.get('payment') or item.get('payment_token') or {}
        payment_symbol = str(payment.get('symbol') or item.get('payment_token_symbol') or '').upper()
        payment_token_address = str(payment.get('address') or payment.get('token_address') or '').lower()
        value_eth = None
        if payment_symbol in ETH_PRICED_SYMBOLS:
            raw_value = item.get('payment_quantity') or item.get('price') or item.get('total_price') or '0'
            try:
                value_eth = int(str(raw_value), 10) / float(10 ** 18)
            except ValueError:
                try:
                    value_eth = float(raw_value)
                except (TypeError, ValueError):
                    value_eth = None
        rows.append(
            NFTSaleRecord(
                contract_address=str(nft.get('contract') or nft.get('contract_address') or item.get('asset_contract_address') or contract_address).lower(),
                token_id=_normalize_token_id(nft.get('identifier') or nft.get('token_id') or token_id),
                tx_hash=str(item.get('transaction') or item.get('transaction_hash') or item.get('order_hash') or '').lower(),
                block_number=int(item.get('block_number') or 0),
                log_index=int(item.get('event_index') or item.get('log_index') or 0),
                bundle_index=int(item.get('bundle_index') or 0),
                buyer_address=str(item.get('to_account', {}).get('address') or item.get('winner_account', {}).get('address') or '').lower(),
                seller_address=str(item.get('from_account', {}).get('address') or item.get('seller', {}).get('address') or '').lower(),
                marketplace='opensea',
                taker=str(item.get('taker') or ''),
                payment_token_symbol=payment_symbol,
                payment_token_address=payment_token_address,
                price_eth=value_eth if payment_symbol in ETH_PRICED_SYMBOLS else None,
                seller_fee_eth=value_eth or 0.0 if payment_symbol in ETH_PRICED_SYMBOLS else 0.0,
                protocol_fee_eth=0.0,
                royalty_fee_eth=0.0,
                source='opensea',
                is_native_eth=(payment_symbol == 'ETH'),
            )
        )
    return rows


async def fetch_opensea_nft_events_async(
    *,
    client: AsyncApiClient,
    contract_address: str,
    token_id: str = '',
    opensea_api_key: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> List[NFTSaleRecord]:
    del timeout
    headers = {'accept': 'application/json', 'x-api-key': opensea_api_key}
    params = {'event_type': 'sale'}
    if token_id:
        url = f'https://api.opensea.io/api/v2/events/chain/ethereum/contract/{contract_address}/nfts/{token_id}'
    else:
        url = 'https://api.opensea.io/api/v2/events'
        params['asset_contract_address'] = contract_address
        params['chain'] = 'ethereum'
    payload = await client.get_json(url, params=params, headers=headers)
    events = payload.get('asset_events') or payload.get('events') or []
    rows: List[NFTSaleRecord] = []
    for item in events:
        event_type = str(item.get('event_type') or item.get('eventType') or '').casefold()
        if event_type and event_type != 'sale':
            continue
        nft = item.get('nft') or item.get('asset') or {}
        payment = item.get('payment') or item.get('payment_token') or {}
        payment_symbol = str(payment.get('symbol') or item.get('payment_token_symbol') or '').upper()
        payment_token_address = str(payment.get('address') or payment.get('token_address') or '').lower()
        value_eth = None
        if payment_symbol in ETH_PRICED_SYMBOLS:
            raw_value = item.get('payment_quantity') or item.get('price') or item.get('total_price') or '0'
            try:
                value_eth = int(str(raw_value), 10) / float(10 ** 18)
            except ValueError:
                try:
                    value_eth = float(raw_value)
                except (TypeError, ValueError):
                    value_eth = None
        rows.append(
            NFTSaleRecord(
                contract_address=str(nft.get('contract') or nft.get('contract_address') or item.get('asset_contract_address') or contract_address).lower(),
                token_id=_normalize_token_id(nft.get('identifier') or nft.get('token_id') or token_id),
                tx_hash=str(item.get('transaction') or item.get('transaction_hash') or item.get('order_hash') or '').lower(),
                block_number=int(item.get('block_number') or 0),
                log_index=int(item.get('event_index') or item.get('log_index') or 0),
                bundle_index=int(item.get('bundle_index') or 0),
                buyer_address=str(item.get('to_account', {}).get('address') or item.get('winner_account', {}).get('address') or '').lower(),
                seller_address=str(item.get('from_account', {}).get('address') or item.get('seller', {}).get('address') or '').lower(),
                marketplace='opensea',
                taker=str(item.get('taker') or ''),
                payment_token_symbol=payment_symbol,
                payment_token_address=payment_token_address,
                price_eth=value_eth if payment_symbol in ETH_PRICED_SYMBOLS else None,
                seller_fee_eth=value_eth or 0.0 if payment_symbol in ETH_PRICED_SYMBOLS else 0.0,
                protocol_fee_eth=0.0,
                royalty_fee_eth=0.0,
                source='opensea',
                is_native_eth=(payment_symbol == 'ETH'),
            )
        )
    return rows


def fetch_contract_sales(*, alchemy_api_key: str, alchemy_network: str, contract_address: str, opensea_api_key: str = '', timeout: int = DEFAULT_TIMEOUT, session=None) -> List[NFTSaleRecord]:
    sales = fetch_alchemy_nft_sales(
        api_key=alchemy_api_key,
        network=alchemy_network,
        contract_address=contract_address,
        timeout=timeout,
        session=session,
    )
    if sales or not opensea_api_key:
        return sales
    return fetch_opensea_nft_events(
        contract_address=contract_address,
        opensea_api_key=opensea_api_key,
        timeout=timeout,
        session=session,
    )


async def fetch_contract_sales_async(
    *,
    client: AsyncApiClient,
    alchemy_api_key: str,
    alchemy_network: str,
    contract_address: str,
    opensea_api_key: str = '',
    timeout: int = DEFAULT_TIMEOUT,
) -> List[NFTSaleRecord]:
    sales = await fetch_alchemy_nft_sales_async(
        client=client,
        api_key=alchemy_api_key,
        network=alchemy_network,
        contract_address=contract_address,
        timeout=timeout,
    )
    if sales or not opensea_api_key:
        return sales
    return await fetch_opensea_nft_events_async(
        client=client,
        contract_address=contract_address,
        opensea_api_key=opensea_api_key,
        timeout=timeout,
    )
