from __future__ import annotations

import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Sequence


def _count_unique_malicious_addresses(payload: Dict[str, Any]) -> int:
    keys: set[str] = set()
    for item in payload.get('malicious_addresses') or []:
        address = str(item.get('address') or '').strip()
        if not address:
            continue
        keys.add(address)
    return len(keys)


def _count_unique_honest_addresses(payload: Dict[str, Any]) -> int:
    keys: set[str] = set()
    for item in payload.get('honest_addresses') or []:
        address = str(item.get('address') or '').strip()
        if not address:
            continue
        keys.add(address)
    return len(keys)


def _payload_stuck_cost_eth(payload: Dict[str, Any]) -> float:
    return sum(
        float(item.get('last_buy_amount_eth') or 0.0)
        for item in (payload.get('victim_addresses') or [])
        if item.get('is_stuck')
    )


def dump_results(payload: Dict[str, Any], output_path: Optional[str]) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    Path(output_path).write_text(text, encoding='utf-8')


def _slugify_filename_part(value: str) -> str:
    text = unicodedata.normalize('NFKC', value or '').strip().casefold()
    text = re.sub(r'[^0-9a-zA-Z\u4e00-\u9fff]+', '_', text)
    text = text.strip('_')
    return text or 'unknown_collection'


def default_output_basename(payload: Dict[str, Any]) -> str:
    seed = payload.get('seed_contract') or {}
    name = str(seed.get('name') or '').strip()
    if not name:
        name = str(seed.get('contract_address') or 'unknown_collection')
    return f'top_contract_analysis__{_slugify_filename_part(name)}'


def write_default_outputs(payload: Dict[str, Any], output_path: str = '') -> tuple[Path, Path]:
    if output_path:
        json_path = Path(output_path)
    else:
        result_dir = Path.cwd() / 'result'
        result_dir.mkdir(parents=True, exist_ok=True)
        json_path = result_dir / f'{default_output_basename(payload)}.json'
    md_path = json_path.with_suffix('.md')
    dump_results(payload, str(json_path))
    md_path.write_text(render_human_readable_report(payload), encoding='utf-8')
    return json_path, md_path


def write_outputs_to_directory(payload: Dict[str, Any], output_dir: str | Path) -> tuple[Path, Path]:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    json_path = target_dir / f'{default_output_basename(payload)}.json'
    md_path = json_path.with_suffix('.md')
    dump_results(payload, str(json_path))
    md_path.write_text(render_human_readable_report(payload), encoding='utf-8')
    return json_path, md_path


def build_batch_summary_payload(
    payloads: Sequence[Dict[str, Any]],
    output_index: Optional[Sequence[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    reports: list[Dict[str, Any]] = []
    total_candidates = 0
    total_high = 0
    total_low = 0
    total_infringing_nfts = 0
    total_malicious_addresses = 0
    total_honest_addresses = 0
    total_repeat_infringing_addresses = 0
    total_legit = 0
    open_license_count = 0
    total_honest_purchase_eth = 0.0
    total_stuck_cost_eth = 0.0
    total_ratio_known = 0
    total_ratio_over_60 = 0
    total_ratio_over_80 = 0
    total_stuck_honest = 0
    mean_honest_holder_values: list[float] = []
    median_honest_holder_values: list[float] = []
    mean_first_transfer_values: list[float] = []
    median_first_transfer_values: list[float] = []
    mean_unique_receiver_values: list[float] = []
    chains: list[str] = []
    output_index = output_index or []
    malicious_address_global_set: set[str] = set()
    honest_address_global_set: set[str] = set()
    minter_infringing_contracts: Dict[str, set[str]] = {}

    for index, payload in enumerate(payloads):
        seed = payload.get('seed_contract') or {}
        summary = payload.get('report_summary') or {}
        if summary.get('open_license_detected'):
            open_license_count += 1
        total_candidates += int(summary.get('candidate_contract_count') or 0)
        total_high += int(summary.get('high_confidence_contract_count') or 0)
        total_low += int(summary.get('low_confidence_contract_count') or 0)
        payload_infringing_tokens = payload.get('infringing_tokens') or []
        payload_malicious_address_count = _count_unique_malicious_addresses(payload)
        payload_honest_address_count = _count_unique_honest_addresses(payload)
        payload_stuck_cost_eth = _payload_stuck_cost_eth(payload)
        payload_minter_contracts: Dict[str, set[str]] = {}
        for token in payload_infringing_tokens:
            minter = str(token.get('minter_address') or '').strip()
            contract = str(token.get('contract_address') or '').strip()
            if minter and contract:
                payload_minter_contracts.setdefault(minter, set()).add(contract)
        payload_repeat_infringing_count = sum(
            1 for contracts in payload_minter_contracts.values() if len(contracts) > 1
        )
        summary_infringing_nft_count = int(summary.get('infringing_nft_count') or len(payload_infringing_tokens))
        summary_malicious_address_count = int(summary.get('malicious_address_count') or payload_malicious_address_count)
        summary_honest_address_count = int(summary.get('honest_address_count') or payload_honest_address_count)
        summary_repeat_infringing_count = int(summary.get('repeat_infringing_address_count') or payload_repeat_infringing_count)
        summary_honest_purchase_total_eth = float(summary.get('honest_purchase_total_eth') or 0.0)
        summary_stuck_cost_eth = float(summary.get('stuck_cost_eth') or payload_stuck_cost_eth)

        total_infringing_nfts += summary_infringing_nft_count
        total_repeat_infringing_addresses += summary_repeat_infringing_count
        total_legit += int(summary.get('legit_duplicate_contract_count') or 0)
        total_honest_purchase_eth += summary_honest_purchase_total_eth
        total_stuck_cost_eth += summary_stuck_cost_eth
        total_ratio_known += int(summary.get('buy_asset_ratio_known_address_count') or 0)
        total_ratio_over_60 += int(summary.get('ratio_over_60_address_count') or 0)
        total_ratio_over_80 += int(summary.get('ratio_over_80_address_count') or 0)
        total_stuck_honest += int(summary.get('stuck_honest_address_count') or 0)
        if isinstance(summary.get('avg_seconds_to_honest_holder'), (int, float)):
            mean_honest_holder_values.append(float(summary['avg_seconds_to_honest_holder']))
        summary_median_honest_holder = summary.get('median_seconds_to_honest_holder')
        if not isinstance(summary_median_honest_holder, (int, float)):
            summary_median_honest_holder = _payload_median_seconds_to_honest_holder(payload)
        if isinstance(summary_median_honest_holder, (int, float)):
            median_honest_holder_values.append(float(summary_median_honest_holder))
        if isinstance(summary.get('avg_mint_to_first_transfer_seconds'), (int, float)):
            mean_first_transfer_values.append(float(summary['avg_mint_to_first_transfer_seconds']))
        summary_median_first_transfer = summary.get('median_mint_to_first_transfer_seconds')
        if not isinstance(summary_median_first_transfer, (int, float)):
            summary_median_first_transfer = _payload_median_mint_to_first_transfer_seconds(payload)
        if isinstance(summary_median_first_transfer, (int, float)):
            median_first_transfer_values.append(float(summary_median_first_transfer))
        if isinstance(summary.get('avg_unique_receiver_count'), (int, float)):
            mean_unique_receiver_values.append(float(summary['avg_unique_receiver_count']))
        chain = str(seed.get('chain') or '').strip()
        if chain:
            chains.append(chain)
        for token in payload_infringing_tokens:
            minter = str(token.get('minter_address') or '').strip()
            contract = str(token.get('contract_address') or '').strip()
            if minter and contract:
                minter_infringing_contracts.setdefault(minter, set()).add(contract)
        for item in payload.get('malicious_addresses') or []:
            address = str(item.get('address') or '').strip()
            if address:
                malicious_address_global_set.add(address)
        for item in payload.get('honest_addresses') or []:
            address = str(item.get('address') or '').strip()
            if address:
                honest_address_global_set.add(address)

        report_summary = dict(summary)
        report_summary['infringing_nft_count'] = summary_infringing_nft_count
        report_summary['malicious_address_count'] = summary_malicious_address_count
        report_summary['honest_address_count'] = summary_honest_address_count
        report_summary['repeat_infringing_address_count'] = summary_repeat_infringing_count
        report_summary['honest_purchase_total_eth'] = summary_honest_purchase_total_eth
        report_summary['stuck_cost_eth'] = summary_stuck_cost_eth
        report_summary['stuck_cost_ratio'] = (
            summary_stuck_cost_eth / summary_honest_purchase_total_eth
            if summary_honest_purchase_total_eth else None
        )
        if not isinstance(report_summary.get('median_seconds_to_honest_holder'), (int, float)):
            report_summary['median_seconds_to_honest_holder'] = _payload_median_seconds_to_honest_holder(payload)
        if not isinstance(report_summary.get('median_mint_to_first_transfer_seconds'), (int, float)):
            report_summary['median_mint_to_first_transfer_seconds'] = _payload_median_mint_to_first_transfer_seconds(payload)
        report_entry: Dict[str, Any] = {
            'seed_contract': seed,
            'report_summary': report_summary,
        }
        if index < len(output_index):
            report_entry['output_files'] = output_index[index]
        elif payload.get('output_files'):
            report_entry['output_files'] = payload['output_files']
        reports.append(report_entry)

    distinct_chains = sorted(set(chains))
    repeat_infringing_address_count_global = sum(
        1 for contracts in minter_infringing_contracts.values() if len(contracts) > 1
    )
    total_malicious_addresses = len(malicious_address_global_set)
    total_honest_addresses = len(honest_address_global_set)
    return {
        'batch_summary': {
            'seed_report_count': len(payloads),
            'chain': distinct_chains[0] if len(distinct_chains) == 1 else '',
            'chains': distinct_chains,
            'open_license_detected_count': open_license_count,
            'candidate_contract_count_total': total_candidates,
            'high_confidence_contract_count_total': total_high,
            'low_confidence_contract_count_total': total_low,
            'infringing_nft_count_total': total_infringing_nfts,
            'malicious_address_count_total': total_malicious_addresses,
            'honest_address_count_total': total_honest_addresses,
            'repeat_infringing_address_count_total': total_repeat_infringing_addresses,
            'repeat_infringing_address_count_global': repeat_infringing_address_count_global,
            'legit_duplicate_contract_count_total': total_legit,
            'honest_purchase_total_eth_total': total_honest_purchase_eth,
            'stuck_cost_eth_total': total_stuck_cost_eth,
            'stuck_cost_ratio_overall': (total_stuck_cost_eth / total_honest_purchase_eth) if total_honest_purchase_eth else None,
            'buy_asset_ratio_known_address_count_total': total_ratio_known,
            'ratio_over_60_address_count_total': total_ratio_over_60,
            'ratio_over_60_address_ratio_overall': (total_ratio_over_60 / total_ratio_known) if total_ratio_known else None,
            'ratio_over_80_address_count_total': total_ratio_over_80,
            'ratio_over_80_address_ratio_overall': (total_ratio_over_80 / total_ratio_known) if total_ratio_known else None,
            'stuck_honest_address_count_total': total_stuck_honest,
            'stuck_honest_address_ratio_overall': (total_stuck_honest / total_ratio_known) if total_ratio_known else None,
            'avg_seconds_to_honest_holder_mean': (
                sum(mean_honest_holder_values) / len(mean_honest_holder_values) if mean_honest_holder_values else None
            ),
            'median_seconds_to_honest_holder_median': _format_numeric_median(median_honest_holder_values),
            'avg_mint_to_first_transfer_seconds_mean': (
                sum(mean_first_transfer_values) / len(mean_first_transfer_values) if mean_first_transfer_values else None
            ),
            'median_mint_to_first_transfer_seconds_median': _format_numeric_median(median_first_transfer_values),
            'avg_unique_receiver_count_mean': (
                sum(mean_unique_receiver_values) / len(mean_unique_receiver_values) if mean_unique_receiver_values else None
            ),
            'generated_at': datetime.now(timezone.utc).isoformat(),
        },
        'seed_reports': reports,
    }


def render_batch_human_readable_report(payload: Dict[str, Any]) -> str:
    summary = payload.get('batch_summary') or {}
    seed_reports = payload.get('seed_reports') or []
    lines = [
        '# Top NFT 合约批量分析总报告',
        '',
        '## 汇总',
        f"- 种子合约报告数: {summary.get('seed_report_count', 0)}",
        f"- 链: {summary.get('chain') or ', '.join(summary.get('chains') or []) or 'unknown'}",
        f"- 检测到开放许可的 seed 数: {summary.get('open_license_detected_count', 0)}",
        f"- 重复候选合约总数: {summary.get('candidate_contract_count_total', 0)}",
        f"- 高置信疑似侵权合约总数: {summary.get('high_confidence_contract_count_total', 0)}",
        f"- 低置信疑似侵权合约总数: {summary.get('low_confidence_contract_count_total', 0)}",
        f"- 疑似侵权 NFT 总数: {summary.get('infringing_nft_count_total', 0)}",
        f"- 恶意地址总数: {summary.get('malicious_address_count_total', 0)}",
        f"- 诚实地址总数: {summary.get('honest_address_count_total', 0)}",
        f"- 多次侵权地址总数(按 seed 求和): {summary.get('repeat_infringing_address_count_total', 0)}",
        f"- 多次侵权地址总数(跨批次全局去重): {summary.get('repeat_infringing_address_count_global', 0)}",
        f"- 官方参与型重复合约总数: {summary.get('legit_duplicate_contract_count_total', 0)}",
        f"- 诚实地址购买总金额(ETH/WETH)汇总: {summary.get('honest_purchase_total_eth_total', 0.0)}",
        f"- 套牢资金(ETH/WETH)汇总: {summary.get('stuck_cost_eth_total', 0.0)} / {_format_ratio(summary.get('stuck_cost_ratio_overall'))}",
        f"- 可计算买入占比的诚实地址总数: {summary.get('buy_asset_ratio_known_address_count_total', 0)}",
        f"- 买入金额占钱包总额 >60% 的地址数/总体占比: {summary.get('ratio_over_60_address_count_total', 0)} / {_format_ratio(summary.get('ratio_over_60_address_ratio_overall'))}",
        f"- 买入金额占钱包总额 >80% 的地址数/总体占比: {summary.get('ratio_over_80_address_count_total', 0)} / {_format_ratio(summary.get('ratio_over_80_address_ratio_overall'))}",
        f"- 购买后无法再次售出的诚实节点数/总体占比: {summary.get('stuck_honest_address_count_total', 0)} / {_format_ratio(summary.get('stuck_honest_address_ratio_overall'))}",
        f"- Mint 到被诚实节点购买平均时间(跨 seed 均值): {_format_scalar(summary.get('avg_seconds_to_honest_holder_mean'))} 秒",
        f"- 传播时间中位数(跨 seed 中位数): {_format_scalar(summary.get('median_seconds_to_honest_holder_median'))} 秒",
        f"- Mint 到首次转手平均时间(跨 seed 均值): {_format_scalar(summary.get('avg_mint_to_first_transfer_seconds_mean'))} 秒",
        f"- Mint 到首次转手中位数(跨 seed 中位数): {_format_scalar(summary.get('median_mint_to_first_transfer_seconds_median'))} 秒",
        f"- 候选合约平均唯一接收钱包数(跨 seed 均值): {_format_scalar(summary.get('avg_unique_receiver_count_mean'))}",
        f"- 生成时间(UTC): {summary.get('generated_at', '')}",
        '',
        '## Seed 报告索引',
    ]

    if not seed_reports:
        lines.append('- 无')
    else:
        for item in seed_reports:
            seed = item.get('seed_contract') or {}
            report_summary = item.get('report_summary') or {}
            output_files = item.get('output_files') or {}
            seed_name = seed.get('name') or seed.get('contract_address') or 'unknown'
            lines.append(
                f"- {seed_name} ({seed.get('contract_address', '')}) | "
                f"高置信={report_summary.get('high_confidence_contract_count', 0)} | "
                f"低置信={report_summary.get('low_confidence_contract_count', 0)} | "
                f"侵权NFT={report_summary.get('infringing_nft_count', 0)} | "
                f"恶意地址={report_summary.get('malicious_address_count', _count_unique_malicious_addresses(item))} | "
                f"诚实地址={report_summary.get('honest_address_count', _count_unique_honest_addresses(item))} | "
                f"多次侵权地址={report_summary.get('repeat_infringing_address_count', 0)} | "
                f"官方参与={report_summary.get('legit_duplicate_contract_count', 0)} | "
                f"诚实购买额={report_summary.get('honest_purchase_total_eth', 0.0)} | "
                f"套牢资金={report_summary.get('stuck_cost_eth', 0.0)}/{_format_ratio(report_summary.get('stuck_cost_ratio'))} | "
                f">60%={report_summary.get('ratio_over_60_address_count', 0)}/{_format_ratio(report_summary.get('ratio_over_60_address_ratio'))} | "
                f"套牢={report_summary.get('stuck_honest_address_count', 0)}/{_format_ratio(report_summary.get('stuck_honest_address_ratio'))} | "
                f"传播中位数={_format_scalar(report_summary.get('median_seconds_to_honest_holder'))}秒 | "
                f"首次转手中位数={_format_scalar(report_summary.get('median_mint_to_first_transfer_seconds'))}秒 | "
                f"JSON={output_files.get('json', '')} | MD={output_files.get('markdown', '')}"
            )

    return '\n'.join(lines) + '\n'


def write_batch_summary_outputs(
    payloads: Sequence[Dict[str, Any]],
    output_dir: str | Path,
    output_index: Optional[Sequence[Dict[str, str]]] = None,
) -> tuple[Path, Path]:
    summary_payload = build_batch_summary_payload(payloads, output_index=output_index)
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    json_path = target_dir / 'top_contract_analysis__summary.json'
    md_path = target_dir / 'top_contract_analysis__summary.md'
    dump_results(summary_payload, str(json_path))
    md_path.write_text(render_batch_human_readable_report(summary_payload), encoding='utf-8')
    return json_path, md_path


def _format_ratio(value: Any) -> str:
    if isinstance(value, (int, float)):
        return f'{value:.2%}'
    return 'n/a'


def _format_scalar(value: Any) -> str:
    if value is None:
        return 'n/a'
    return str(value)


def _format_numeric_median(values: Sequence[float]) -> float | None:
    cleaned = [float(value) for value in values if value is not None]
    if not cleaned:
        return None
    cleaned.sort()
    mid = len(cleaned) // 2
    if len(cleaned) % 2:
        return cleaned[mid]
    return (cleaned[mid - 1] + cleaned[mid]) / 2.0


def _payload_median_seconds_to_honest_holder(payload: Dict[str, Any]) -> float | None:
    values: list[float] = []
    for item in payload.get('honest_addresses') or []:
        for sample in item.get('mint_to_honest_seconds_samples') or []:
            if sample is not None:
                values.append(float(sample))
    return _format_numeric_median(values)


def _payload_median_mint_to_first_transfer_seconds(payload: Dict[str, Any]) -> float | None:
    values = [
        float(signal.get('mint_to_first_transfer_seconds'))
        for signal in (payload.get('address_signals') or {}).values()
        if signal.get('mint_to_first_transfer_seconds') is not None
    ]
    return _format_numeric_median(values)


def render_human_readable_report(payload: Dict[str, Any]) -> str:
    seed = payload.get('seed_contract') or {}
    seed_stats = payload.get('seed_collection_stats') or {}
    summary = payload.get('report_summary') or {}
    high = payload.get('suspected_infringing_duplicates_high_confidence') or []
    low = payload.get('suspected_infringing_duplicates_low_confidence') or []
    legit = payload.get('legit_duplicates') or []
    address_signals = payload.get('address_signals') or {}
    victim_signals = payload.get('victim_signals') or {}
    honest_addresses = payload.get('honest_addresses') or []
    honest_address_stats = payload.get('honest_address_stats') or {}
    victim_addresses = payload.get('victim_addresses') or []
    fraud_trade_stats = payload.get('fraud_trade_stats') or {}
    infringing_tokens = payload.get('infringing_tokens') or []
    repeat_minter_contracts: Dict[str, set[str]] = {}
    for item in infringing_tokens:
        minter = str(item.get('minter_address') or '').strip()
        contract = str(item.get('contract_address') or '').strip()
        if minter and contract:
            repeat_minter_contracts.setdefault(minter, set()).add(contract)
    repeat_infringing_address_count = sum(
        1 for contracts in repeat_minter_contracts.values() if len(contracts) > 1
    )
    median_seconds_to_honest_holder = summary.get('median_seconds_to_honest_holder')
    if not isinstance(median_seconds_to_honest_holder, (int, float)):
        median_seconds_to_honest_holder = _payload_median_seconds_to_honest_holder(payload)
    median_mint_to_first_transfer_seconds = summary.get('median_mint_to_first_transfer_seconds')
    if not isinstance(median_mint_to_first_transfer_seconds, (int, float)):
        median_mint_to_first_transfer_seconds = _payload_median_mint_to_first_transfer_seconds(payload)
    honest_purchase_total_eth = float(summary.get('honest_purchase_total_eth') or 0.0)
    stuck_cost_eth = float(summary.get('stuck_cost_eth') or _payload_stuck_cost_eth(payload))
    stuck_cost_ratio = (stuck_cost_eth / honest_purchase_total_eth) if honest_purchase_total_eth else None

    lines = [
        '# Top NFT 合约重复样本分析报告',
        '',
        '## 种子合约',
        f"- 链: {seed.get('chain', '')}",
        f"- 合约地址: {seed.get('contract_address', '')}",
        f"- 名称: {seed.get('name', '')}",
        f"- 符号: {seed.get('symbol', '')}",
        f"- Token 类型: {seed.get('token_type', '')}",
        f"- 合约部署者: {seed.get('contract_deployer', '') or 'unknown'}",
        f"- 部署区块: {seed.get('deployed_block_number', 0)}",
        '',
        '## 摘要',
        f"- 检测到开放许可: {'是' if summary.get('open_license_detected') else '否'}",
        f"- 重复候选合约数: {summary.get('candidate_contract_count', 0)}",
        f"- 高置信疑似侵权合约数: {summary.get('high_confidence_contract_count', 0)}",
        f"- 低置信疑似侵权合约数: {summary.get('low_confidence_contract_count', 0)}",
        f"- 疑似侵权 NFT 总数: {summary.get('infringing_nft_count', len(infringing_tokens))}",
        f"- 恶意地址数: {summary.get('malicious_address_count', _count_unique_malicious_addresses(payload))}",
        f"- 诚实地址数: {summary.get('honest_address_count', _count_unique_honest_addresses(payload))}",
        f"- 多次侵权地址数: {summary.get('repeat_infringing_address_count', repeat_infringing_address_count)}",
        f"- 归为官方参与型重复的合约数: {summary.get('legit_duplicate_contract_count', 0)}",
        f"- 候选侧开放许可 token 数: {summary.get('candidate_open_license_token_count', 0)}",
        f"- 候选侧开放许可合约数: {summary.get('candidate_open_license_contract_count', 0)}",
        f"- 诚实地址购买总金额(ETH/WETH): {honest_purchase_total_eth}",
        f"- 套牢资金(ETH/WETH): {stuck_cost_eth} / {_format_ratio(stuck_cost_ratio)}",
        f"- 可计算买入占比的诚实地址数: {summary.get('buy_asset_ratio_known_address_count', 0)}",
        f"- 买入金额占钱包总额 >60% 的地址数/占比: {summary.get('ratio_over_60_address_count', 0)} / {_format_ratio(summary.get('ratio_over_60_address_ratio'))}",
        f"- 买入金额占钱包总额 >80% 的地址数/占比: {summary.get('ratio_over_80_address_count', 0)} / {_format_ratio(summary.get('ratio_over_80_address_ratio'))}",
        f"- 购买后无法再次售出的诚实节点数/占比: {summary.get('stuck_honest_address_count', 0)} / {_format_ratio(summary.get('stuck_honest_address_ratio'))}",
        f"- Mint 到被诚实节点购买平均时间: {_format_scalar(summary.get('avg_seconds_to_honest_holder'))} 秒",
        f"- 传播时间中位数: {_format_scalar(median_seconds_to_honest_holder)} 秒",
        f"- Mint 到首次转手平均时间: {_format_scalar(summary.get('avg_mint_to_first_transfer_seconds'))} 秒",
        f"- Mint 到首次转手中位数: {_format_scalar(median_mint_to_first_transfer_seconds)} 秒",
        f"- 候选合约平均唯一接收钱包数: {_format_scalar(summary.get('avg_unique_receiver_count'))}",
        '',
        '## 种子集合统计',
        f"- 拉取到的种子 NFT 数: {seed_stats.get('seed_nft_count', 0)}",
        f"- 唯一 token URI 数: {seed_stats.get('unique_token_uri_count', 0)}",
        f"- 唯一 image URI 数: {seed_stats.get('unique_image_uri_count', 0)}",
        f"- 唯一规范化名称数: {seed_stats.get('unique_name_count', 0)}",
        f"- 唯一规范化符号数: {seed_stats.get('unique_symbol_count', 0)}",
        '',
        '## 高置信疑似侵权合约',
    ]

    if high:
        for item in high:
            lines.append(
                f"- {item.get('contract_address', '')}: {item.get('candidate_count', 0)} 个重复 NFT "
                f"| 命中原因={', '.join(item.get('match_reasons') or [])}"
            )
    else:
        lines.append('- 无')

    lines.extend(['', '## 低置信疑似侵权合约'])
    if low:
        for item in low:
            lines.append(
                f"- {item.get('contract_address', '')}: {item.get('candidate_count', 0)} 个重复 NFT "
                f"| 命中原因={', '.join(item.get('match_reasons') or [])}"
            )
    else:
        lines.append('- 无')

    lines.extend([
        '',
        '## 被算法归为官方参与型重复的合约',
        '- 说明: 该分组仅表示 mint 接收地址与官方地址集合存在交集。',
    ])
    if legit:
        for item in legit:
            recipients = ', '.join(item.get('mint_recipients') or [])
            lines.append(
                f"- {item.get('contract_address', '')}: {item.get('candidate_count', 0)} 个重复 NFT "
                f"| mint 接收地址(命中官方地址规则)={recipients}"
            )
    else:
        lines.append('- 无')

    lines.extend(['', '## 地址行为信号'])
    if address_signals:
        for contract, signal in address_signals.items():
            lines.extend([
                f"### {contract}",
                f"- Mint 地址数: {signal.get('mint_address_count', 0)}",
                f"- Mint 交易数: {signal.get('mint_count', 0)}",
                f"- 唯一接收地址数: {signal.get('unique_receiver_count', 0)}",
                f"- 循环交易边数: {signal.get('cycle_edge_count', 0)}",
                f"- 星状扩散中心数: {signal.get('star_distributor_count', 0)}",
                f"- Mint 到首次转手时间: {signal.get('mint_to_first_transfer_seconds', 0)} 秒",
                f"- 快速扩散: {'是' if signal.get('fast_spread') else '否'}",
            ])
    else:
        lines.append('- 无')

    lines.extend(['', '## 受害者信号'])
    if victim_signals:
        for contract, signal in victim_signals.items():
            lines.extend([
                f"### {contract}",
                f"- 当前持有地址数: {signal.get('owner_count', 0)}",
                f"- 套牢地址数: {signal.get('stuck_holder_count', 0)}",
                f"- 套牢地址占比: {_format_ratio(signal.get('stuck_holder_ratio'))}",
                f"- 疑似受害地址数: {signal.get('victim_wallet_count', 0)}",
            ])
    else:
        lines.append('- 无')

    lines.extend(['', '## 诚实地址画像'])
    if honest_address_stats:
        for contract, stats in honest_address_stats.items():
            lines.extend([
                f"### {contract}",
                f"- 诚实地址数: {stats.get('honest_address_count', 0)}",
                f"- 被腐化地址数: {stats.get('corrupted_address_count', 0)}",
                f"- 诚实地址之间转售次数: {stats.get('honest_to_honest_transfer_count', 0)}",
                f"- 持有时长中位数: {stats.get('median_holding_seconds')} 秒",
                f"- Mint 到诚实地址平均时间: {stats.get('avg_seconds_to_honest_holder')} 秒",
            ])
        if honest_addresses:
            for item in honest_addresses:
                lines.append(
                    f"- {item.get('contract_address', '')}:{item.get('address', '')}: "
                    f"interacted_token_count={item.get('interacted_token_count', 0)} | "
                    f"currently_holding_token_count={item.get('currently_holding_token_count', 0)} | "
                    f"hold_duration_median_seconds={item.get('hold_duration_median_seconds')} | "
                    f"被腐化={'是' if item.get('is_corrupted_address') else '否'} | "
                    f"honest_sale_to_honest_count={item.get('honest_sale_to_honest_count', 0)}"
                )
    else:
        lines.append('- 无')

    lines.extend(['', '## 被骗地址画像'])
    if victim_addresses:
        for item in victim_addresses:
            lines.append(
                f"- {item.get('address', '')}: buy_tx_count={len(item.get('buy_tx_hashes') or [])} | "
                f"买入金额(ETH/WETH)={item.get('buy_amount_eth', 0)} | "
                f"最后一次买入金额(ETH/WETH)={item.get('last_buy_amount_eth')} | "
                f"买入前 ETH 余额: {item.get('buy_before_eth_balance')} | "
                f"买入占比={_format_ratio(item.get('buy_asset_ratio'))} | "
                f"套牢={'是' if item.get('is_stuck') else '否'} | "
                f"last_buy_tx={item.get('last_buy_tx_hash', '') or 'n/a'}"
            )
    else:
        lines.append('- 无')

    lines.extend(['', '## 被骗交易与套牢资金'])
    if fraud_trade_stats:
        for contract, stats in fraud_trade_stats.items():
            lines.append(
                f"- {contract}: unique_buyers={stats.get('unique_buyers', 0)} | "
                f"eth_priced_sale_count={stats.get('eth_priced_sale_count', stats.get('native_eth_sale_count', 0))} | "
                f"eth_priced_volume={stats.get('eth_priced_volume', stats.get('native_eth_volume', 0))} | "
                f"stuck_wallet_count={stats.get('stuck_wallet_count', 0)} | "
                f"stuck_cost_eth={stats.get('stuck_cost_eth', 0)}"
            )
    else:
        lines.append('- 无')

    return '\n'.join(lines) + '\n'
