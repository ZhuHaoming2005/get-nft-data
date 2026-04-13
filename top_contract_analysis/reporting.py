from __future__ import annotations

import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Sequence


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
    total_legit = 0
    open_license_count = 0
    chains: list[str] = []
    output_index = output_index or []

    for index, payload in enumerate(payloads):
        seed = payload.get('seed_contract') or {}
        summary = payload.get('report_summary') or {}
        if summary.get('open_license_detected'):
            open_license_count += 1
        total_candidates += int(summary.get('candidate_contract_count') or 0)
        total_high += int(summary.get('high_confidence_contract_count') or 0)
        total_low += int(summary.get('low_confidence_contract_count') or 0)
        total_legit += int(summary.get('legit_duplicate_contract_count') or 0)
        chain = str(seed.get('chain') or '').strip()
        if chain:
            chains.append(chain)

        report_entry: Dict[str, Any] = {
            'seed_contract': seed,
            'report_summary': summary,
        }
        if index < len(output_index):
            report_entry['output_files'] = output_index[index]
        elif payload.get('output_files'):
            report_entry['output_files'] = payload['output_files']
        reports.append(report_entry)

    distinct_chains = sorted(set(chains))
    return {
        'batch_summary': {
            'seed_report_count': len(payloads),
            'chain': distinct_chains[0] if len(distinct_chains) == 1 else '',
            'chains': distinct_chains,
            'open_license_detected_count': open_license_count,
            'candidate_contract_count_total': total_candidates,
            'high_confidence_contract_count_total': total_high,
            'low_confidence_contract_count_total': total_low,
            'legit_duplicate_contract_count_total': total_legit,
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
        f"- 官方参与型重复合约总数: {summary.get('legit_duplicate_contract_count_total', 0)}",
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
                f"官方参与={report_summary.get('legit_duplicate_contract_count', 0)} | "
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
        '## 种子集合统计',
        f"- 拉取到的种子 NFT 数: {seed_stats.get('seed_nft_count', 0)}",
        f"- 唯一 token URI 数: {seed_stats.get('unique_token_uri_count', 0)}",
        f"- 唯一 image URI 数: {seed_stats.get('unique_image_uri_count', 0)}",
        f"- 唯一规范化名称数: {seed_stats.get('unique_name_count', 0)}",
        f"- 唯一规范化符号数: {seed_stats.get('unique_symbol_count', 0)}",
        '',
        '## 摘要',
        f"- 检测到开放许可: {'是' if summary.get('open_license_detected') else '否'}",
        f"- 重复候选合约数: {summary.get('candidate_contract_count', 0)}",
        f"- 高置信疑似侵权合约数: {summary.get('high_confidence_contract_count', 0)}",
        f"- 低置信疑似侵权合约数: {summary.get('low_confidence_contract_count', 0)}",
        f"- 归为官方参与型重复的合约数: {summary.get('legit_duplicate_contract_count', 0)}",
        f"- 候选侧开放许可 token 数: {summary.get('candidate_open_license_token_count', 0)}",
        f"- 候选侧开放许可合约数: {summary.get('candidate_open_license_contract_count', 0)}",
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
