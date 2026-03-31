#!/usr/bin/env python3
"""
统计 JSON 数组中 metadata_url 与 image_url 同时有内容的条数（流式，不加载整文件）。

输出两组核心数字：
  - 两 URI strip 后均非空
  - 两 URI 均通过 import_eth 的 URL 校验（占位符 / data: 等会排除）

依赖：pip install ijson（可选；无则整文件 json.load）

用法：
  python count_json_double_uri.py
  python count_json_double_uri.py "classification&status/*.json"
  python count_json_double_uri.py -pattern "data/*.json"
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from glob import glob
from typing import Any, Dict, Iterator, Tuple

try:
    import ijson

    _HAS_IJSON = True
except ImportError:
    _HAS_IJSON = False

_SKIP_VALUES = frozenset(
    {
        "nano",
        "null",
        "none",
        "undefined",
        "n/a",
        "na",
        "-",
        ".",
        "false",
        "true",
        "0",
    }
)


def _is_valid_url(raw: str) -> bool:
    """与 import_eth.py 一致：有效 URL 字段（非占位、非 data:）。"""
    s = raw.strip()
    if not s:
        return False
    lo = s.lower()
    if lo.startswith("data:"):
        return False
    return lo not in _SKIP_VALUES


def _coerce_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    return str(v).strip()


def _iter_records(path: str) -> Iterator[Dict[str, Any]]:
    if _HAS_IJSON:
        with open(path, "rb") as f:
            yield from ijson.items(f, "item")
        return
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        yield from data
    elif isinstance(data, dict):
        yield data


def _classify(meta: str, img: str) -> Tuple[bool, bool]:
    """返回 (两串均非空 strip, 两串均符合 import_eth URL 规则)。"""
    ne = bool(meta) and bool(img)
    ok = _is_valid_url(meta) and _is_valid_url(img)
    return ne, ok


def main() -> None:
    ap = argparse.ArgumentParser(
        description="统计 JSON 中 metadata_url 与 image_url 同时非空的数量"
    )
    ap.add_argument(
        "-pattern",
        "--pattern",
        default="classification&status/*.json",
        help="glob 模式",
    )
    ap.add_argument(
        "positional_pattern",
        nargs="?",
        default=None,
        help="若提供则覆盖 -pattern",
    )
    args = ap.parse_args()

    if not _HAS_IJSON:
        print(
            "警告: 未安装 ijson，将整文件加载；大文件请 pip install ijson",
            file=sys.stderr,
        )

    glob_pattern = (
        args.positional_pattern
        if args.positional_pattern is not None
        else args.pattern
    )
    files = sorted(glob(glob_pattern))
    if not files:
        print("未找到匹配文件: %s" % glob_pattern, file=sys.stderr)
        sys.exit(1)

    total = 0
    both_nonempty = 0
    both_import_ok = 0
    only_meta = 0
    only_img = 0
    neither = 0

    for path in files:
        base = os.path.basename(path)
        try:
            for rec in _iter_records(path):
                total += 1
                meta = _coerce_str(rec.get("metadata_url"))
                img = _coerce_str(rec.get("image_url"))
                ne, ok = _classify(meta, img)
                if ne:
                    both_nonempty += 1
                if ok:
                    both_import_ok += 1
                if meta and not img:
                    only_meta += 1
                elif img and not meta:
                    only_img += 1
                elif not meta and not img:
                    neither += 1
        except Exception as e:
            print("读取失败 %s: %s" % (base, e), file=sys.stderr)
            sys.exit(1)

    sep = "═" * 56
    print(sep)
    print("  文件数:     %d" % len(files))
    print("  模式:       %s" % glob_pattern)
    print("  总记录数:   %d" % total)
    print()
    print("  两 URI strip 后均非空:           %d" % both_nonempty)
    print("  两 URI 均符合 import_eth 校验:   %d" % both_import_ok)
    print("    （与入库一致时需合约+identifier 仍有效，见 import_eth）")
    print()
    print("  仅 metadata_url 非空:           %d" % only_meta)
    print("  仅 image_url 非空:              %d" % only_img)
    print("  两者皆空:                       %d" % neither)
    print(sep)


if __name__ == "__main__":
    main()
