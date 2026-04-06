from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Sequence

from psycopg2.extras import execute_values

from .progress import ProgressPrinter
from .report import DuplicateGroupStats, SummaryRow, summarize_groups

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Atom:
    atom_id: int
    chain: str
    name_norm: str
    contract_count: int
    nft_count: int


@dataclass(frozen=True)
class Edge:
    left_atom_id: int
    right_atom_id: int
    similarity_score: float


class UnionFind:
    def __init__(self, node_ids: Iterable[int]) -> None:
        self.parent = {node_id: node_id for node_id in node_ids}
        self.rank = {node_id: 0 for node_id in node_ids}

    def find(self, node_id: int) -> int:
        parent = self.parent[node_id]
        if parent != node_id:
            self.parent[node_id] = self.find(parent)
        return self.parent[node_id]

    def union(self, left: int, right: int) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        if self.rank[left_root] < self.rank[right_root]:
            self.parent[left_root] = right_root
            return
        if self.rank[left_root] > self.rank[right_root]:
            self.parent[right_root] = left_root
            return
        self.parent[right_root] = left_root
        self.rank[left_root] += 1

    def groups(self) -> list[list[int]]:
        grouped: dict[int, list[int]] = defaultdict(list)
        for node_id in self.parent:
            grouped[self.find(node_id)].append(node_id)
        return list(grouped.values())


def _load_chain_totals(conn, run_label: str, chains: Sequence[str]) -> dict[str, tuple[int, int]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT chain, count(*)::int, coalesce(sum(nft_count), 0)::bigint
            FROM nsv2_contract_identity
            WHERE run_label = %s AND chain = ANY(%s)
            GROUP BY chain
            """,
            (run_label, list(chains)),
        )
        rows = {chain: (contract_count, nft_count) for chain, contract_count, nft_count in cur.fetchall()}
    return {chain: rows.get(chain, (0, 0)) for chain in chains}


def _task_rows(conn, run_label: str) -> list[tuple[int, str, str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, task_key, name_block_key, signature_prefix
            FROM nsv2_name_work_items
            WHERE run_label = %s
              AND status = 'done'
            ORDER BY id
            """,
            (run_label,),
        )
        return list(cur.fetchall())


def _load_atoms_for_task(conn, run_label: str, block_key: str, signature_prefix: str) -> dict[int, Atom]:
    sql = """
        SELECT atom_id, chain, name_norm, contract_count, nft_count
        FROM nsv2_name_atoms
        WHERE run_label = %s
          AND name_block_key = %s
    """
    params: list[object] = [run_label, block_key]
    if signature_prefix:
        sql += " AND left(name_signature_hash, %s) = %s"
        params.extend([len(signature_prefix), signature_prefix])
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return {
            int(atom_id): Atom(
                atom_id=int(atom_id),
                chain=chain,
                name_norm=name_norm,
                contract_count=int(contract_count),
                nft_count=int(nft_count),
            )
            for atom_id, chain, name_norm, contract_count, nft_count in cur.fetchall()
        }


def _load_edges_for_task(conn, run_label: str, task_id: int) -> list[Edge]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT left_atom_id, right_atom_id, similarity_score
            FROM nsv2_name_match_edges
            WHERE run_label = %s
              AND task_id = %s
            """,
            (run_label, task_id),
        )
        return [Edge(int(left_atom_id), int(right_atom_id), float(similarity_score)) for left_atom_id, right_atom_id, similarity_score in cur.fetchall()]


def _component_groups(atoms: dict[int, Atom], edges: Sequence[Edge], *, threshold: float) -> list[list[int]]:
    union_find = UnionFind(atoms.keys())
    for edge in edges:
        if edge.similarity_score >= threshold:
            union_find.union(edge.left_atom_id, edge.right_atom_id)
    return union_find.groups()


def _component_groups_by_thresholds(
    atoms: dict[int, Atom],
    edges: Sequence[Edge],
    thresholds: Sequence[float],
) -> dict[float, list[list[int]]]:
    unique_thresholds = sorted(set(thresholds), reverse=True)
    if not unique_thresholds:
        return {}

    sorted_edges = sorted(edges, key=lambda edge: edge.similarity_score, reverse=True)
    union_find = UnionFind(atoms.keys())
    edge_index = 0
    groups_by_threshold: dict[float, list[list[int]]] = {}

    for threshold in unique_thresholds:
        while edge_index < len(sorted_edges) and sorted_edges[edge_index].similarity_score >= threshold:
            edge = sorted_edges[edge_index]
            union_find.union(edge.left_atom_id, edge.right_atom_id)
            edge_index += 1
        groups_by_threshold[threshold] = union_find.groups()

    return groups_by_threshold


def _build_group(group_nodes: Sequence[int], atoms: dict[int, Atom], primary_chain: str) -> DuplicateGroupStats | None:
    group_atoms = [atoms[node_id] for node_id in group_nodes]
    primary_atoms = [atom for atom in group_atoms if atom.chain == primary_chain]
    if not primary_atoms:
        return None
    total_contract_count = sum(atom.contract_count for atom in group_atoms)
    primary_contract_count = sum(atom.contract_count for atom in primary_atoms)
    if total_contract_count < 2 and primary_contract_count < 2:
        return None
    digest = hashlib.sha1('|'.join(sorted(f'{atom.chain}:{atom.name_norm}' for atom in group_atoms)).encode('utf-8')).hexdigest()[:16]
    return DuplicateGroupStats(
        group_key=digest,
        primary_contract_count=primary_contract_count,
        primary_nft_count=sum(atom.nft_count for atom in primary_atoms),
        total_contract_count=total_contract_count,
        total_nft_count=sum(atom.nft_count for atom in group_atoms),
        node_count=len(group_atoms),
        sample_value=min(atom.name_norm for atom in group_atoms),
    )


def _scope_cache_key(scope: str, primary_chain: str, secondary_chain: str) -> tuple[str, str, str]:
    if scope == 'chain_matrix':
        left, right = sorted((primary_chain, secondary_chain))
        return scope, left, right
    if scope == 'cross_chain_summary':
        return scope, '', ''
    return scope, primary_chain, ''


def _resolve_scope_graph(
    atoms: dict[int, Atom],
    edges: Sequence[Edge],
    *,
    scope: str,
    primary_chain: str,
    secondary_chain: str,
    scope_data_cache: dict[tuple[str, str, str], tuple[dict[int, Atom], list[Edge]]] | None = None,
) -> tuple[dict[int, Atom], list[Edge]]:
    cache_key = _scope_cache_key(scope, primary_chain, secondary_chain)
    if scope_data_cache is not None and cache_key in scope_data_cache:
        return scope_data_cache[cache_key]

    if scope == 'intra_chain':
        scoped_atoms = {atom_id: atom for atom_id, atom in atoms.items() if atom.chain == primary_chain}
        scoped_edges = [edge for edge in edges if atoms[edge.left_atom_id].chain == primary_chain and atoms[edge.right_atom_id].chain == primary_chain]
    elif scope == 'cross_chain_summary':
        scoped_edges = [edge for edge in edges if atoms[edge.left_atom_id].chain != atoms[edge.right_atom_id].chain]
        node_ids = {edge.left_atom_id for edge in scoped_edges} | {edge.right_atom_id for edge in scoped_edges}
        scoped_atoms = {atom_id: atoms[atom_id] for atom_id in node_ids}
    else:
        scoped_edges = [
            edge for edge in edges
            if {atoms[edge.left_atom_id].chain, atoms[edge.right_atom_id].chain} == {primary_chain, secondary_chain}
        ]
        node_ids = {edge.left_atom_id for edge in scoped_edges} | {edge.right_atom_id for edge in scoped_edges}
        scoped_atoms = {atom_id: atoms[atom_id] for atom_id in node_ids}

    if scope_data_cache is not None:
        scope_data_cache[cache_key] = (scoped_atoms, scoped_edges)
    return scoped_atoms, scoped_edges


def _groups_for_scope(
    atoms: dict[int, Atom],
    edges: Sequence[Edge],
    *,
    scope: str,
    primary_chain: str,
    secondary_chain: str,
    threshold: float,
    all_thresholds: Sequence[float] | None = None,
    scope_data_cache: dict[tuple[str, str, str], tuple[dict[int, Atom], list[Edge]]] | None = None,
    components_cache: dict[tuple[str, str, str], dict[float, list[list[int]]]] | None = None,
) -> list[DuplicateGroupStats]:
    scoped_atoms, scoped_edges = _resolve_scope_graph(
        atoms,
        edges,
        scope=scope,
        primary_chain=primary_chain,
        secondary_chain=secondary_chain,
        scope_data_cache=scope_data_cache,
    )

    if not scoped_atoms:
        return []

    component_cache_key = _scope_cache_key(scope, primary_chain, secondary_chain)
    if components_cache is not None and component_cache_key in components_cache:
        components_by_threshold = components_cache[component_cache_key]
    else:
        thresholds_to_compute = list(all_thresholds or [threshold])
        components_by_threshold = _component_groups_by_thresholds(scoped_atoms, scoped_edges, thresholds_to_compute)
        if components_cache is not None:
            components_cache[component_cache_key] = components_by_threshold

    components = components_by_threshold[threshold]
    groups: list[DuplicateGroupStats] = []
    for component in components:
        group = _build_group(component, scoped_atoms, primary_chain)
        if group is None:
            continue
        if scope == 'cross_chain_summary':
            chains_in_group = {scoped_atoms[node_id].chain for node_id in component}
            if primary_chain not in chains_in_group or len(chains_in_group) < 2:
                continue
        if scope == 'chain_matrix':
            chains_in_group = {scoped_atoms[node_id].chain for node_id in component}
            if chains_in_group != {primary_chain, secondary_chain}:
                continue
        groups.append(group)
    return groups


def _group_insert_rows(
    run_label: str,
    scope: str,
    primary_chain: str,
    secondary_chain: str,
    threshold: float,
    task_key: str,
    groups: Sequence[DuplicateGroupStats],
) -> list[tuple[object, ...]]:
    return [
        (
            run_label,
            'name',
            scope,
            primary_chain,
            secondary_chain,
            threshold,
            task_key,
            group.group_key,
            group.sample_value,
            group.primary_contract_count,
            group.primary_nft_count,
            group.total_contract_count,
            group.total_nft_count,
            group.node_count,
        )
        for group in groups
    ]


def _insert_group_rows(conn, rows: Sequence[tuple[object, ...]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO nsv2_name_duplicate_groups (
                run_label, field_name, scope, primary_chain, secondary_chain, threshold, task_key,
                group_key, sample_value, primary_contract_count, primary_nft_count,
                total_contract_count, total_nft_count, node_count
            ) VALUES %s
            """,
            list(rows),
            page_size=1000,
        )


def _insert_summary_rows(conn, rows: Sequence[SummaryRow]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO nsv2_duplicate_summary (
                run_label, field_name, scope, primary_chain, secondary_chain, threshold,
                total_contracts, total_nfts, group_count, duplicate_contract_count, duplicate_nft_count,
                duplicate_contract_ratio, duplicate_nft_ratio, group_size_ge_2_count, group_size_gt_2_count
            ) VALUES %s
            """,
            [
                (
                    row.run_label,
                    row.field_name,
                    row.scope,
                    row.primary_chain,
                    row.secondary_chain,
                    -1.0 if row.threshold is None else row.threshold,
                    row.total_contracts,
                    row.total_nfts,
                    row.group_count,
                    row.duplicate_contract_count,
                    row.duplicate_nft_count,
                    row.duplicate_contract_ratio,
                    row.duplicate_nft_ratio,
                    row.group_size_ge_2_count,
                    row.group_size_gt_2_count,
                )
                for row in rows
            ],
            page_size=1000,
        )


def finalize_name_stats(conn, run_label: str, chains: Sequence[str], thresholds: Sequence[float]) -> list[SummaryRow]:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM nsv2_name_duplicate_groups
            WHERE run_label = %s
              AND (primary_chain = ANY(%s) OR secondary_chain = ANY(%s))
            """,
            (run_label, list(chains), list(chains)),
        )
        cur.execute(
            """
            DELETE FROM nsv2_duplicate_summary
            WHERE run_label = %s
              AND field_name = 'name'
              AND (primary_chain = ANY(%s) OR secondary_chain = ANY(%s))
            """,
            (run_label, list(chains), list(chains)),
        )
    conn.commit()

    task_rows = _task_rows(conn, run_label)
    tracker = ProgressPrinter('finalize-name-stats', len(task_rows), 'tasks', logger)
    chain_totals = _load_chain_totals(conn, run_label, chains)
    aggregate_groups: dict[tuple[str, str, str, float], list[DuplicateGroupStats]] = defaultdict(list)
    processed_tasks = 0
    inserted_group_count = 0

    for task_id, task_key, block_key, signature_prefix in task_rows:
        atoms = _load_atoms_for_task(conn, run_label, block_key, signature_prefix)
        edges = _load_edges_for_task(conn, run_label, task_id)
        task_group_rows: list[tuple[object, ...]] = []
        scope_data_cache: dict[tuple[str, str, str], tuple[dict[int, Atom], list[Edge]]] = {}
        components_cache: dict[tuple[str, str, str], dict[float, list[list[int]]]] = {}

        for threshold in thresholds:
            for primary_chain in chains:
                groups = _groups_for_scope(
                    atoms,
                    edges,
                    scope='intra_chain',
                    primary_chain=primary_chain,
                    secondary_chain='',
                    threshold=threshold,
                    all_thresholds=thresholds,
                    scope_data_cache=scope_data_cache,
                    components_cache=components_cache,
                )
                aggregate_groups[('intra_chain', primary_chain, '', threshold)].extend(groups)
                inserted_group_count += len(groups)
                task_group_rows.extend(_group_insert_rows(run_label, 'intra_chain', primary_chain, '', threshold, task_key, groups))

                groups = _groups_for_scope(
                    atoms,
                    edges,
                    scope='cross_chain_summary',
                    primary_chain=primary_chain,
                    secondary_chain='',
                    threshold=threshold,
                    all_thresholds=thresholds,
                    scope_data_cache=scope_data_cache,
                    components_cache=components_cache,
                )
                aggregate_groups[('cross_chain_summary', primary_chain, '', threshold)].extend(groups)
                inserted_group_count += len(groups)
                task_group_rows.extend(_group_insert_rows(run_label, 'cross_chain_summary', primary_chain, '', threshold, task_key, groups))

                for secondary_chain in chains:
                    if secondary_chain == primary_chain:
                        continue
                    groups = _groups_for_scope(
                        atoms,
                        edges,
                        scope='chain_matrix',
                        primary_chain=primary_chain,
                        secondary_chain=secondary_chain,
                        threshold=threshold,
                    all_thresholds=thresholds,
                    scope_data_cache=scope_data_cache,
                        components_cache=components_cache,
                    )
                    aggregate_groups[('chain_matrix', primary_chain, secondary_chain, threshold)].extend(groups)
                    inserted_group_count += len(groups)
                    task_group_rows.extend(_group_insert_rows(run_label, 'chain_matrix', primary_chain, secondary_chain, threshold, task_key, groups))
        _insert_group_rows(conn, task_group_rows)
        processed_tasks += 1
        tracker.update(processed_tasks, extra=f'task={task_key} groups={inserted_group_count}')

    summary_rows: list[SummaryRow] = []
    for (scope, primary_chain, secondary_chain, threshold), groups in aggregate_groups.items():
        total_contracts, total_nfts = chain_totals.get(primary_chain, (0, 0))
        summary_rows.append(
            summarize_groups(
                run_label=run_label,
                field_name='name',
                scope=scope,
                primary_chain=primary_chain,
                secondary_chain=secondary_chain,
                threshold=threshold,
                total_contracts=total_contracts,
                total_nfts=total_nfts,
                groups=groups,
            )
        )

    _insert_summary_rows(conn, summary_rows)
    conn.commit()
    tracker.close(extra=f'groups={inserted_group_count} summary_rows={len(summary_rows)}')
    logger.info('wrote %d name summary rows for run %s', len(summary_rows), run_label)
    return summary_rows






