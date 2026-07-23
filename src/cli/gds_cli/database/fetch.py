"""Read data back from Neo4j: reconstruct a :class:`Graph`, summarize, and verify.

The ``verify_property`` helper is the core check for the whole prototype: after a
job writes an algorithm result back to the DB, it confirms the property landed on
(almost) every node and reports its value range.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from gds_cli.common.env import DatabaseConfig
from gds_cli.database.db import DEFAULT_EXTRA_LABEL, open_driver
from gds_cli.database.graph import Graph


def download_graph(
    db_config: DatabaseConfig,
    node_labels: list[str] | None = None,
) -> Graph:
    """Download Dev-labelled nodes and their relationships as a :class:`Graph`."""
    extra = DEFAULT_EXTRA_LABEL

    # Labels come from user input (`--label`), so filter on them as a query
    # parameter rather than interpolating into the Cypher string (no injection,
    # consistent with db.py). `extra` is the hardcoded `Dev` constant, safe inline.
    params: dict[str, Any] = {}
    if node_labels is not None:
        params["labels"] = node_labels
        label_filter = "WHERE any(l IN $labels WHERE l IN labels(n))"
        rel_filter = "WHERE any(l IN $labels WHERE l IN labels(a))\n  AND any(l IN $labels WHERE l IN labels(b))"
    else:
        label_filter = ""
        rel_filter = ""

    non_extra_label = f"[l IN labels(n) WHERE l <> '{extra}'][0]"
    non_extra_src = f"[l IN labels(a) WHERE l <> '{extra}'][0]"
    non_extra_tgt = f"[l IN labels(b) WHERE l <> '{extra}'][0]"

    with open_driver(db_config) as driver:
        with driver.session(database=db_config.database) as session:
            node_records = session.run(
                f"""
                MATCH (n:{extra})
                {label_filter}
                RETURN {non_extra_label} AS label, properties(n) AS props
                """,
                **params,
            ).data()

            rel_records = session.run(
                f"""
                MATCH (a:{extra})-[r]->(b:{extra})
                {rel_filter}
                RETURN {non_extra_src} AS src_label,
                       type(r) AS rel_type,
                       {non_extra_tgt} AS tgt_label,
                       a.nodeId AS sourceNodeId,
                       b.nodeId AS targetNodeId,
                       properties(r) AS props
                """,
                **params,
            ).data()

    node_rows: dict[str, list[dict[str, Any]]] = {}
    for record in node_records:
        node_rows.setdefault(record["label"], []).append(record["props"])

    node_dfs = {}
    for label, rows in node_rows.items():
        df = pd.DataFrame(rows)
        df["labels"] = label
        node_dfs[label] = df

    rel_rows: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for record in rel_records:
        key = (record["src_label"], record["rel_type"], record["tgt_label"])
        rel_rows.setdefault(key, []).append(
            {
                "sourceNodeId": record["sourceNodeId"],
                "targetNodeId": record["targetNodeId"],
                **record["props"],
            }
        )

    rel_dfs = {}
    for key, rows in rel_rows.items():
        df = pd.DataFrame(rows)
        df["relationshipType"] = key[1]
        rel_dfs[key] = df

    return Graph(node_dfs, rel_dfs)


@dataclass
class GraphStats:
    node_counts: dict[str, int]
    rel_counts: dict[str, int]

    @property
    def total_nodes(self) -> int:
        return sum(self.node_counts.values())

    @property
    def total_rels(self) -> int:
        return sum(self.rel_counts.values())


def stats(db_config: DatabaseConfig) -> GraphStats:
    """Count Dev-labelled nodes per label and relationships per type."""
    extra = DEFAULT_EXTRA_LABEL
    with open_driver(db_config) as driver:
        with driver.session(database=db_config.database) as session:
            node_rows = session.run(
                f"""
                MATCH (n:{extra})
                UNWIND [l IN labels(n) WHERE l <> '{extra}'] AS label
                RETURN label, count(*) AS count
                """
            ).data()
            rel_rows = session.run(
                f"MATCH (:{extra})-[r]->(:{extra}) RETURN type(r) AS rel_type, count(r) AS count"
            ).data()
    return GraphStats(
        node_counts={r["label"]: r["count"] for r in node_rows},
        rel_counts={r["rel_type"]: r["count"] for r in rel_rows},
    )


@dataclass
class PropertyStats:
    property_name: str
    total_nodes: int
    with_property: int
    min: float | None
    max: float | None

    @property
    def coverage(self) -> float:
        return self.with_property / self.total_nodes if self.total_nodes else 0.0


def verify_property(db_config: DatabaseConfig, property_name: str) -> PropertyStats:
    """Report how many Dev-labelled nodes carry ``property_name`` and its value range.

    Numeric min/max are only computed when the property is numeric; for
    non-numeric (e.g. list-valued embeddings) min/max come back as None.
    """
    extra = DEFAULT_EXTRA_LABEL
    with open_driver(db_config) as driver:
        with driver.session(database=db_config.database) as session:
            total_record = session.run(f"MATCH (n:{extra}) RETURN count(n) AS c").single()
            assert total_record is not None
            total = total_record["c"]
            with_prop_record = session.run(
                f"MATCH (n:{extra}) WHERE n[$p] IS NOT NULL RETURN count(n) AS c",
                p=property_name,
            ).single()
            assert with_prop_record is not None
            with_prop = with_prop_record["c"]
            bounds = session.run(
                f"""
                MATCH (n:{extra}) WHERE n[$p] IS NOT NULL AND (n[$p] IS :: INTEGER OR n[$p] IS :: FLOAT)
                RETURN min(n[$p]) AS lo, max(n[$p]) AS hi
                """,
                p=property_name,
            ).single()
    return PropertyStats(
        property_name=property_name,
        total_nodes=total,
        with_property=with_prop,
        min=bounds["lo"] if bounds else None,
        max=bounds["hi"] if bounds else None,
    )
