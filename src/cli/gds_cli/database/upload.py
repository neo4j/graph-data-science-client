"""Batched upload of a :class:`Graph` into Neo4j (vendored / adapted).

Nodes are created with a dynamic label plus the ``Dev`` extra label and their
business ``nodeId`` is mapped to the Neo4j ``elementId``; relationships are then
created by matching endpoints on that ``elementId``.
"""

from __future__ import annotations

from typing import Any

from neo4j import GraphDatabase, ManagedTransaction
from tqdm.auto import tqdm

from gds_cli.common.env import DatabaseConfig
from gds_cli.database.db import DEFAULT_EXTRA_LABEL, delete_graph, graph_exists
from gds_cli.database.graph import Graph


def _batched(records: list[Any], size: int) -> Any:
    for i in range(0, len(records), size):
        yield records[i : i + size]


def _load_nodes(tx: ManagedTransaction, label: str, dev_label: str, batch: list[Any]) -> dict[Any, Any]:
    result = tx.run(
        """//cypher
        UNWIND $nodes AS props
        CREATE (n:$($label):$($dev_label))
        SET n = props
        RETURN props.nodeId AS nodeId, elementId(n) AS elementId
        """,
        label=label,
        dev_label=dev_label,
        nodes=batch,
    )
    return {record["nodeId"]: record["elementId"] for record in result}


def _load_rels(tx: ManagedTransaction, rel_type: str, batch: list[Any]) -> None:
    tx.run(
        """//cypher
        UNWIND $rels AS rel
        MATCH (a) WHERE elementId(a) = rel.srcElementId
        MATCH (b) WHERE elementId(b) = rel.tgtElementId
        CREATE (a)-[r:$($rel_type)]->(b)
        SET r = rel.props
        """,
        rels=batch,
        rel_type=rel_type,
    )


def upload_graph(
    graph: Graph,
    db_config: DatabaseConfig,
    batch_size: int = 5_000,
    overwrite: bool = False,
    show_progress: bool = True,
) -> None:
    dev_label = DEFAULT_EXTRA_LABEL
    node_labels = list(graph.node_dfs.keys())

    if graph_exists(db_config, node_labels):
        if not overwrite:
            raise RuntimeError(
                f"Database '{db_config.database}' already contains nodes with label '{dev_label}' "
                f"and one of {node_labels}. Pass overwrite=True to delete them first."
            )
        delete_graph(db_config, node_labels)

    node_id_map: dict[Any, Any] = {}

    with GraphDatabase.driver(db_config.uri, auth=db_config.auth) as driver:
        with driver.session(database=db_config.database) as session:
            # Load nodes
            for label, df in graph.node_dfs.items():
                node_cols = [c for c in df.columns if c != "labels"]
                records = df[node_cols].to_dict(orient="records")
                with tqdm(
                    total=len(records), desc=f"Nodes (:{label})", unit="node", disable=not show_progress, leave=False
                ) as pbar:
                    for batch in _batched(records, batch_size):
                        node_id_map.update(session.execute_write(_load_nodes, label, dev_label, batch))
                        pbar.update(len(batch))

            # Load relationships
            for (src_label, rel_type, tgt_label), df in graph.rel_dfs.items():
                rel_cols = [c for c in df.columns if c != "relationshipType"]
                records = df[rel_cols].to_dict(orient="records")
                rels = [
                    {
                        "srcElementId": node_id_map[r["sourceNodeId"]],
                        "tgtElementId": node_id_map[r["targetNodeId"]],
                        "props": {k: v for k, v in r.items() if k not in ("sourceNodeId", "targetNodeId")},
                    }
                    for r in records
                ]
                desc = f"Edges (:{src_label})-[:{rel_type}]->(:{tgt_label})"
                with tqdm(total=len(rels), desc=desc, unit="edge", disable=not show_progress, leave=False) as pbar:
                    for batch in _batched(rels, batch_size):
                        session.execute_write(_load_rels, rel_type, batch)
                        pbar.update(len(batch))
