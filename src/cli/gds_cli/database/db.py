"""Database connection helpers and a thin client over the vendored graph tools.

Reuses :class:`gds_cli.common.env.DatabaseConfig` (built from the
unified env set) so ``gds database`` and ``gds session`` never diverge on
connection details.

Every node written by this tool gets an extra ``Dev`` label so test data can be
found and cleaned up without touching real data.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from neo4j import GraphDatabase

from gds_cli.common.env import DatabaseConfig, database_config_from_env
from gds_cli.database.graph import Graph

DEFAULT_EXTRA_LABEL = "Dev"

__all__ = [
    "DatabaseConfig",
    "DatabaseClient",
    "DeleteStats",
    "DEFAULT_EXTRA_LABEL",
    "graph_exists",
    "delete_graph",
]


@dataclass
class DeleteStats:
    """How many nodes and relationships a delete removed (or would remove)."""

    nodes: int
    relationships: int


def graph_exists(db_config: DatabaseConfig, labels: list[str]) -> bool:
    """Return True if any node with the extra label AND one of the given labels exists."""
    with GraphDatabase.driver(db_config.uri, auth=db_config.auth) as driver:
        with driver.session(database=db_config.database) as session:
            result = session.run(
                f"MATCH (n:{DEFAULT_EXTRA_LABEL}) WHERE any(lbl IN $labels WHERE lbl IN labels(n))"
                " RETURN count(n) > 0 AS exists LIMIT 1",
                labels=labels,
            ).single()
            return bool(result["exists"]) if result else False


def _dev_match(labels: list[str] | None) -> tuple[str, dict[str, Any]]:
    """Build the MATCH clause (and params) selecting the Dev nodes to delete."""
    if labels is None:
        return f"MATCH (n:{DEFAULT_EXTRA_LABEL})", {}
    return (
        f"MATCH (n:{DEFAULT_EXTRA_LABEL}) WHERE any(lbl IN $labels WHERE lbl IN labels(n))",
        {"labels": labels},
    )


def delete_graph(
    db_config: DatabaseConfig,
    labels: list[str] | None = None,
    *,
    dry_run: bool = False,
) -> DeleteStats:
    """Delete Dev-labelled nodes (and their relationships); return the counts.

    If ``labels`` is None, target all Dev-labelled nodes; otherwise only those
    that also carry one of the given labels. With ``dry_run=True`` nothing is
    deleted and the returned counts reflect what *would* be removed.
    """
    match, params = _dev_match(labels)
    with GraphDatabase.driver(db_config.uri, auth=db_config.auth) as driver:
        with driver.session(database=db_config.database) as session:
            counts = session.run(
                f"{match} "
                "OPTIONAL MATCH (n)-[r]-() "
                "RETURN count(DISTINCT n) AS nodes, count(DISTINCT r) AS relationships",
                **params,
            ).single()
            assert counts is not None
            stats = DeleteStats(nodes=counts["nodes"], relationships=counts["relationships"])
            if not dry_run and stats.nodes:
                session.run(f"{match} DETACH DELETE n", **params)
    return stats


class DatabaseClient:
    """Convenience wrapper around upload / fetch / delete for a single database."""

    def __init__(self, db_config: DatabaseConfig) -> None:
        self.db_config = db_config

    @classmethod
    def from_config(cls, config: DatabaseConfig) -> "DatabaseClient":
        return cls(config)

    @classmethod
    def from_env(cls) -> "DatabaseClient":
        return cls(database_config_from_env())

    def verify_connection(self) -> None:
        with GraphDatabase.driver(self.db_config.uri, auth=self.db_config.auth) as driver:
            driver.verify_connectivity()

    def upload(
        self, graph: Graph, *, overwrite: bool = False, batch_size: int = 5_000, show_progress: bool = True
    ) -> None:
        from gds_cli.database.upload import upload_graph

        upload_graph(graph, self.db_config, batch_size=batch_size, overwrite=overwrite, show_progress=show_progress)

    def fetch(self, node_labels: list[str] | None = None) -> Graph:
        from gds_cli.database.fetch import download_graph

        return download_graph(self.db_config, node_labels=node_labels)

    def exists(self, graph: Graph | str, *labels: str) -> bool:
        if isinstance(graph, Graph):
            _labels = list(graph.node_dfs.keys())
        else:
            _labels = [graph, *labels]
        return graph_exists(self.db_config, _labels)

    def delete(
        self,
        graph: Graph | str | None = None,
        *labels: str,
        all: bool = False,
        dry_run: bool = False,
    ) -> DeleteStats:
        if all:
            return delete_graph(self.db_config, dry_run=dry_run)
        if isinstance(graph, Graph):
            _labels = list(graph.node_dfs.keys())
        elif graph is None:
            _labels = None
        else:
            _labels = [graph, *labels]
        return delete_graph(self.db_config, _labels, dry_run=dry_run)
