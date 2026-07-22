"""Rendering of a downloaded :class:`Graph` for the CLI.

Three formats:

* ``summary`` (default) — compact counts + property-name listing;
* ``table`` — a nicely formatted ``rich`` table per node label and relationship
  type, showing every property (mirrors the ``aura-telemetry`` CLI style);
* ``json`` — the construct format that ``upload --file`` consumes, so a
  downloaded graph round-trips straight back into another database.

Rows are sorted deterministically: nodes by ``nodeId``, relationships by
``sourceNodeId`` then ``targetNodeId``.

Heavy imports (``rich``) are deferred so importing this module (e.g. just for the
``OutputFormat`` enum) stays cheap and ``--help`` stays fast.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, Literal, Optional

from gds_cli.common import CONSOLE_WIDTH

if TYPE_CHECKING:
    import pandas as pd

    from gds_cli.database.graph import Graph


class OutputFormat(str, Enum):
    table = "table"
    json = "json"


def _sorted_nodes(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values("nodeId").reset_index(drop=True) if "nodeId" in df.columns else df


def _sorted_rels(df: pd.DataFrame) -> pd.DataFrame:
    keys = [k for k in ("sourceNodeId", "targetNodeId") if k in df.columns]
    return df.sort_values(keys).reset_index(drop=True) if keys else df


def print_summary(graph: Graph) -> None:
    """Compact listing: one line per node label / relationship type with counts."""
    lines = ["Nodes:"]
    for label, df in graph.node_dfs.items():
        props = [c for c in df.columns if c not in ("nodeId", "labels")]
        lines.append(f"  (:{label})  count={len(df)}  properties={props}")
    lines.append("Relationships:")
    for (src, rel, tgt), df in graph.rel_dfs.items():
        props = [c for c in df.columns if c not in ("sourceNodeId", "targetNodeId", "relationshipType")]
        lines.append(f"  (:{src})-[:{rel}]->(:{tgt})  count={len(df)}  properties={props}")
    print("\n".join(lines))


def print_tables(graph: Graph, limit: Optional[int] = None) -> None:
    """One rich table per node label and relationship type, showing all properties."""
    from rich.console import Console

    console = Console(width=CONSOLE_WIDTH)

    console.rule("Nodes")
    if not graph.node_dfs:
        console.print("[dim](none)[/dim]")
    for label, df in graph.node_dfs.items():
        cols = ["nodeId", *(c for c in df.columns if c not in ("nodeId", "labels"))]
        console.print(_table(f"(:{label})", _sorted_nodes(df)[cols], limit))

    console.rule("Relationships")
    if not graph.rel_dfs:
        console.print("[dim](none)[/dim]")
    for (src, rel, tgt), df in graph.rel_dfs.items():
        lead = ["sourceNodeId", "targetNodeId"]
        cols = [*lead, *(c for c in df.columns if c not in (*lead, "relationshipType"))]
        console.print(_table(f"(:{src})-[:{rel}]->(:{tgt})", _sorted_rels(df)[cols], limit))

    if limit is not None:
        all_dfs = [*graph.node_dfs.values(), *graph.rel_dfs.values()]
        if any(len(df) > limit for df in all_dfs):
            console.print(
                f"[yellow]Output truncated to {limit} rows per table. Use --limit all to show everything.[/yellow]"
            )


def print_json(graph: Graph) -> None:
    """Emit the graph as construct-format JSON (the shape ``upload --file`` accepts)."""
    import json

    def _node_records(label: str, df: pd.DataFrame) -> list[Any]:
        df = _sorted_nodes(df).copy()
        if "labels" not in df.columns:
            df["labels"] = label
        # via to_json so numpy scalar types become native JSON numbers
        records: list[Any] = json.loads(df.to_json(orient="records"))
        return records

    def _rel_records(rel_type: str, df: pd.DataFrame) -> list[Any]:
        df = _sorted_rels(df).copy()
        if "relationshipType" not in df.columns:
            df["relationshipType"] = rel_type
        records: list[Any] = json.loads(df.to_json(orient="records"))
        return records

    payload = {
        "nodes": [_node_records(label, df) for label, df in graph.node_dfs.items()],
        "relationships": [_rel_records(key[1], df) for key, df in graph.rel_dfs.items()],
    }
    print(json.dumps(payload, indent=2))


def _table(title: str, df: pd.DataFrame, limit: Optional[int]) -> Any:
    import pandas as pd
    from rich import box
    from rich.table import Table

    total = len(df)
    shown = df if not limit or limit >= total else df.head(limit)
    caption = f"{total} rows" if len(shown) == total else f"showing {len(shown)} of {total}"

    table = Table(title=title, caption=caption, box=box.ROUNDED, show_header=True, title_justify="left")
    for col in df.columns:
        justify: Literal["right", "left"] = "right" if pd.api.types.is_numeric_dtype(df[col]) else "left"
        table.add_column(str(col), justify=justify)
    for _, row in shown.iterrows():
        table.add_row(*[_fmt(v) for v in row])
    return table


def _fmt(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value)
