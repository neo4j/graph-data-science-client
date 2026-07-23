"""Build a :class:`Graph` from the *construct format* — the same shape used by
``catalog.construct(...)`` / ``graph_from_construct_format(...)`` in the GDS
session notebooks.

Construct format is a pair of *lists of DataFrames* (or, in JSON, lists of lists
of records):

* **nodes** — one DataFrame per node group. Columns: ``nodeId``, ``labels``
  (a single label string), plus any property columns. Property values may be
  scalars or lists (e.g. ``eventFeatures``).
* **relationships** — one DataFrame per relationship group. Columns:
  ``sourceNodeId``, ``targetNodeId``, ``relationshipType``, plus properties.

Endpoint labels for each relationship are resolved from the node groups, so
``nodeId`` must be **globally unique** across all labels (the uploader maps
``nodeId -> elementId`` in a single dict - see :mod:`gds_cli.database.upload`).

Example (matches the FastPath "direct" notebook)::

    import pandas as pd
    from gds_cli.database.construct import graph_from_construct_format

    nodes = [
        pd.DataFrame([{"nodeId": 1, "labels": "Entity", "outputTime": 120}]),
        pd.DataFrame([{"nodeId": 101, "labels": "Event", "eventFeatures": [0.1, 0.0]}]),
    ]
    relationships = [
        pd.DataFrame([{"sourceNodeId": 1, "targetNodeId": 101, "relationshipType": "HAS_EVENT"}]),
    ]
    graph = graph_from_construct_format(nodes, relationships)
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from gds_cli.database.graph import Graph

DEFAULT_RELATIONSHIP_TYPE = "REL"

__all__ = [
    "graph_from_construct_format",
    "graph_to_construct_frames",
    "load_construct_format",
    "graph_from_json",
    "graph_from_file",
]


def _as_label(value: object) -> str:
    """A node group carries a single label string. Reject list/multi-labels."""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)) and len(value) == 1 and isinstance(value[0], str):
        return value[0]
    raise ValueError(
        f"'labels' must be a single label string, got {value!r}. Split multi-label nodes into separate node groups."
    )


def graph_from_construct_format(
    nodes: list[pd.DataFrame],
    relationships: list[pd.DataFrame],
) -> Graph:
    """Convert construct-format ``nodes``/``relationships`` into a :class:`Graph`."""
    node_dfs: dict[str, pd.DataFrame] = {}
    label_of: dict[object, str] = {}
    for df in nodes:
        if "nodeId" not in df.columns or "labels" not in df.columns:
            raise ValueError("each node DataFrame needs 'nodeId' and 'labels' columns")
        for raw_label, group in df.groupby("labels", sort=False):
            label = _as_label(raw_label)
            group = group.drop(columns=["labels"]).reset_index(drop=True)
            for node_id in group["nodeId"]:
                if node_id in label_of:
                    raise ValueError(
                        f"nodeId {node_id!r} is not globally unique (labels {label_of[node_id]!r} and {label!r})"
                    )
                label_of[node_id] = label
            node_dfs[label] = pd.concat([node_dfs[label], group], ignore_index=True) if label in node_dfs else group

    rel_dfs: dict[tuple[str, str, str], pd.DataFrame] = {}
    for df in relationships:
        if "sourceNodeId" not in df.columns or "targetNodeId" not in df.columns:
            raise ValueError("each relationship DataFrame needs 'sourceNodeId' and 'targetNodeId' columns")
        rel_types = (
            df["relationshipType"]
            if "relationshipType" in df.columns
            else pd.Series([DEFAULT_RELATIONSHIP_TYPE] * len(df))
        )
        keys = pd.DataFrame(
            {
                "src": df["sourceNodeId"].map(label_of),
                "rel": list(rel_types),
                "tgt": df["targetNodeId"].map(label_of),
            }
        )
        unresolved = keys["src"].isna() | keys["tgt"].isna()
        if unresolved.any():
            bad = df.loc[unresolved.values, ["sourceNodeId", "targetNodeId"]].to_dict("records")  # type: ignore[union-attr,index]
            raise ValueError(f"relationship endpoints reference unknown nodeIds: {bad}")
        for group_key, idx in keys.groupby(["src", "rel", "tgt"], sort=False).groups.items():
            group = df.loc[idx]
            if "relationshipType" in group.columns:
                group = group.drop(columns=["relationshipType"])
            group = group.reset_index(drop=True)
            key: tuple[str, str, str] = group_key  # type: ignore[assignment]
            rel_dfs[key] = pd.concat([rel_dfs[key], group], ignore_index=True) if key in rel_dfs else group

    return Graph(node_dfs, rel_dfs)


def _is_string_scalar_column(series: pd.Series) -> bool:
    """True if a property column holds scalar strings (which GDS construct rejects).

    List/vector properties (e.g. embeddings) and numeric columns are kept.
    """
    non_null = series.dropna()
    if non_null.empty:
        return False
    return all(isinstance(value, str) for value in non_null)


def graph_to_construct_frames(graph: Graph) -> tuple[list[pd.DataFrame], list[pd.DataFrame], list[str]]:
    """Adapt a :class:`Graph` into the ``(nodes, relationships)`` lists that
    ``gds.graph.construct(...)`` expects, plus the names of any dropped columns.

    Re-attaches the ``labels`` column (from each ``node_dfs`` key) and
    ``relationshipType`` (the middle of each ``rel_dfs`` triple key). GDS
    ``construct`` rejects scalar *string* node properties, so those columns are
    dropped (numeric and list/vector properties are kept); the dropped names are
    returned so the caller can report them. The original ``nodeId``s are preserved
    (do not remap before constructing).
    """
    dropped: set[str] = set()

    def clean(df: pd.DataFrame, id_columns: list[str]) -> pd.DataFrame:
        keep = list(id_columns)
        for column in df.columns:
            if column in id_columns:
                continue
            if _is_string_scalar_column(df[column]):
                dropped.add(column)
            else:
                keep.append(column)
        return df[keep]

    nodes = [clean(df, ["nodeId"]).assign(labels=label) for label, df in graph.node_dfs.items()]
    relationships = [
        clean(df, ["sourceNodeId", "targetNodeId"]).assign(relationshipType=rel_type)
        for (_src, rel_type, _tgt), df in graph.rel_dfs.items()
    ]
    return nodes, relationships, sorted(dropped)


def load_construct_format(path: str | Path) -> tuple[list[pd.DataFrame], list[pd.DataFrame]]:
    """Load construct-format ``(nodes, relationships)`` from a JSON file.

    JSON shape::

        {"nodes": [[{...}, ...], ...], "relationships": [[{...}, ...], ...]}

    where each inner list is one node/relationship group (becomes one DataFrame).
    """
    data = json.loads(Path(path).expanduser().read_text())
    nodes = [pd.DataFrame(group) for group in data.get("nodes", [])]
    relationships = [pd.DataFrame(group) for group in data.get("relationships", [])]
    return nodes, relationships


def graph_from_json(path: str | Path) -> Graph:
    """Load a JSON graph (construct format) and build a :class:`Graph`."""
    return graph_from_construct_format(*load_construct_format(path))


def graph_from_file(path: str | Path) -> Graph:
    """Load a graph file (JSON or YAML), dispatching on its top-level ``kind``.

    * ``kind: construct`` (or omitted) -> explicit construct-format graph
      (``nodes``/``relationships`` as lists of record groups);
    * ``kind: random`` -> generated from a random-graph spec (see
      :mod:`gds_cli.database.spec`).
    """
    # yaml.safe_load also parses JSON, so one loader handles both formats.
    data = yaml.safe_load(Path(path).expanduser().read_text())
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected a mapping at the top level.")

    kind = data.get("kind", "construct")
    if kind == "construct":
        nodes = [pd.DataFrame(group) for group in data.get("nodes", [])]
        relationships = [pd.DataFrame(group) for group in data.get("relationships", [])]
        return graph_from_construct_format(nodes, relationships)
    if kind == "random":
        from gds_cli.database.spec import graph_from_spec

        return graph_from_spec(data)
    raise ValueError(f"Unknown kind {kind!r} in {path}; expected 'construct' or 'random'.")
