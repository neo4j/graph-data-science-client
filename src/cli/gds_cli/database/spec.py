"""Build a random :class:`Graph` from a serialized spec (``kind: random``).

A spec is a declarative form of :class:`RandomGraphConfig`: node groups with
counts and property generators, plus relationship groups with counts, an edge
generator, and property generators. This is what ``gds database upload
--file`` reads for a ``kind: random`` graph file (see
``examples/cli/graphs/*.yaml`` for worked examples).

Spec shape (YAML or JSON)::

    kind: random
    nodes:
      Person:
        count: 100
        properties:
          age:   {type: uniform_int, low: 18, high: 80}
          score: {type: gaussian, mean: 0.0, std: 1.0}
      Company:
        count: 20
    relationships:
      - source: Person
        type: WORKS_AT
        target: Company
        count: 150
        generator: {type: powerlaw, alpha: 0.7}   # default: uniform
        properties:
          since: {type: uniform_int, low: 2000, high: 2024}
    node_id_mapping: globally_unique   # optional; defaults to globally_unique
                                       # when there is more than one node label

Property generator ``type``: gaussian | uniform_int | category | timestamp.
The numeric generators (``gaussian``, ``uniform_int``) accept an optional
``dim`` to emit a *vector* property (a list of that many values) per node/edge
instead of a scalar - e.g. ``{type: gaussian, dim: 4}`` for a 4-d feature
vector. Relationship generator ``type``: uniform | powerlaw (with ``alpha``).
Both edge generators accept ``allow_duplicates: true``.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from gds_cli.database.graph import (
    GaussianGenerator,
    Graph,
    NodeIdMapping,
    PowerLawRelationshipGenerator,
    RandomGenerator,
    RandomGraphConfig,
    RandomNodesConfig,
    RandomRelsConfig,
    RelationshipGenerator,
    UniformIntegerGenerator,
    UniformRelationshipGenerator,
    UniformStringCategoryGenerator,
    UniformTimestampGenerator,
    create_graph,
)

__all__ = ["random_graph_config_from_spec", "graph_from_spec"]

_NODE_ID_MAPPINGS = {
    "globally_unique": NodeIdMapping.GLOBALLY_UNIQUE,
    "label_unique": NodeIdMapping.LABEL_UNIQUE,
}


def _property_generator(cfg: dict[str, Any]) -> RandomGenerator:
    t = cfg.get("type")
    if t == "gaussian":
        return GaussianGenerator(mean=cfg.get("mean", 0.0), std=cfg.get("std", 1.0), dim=cfg.get("dim", 0))
    if t == "uniform_int":
        return UniformIntegerGenerator(low=cfg["low"], high=cfg["high"], dim=cfg.get("dim", 0))
    if t == "category":
        return UniformStringCategoryGenerator(categories=cfg["values"])
    if t == "timestamp":
        return UniformTimestampGenerator(start=pd.Timestamp(cfg["start"]), end=pd.Timestamp(cfg["end"]))
    raise ValueError(f"Unknown property generator type {t!r}. Known: gaussian, uniform_int, category, timestamp.")


def _relationship_generator(cfg: dict[str, Any] | None) -> RelationshipGenerator:
    if cfg is None:
        return UniformRelationshipGenerator()
    t = cfg.get("type", "uniform")
    allow_duplicates = bool(cfg.get("allow_duplicates", False))
    if t == "uniform":
        return UniformRelationshipGenerator(allow_duplicates=allow_duplicates)
    if t == "powerlaw":
        return PowerLawRelationshipGenerator(alpha=cfg.get("alpha", 0.9), allow_duplicates=allow_duplicates)
    raise ValueError(f"Unknown relationship generator type {t!r}. Known: uniform, powerlaw.")


def _properties(cfg: dict[str, Any] | None) -> dict[str, RandomGenerator]:
    return {name: _property_generator(g) for name, g in (cfg or {}).items()}


def random_graph_config_from_spec(spec: dict[str, Any]) -> RandomGraphConfig:
    """Translate a parsed spec dict into a :class:`RandomGraphConfig`."""
    nodes = spec.get("nodes")
    if not isinstance(nodes, dict) or not nodes:
        raise ValueError("random graph spec needs a non-empty 'nodes' mapping of label -> config.")

    node_config: dict[str, RandomNodesConfig] = {}
    for label, cfg in nodes.items():
        if "count" not in cfg:
            raise ValueError(f"node group {label!r} is missing 'count'.")
        node_config[label] = RandomNodesConfig(
            node_count=cfg["count"],
            properties=_properties(cfg.get("properties")),
            labels=cfg.get("labels"),
        )

    rel_config: dict[tuple[str, str, str], RandomRelsConfig] = {}
    for rel in spec.get("relationships", []):
        for key in ("source", "type", "target", "count"):
            if key not in rel:
                raise ValueError(f"relationship is missing '{key}': {rel!r}")
        triplet = (rel["source"], rel["type"], rel["target"])
        rel_config[triplet] = RandomRelsConfig(
            rel_count=rel["count"],
            rels=_relationship_generator(rel.get("generator")),
            properties=_properties(rel.get("properties")),
        )

    mapping_name = spec.get("node_id_mapping")
    if mapping_name is None:
        # Multi-label graphs must be globally unique (uploader maps nodeId -> elementId once).
        mapping = NodeIdMapping.GLOBALLY_UNIQUE if len(node_config) > 1 else None
    elif mapping_name in _NODE_ID_MAPPINGS:
        mapping = _NODE_ID_MAPPINGS[mapping_name]
    else:
        raise ValueError(f"Unknown node_id_mapping {mapping_name!r}. Known: {', '.join(_NODE_ID_MAPPINGS)}.")

    return RandomGraphConfig(node_config=node_config, rel_config=rel_config, nodeIdMapping=mapping)


def graph_from_spec(spec: dict[str, Any]) -> Graph:
    """Generate a :class:`Graph` from a parsed random-graph spec."""
    return create_graph(random_graph_config_from_spec(spec))
