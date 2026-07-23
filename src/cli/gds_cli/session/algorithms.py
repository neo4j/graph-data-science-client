"""Map config algorithm names to graphdatascience client endpoints and start them.

Each ``compute`` entry names an algorithm (case-insensitive), which resolves to
a gds endpoint attribute. The endpoint's ``compute`` method is called with the
config parameters (camelCase in the config, collapsed to snake_case here since
the client's Python methods take snake_case keyword args) and returns a
:class:`JobHandle`; the caller then decides whether to ``mutate`` or ``write``
the result via that handle - see :mod:`gds_cli.session.steps`.
"""

from __future__ import annotations

import re
from typing import Any, cast

from gds_cli.session.config import ComputeSpec
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience

# Canonical (lowercased, non-alphanumeric stripped) name -> gds endpoint attribute.
ALGORITHM_ATTR: dict[str, str] = {
    "pagerank": "page_rank",
    "articlerank": "article_rank",
    "betweenness": "betweenness_centrality",
    "betweennesscentrality": "betweenness_centrality",
    "degree": "degree_centrality",
    "degreecentrality": "degree_centrality",
    "closeness": "closeness_centrality",
    "eigenvector": "eigenvector_centrality",
    "louvain": "louvain",
    "wcc": "wcc",
    "labelpropagation": "label_propagation",
    "trianglecount": "triangle_count",
    "localclusteringcoefficient": "local_clustering_coefficient",
    "nodesimilarity": "node_similarity",
    "fastrp": "fast_rp",
    "fastpath": "fast_path",
    "node2vec": "node2vec",
    "graphsage": "graph_sage",
}

# Canonical algorithm name -> config keys whose value(s) name node properties the
# algorithm reads as INPUT. If such a value equals the `resultProperty` of an
# earlier compute in the same job, that earlier result is auto-materialized
# (mutated) into the in-session graph so this algorithm can read it. Derived from
# the GDS API spec (graph-analytics/tools/gds-api-spec/gds-api-spec.json);
# `relationshipWeightProperty` is intentionally excluded - it names a relationship
# property, and a compute only ever produces node properties. Algorithms with no
# node-property inputs (pageRank, articleRank, betweenness, degree, closeness,
# eigenvector, node2vec, nodeSimilarity, triangleCount) are simply absent.
ALGORITHM_INPUT_PROPERTY_KEYS: dict[str, tuple[str, ...]] = {
    "fastrp": ("featureProperties",),
    "graphsage": ("featureProperties",),
    "louvain": ("seedProperty",),
    "wcc": ("seedProperty",),
    "labelpropagation": ("seedProperty", "nodeWeightProperty"),
    "localclusteringcoefficient": ("triangleCountProperty",),
    "fastpath": ("eventFeatures", "categoricalEventProperties", "timeNodeProperty", "outputTimeProperty"),
}

_CAMEL_BOUNDARY = re.compile(r"(?<!^)(?=[A-Z])")


def _canonical(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def input_property_references(algorithm: str, config: dict[str, Any]) -> set[str]:
    """Node-property names an algorithm reads as input, per its config.

    Looks up the algorithm's input-property config keys and collects the referenced
    property names (a string value, or the string elements of a list value).
    """
    refs: set[str] = set()
    for key in ALGORITHM_INPUT_PROPERTY_KEYS.get(_canonical(algorithm), ()):
        value = config.get(key)
        if isinstance(value, str):
            refs.add(value)
        elif isinstance(value, list):
            refs.update(item for item in value if isinstance(item, str))
    return refs


def to_snake_case(name: str) -> str:
    """Convert a camelCase config key to the snake_case kwarg the client expects."""
    return _CAMEL_BOUNDARY.sub("_", name).lower()


def to_snake_params(params: dict[str, Any]) -> dict[str, Any]:
    """Collapse a config's camelCase parameter keys to snake_case (top-level only).

    Values are passed through untouched - nested dicts (e.g. a scaler config) are
    consumed as-is by the client method.
    """
    return {to_snake_case(key): value for key, value in params.items()}


def resolve_endpoint(gds: AuraGraphDataScience, name: str) -> Any:
    key = _canonical(name)
    attr = ALGORITHM_ATTR.get(key)
    if attr is None:
        raise ValueError(f"Unknown algorithm {name!r}. Known: {', '.join(sorted(ALGORITHM_ATTR))}")
    if not hasattr(gds, attr):
        raise ValueError(f"gds client has no endpoint '{attr}' for algorithm {name!r}")
    return getattr(gds, attr)


def compute_algorithm(gds: AuraGraphDataScience, graph: Graph, spec: ComputeSpec) -> JobHandle:
    """Start one algorithm on the given graph and return its :class:`JobHandle`."""
    endpoint = resolve_endpoint(gds, spec.algorithm)
    if not hasattr(endpoint, "compute"):
        raise ValueError(
            f"algorithm '{spec.algorithm}' endpoint has no `compute` method; "
            "it cannot be run through the compute -> mutate/write pipeline."
        )
    return cast(JobHandle, endpoint.compute(graph, **to_snake_params(spec.parameters)))
