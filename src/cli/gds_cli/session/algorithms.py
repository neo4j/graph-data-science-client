"""Map config algorithm names to graphdatascience client endpoints and run them.

Each config name (case-insensitive) resolves to a gds endpoint attribute; the
endpoint's ``mutate`` / ``write`` method is then called with the property name
and the config's parameters.
"""

from __future__ import annotations

from typing import Any

from gds_cli.session.config import AlgorithmConfig
from graphdatascience.graph.graph_api import Graph
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


def _canonical(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def resolve_endpoint(gds: AuraGraphDataScience, name: str) -> Any:
    key = _canonical(name)
    attr = ALGORITHM_ATTR.get(key)
    if attr is None:
        raise ValueError(f"Unknown algorithm {name!r}. Known: {', '.join(sorted(ALGORITHM_ATTR))}")
    if not hasattr(gds, attr):
        raise ValueError(f"gds client has no endpoint '{attr}' for algorithm {name!r}")
    return getattr(gds, attr)


def run_algorithm(gds: AuraGraphDataScience, graph: Graph, algo: AlgorithmConfig) -> Any:
    """Run one algorithm in its configured mode on the given projected graph."""
    endpoint = resolve_endpoint(gds, algo.name)
    if algo.mode == "mutate":
        return endpoint.mutate(graph, mutate_property=algo.mutate_property, **algo.parameters)
    return endpoint.write(graph, write_property=algo.write_property, **algo.parameters)
