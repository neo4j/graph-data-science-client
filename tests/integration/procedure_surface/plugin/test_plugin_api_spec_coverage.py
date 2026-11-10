from __future__ import annotations

import dataclasses

from graphdatascience import GraphDataScience
from tests.integration.procedure_surface.api_spec import ApiSpec, EndpointSpec
from tests.integration.procedure_surface.api_spec_coverage_test_helper import (
    UNMAPPED_ENDPOINTS,
    assert_api_spec_coverage,
    pythonic_endpoint_name,
)
from tests.integration.procedure_surface.endpoint_spec_coverage_test_helper import (
    assert_endpoint_spec_coverage,
)
from tests.integration.procedure_surface.gds_api_spec import EndpointWithModesSpec

# endpoints that are only available in AGA (GDS Sessions), not in the self-managed GDS plugin
PLUGIN_UNMAPPED_ENDPOINTS = UNMAPPED_ENDPOINTS | {
    "fast_path.mutate",  # python-runtime backed, AGA only
    "fast_path.stream",  # python-runtime backed, AGA only
    "fast_path.write",  # python-runtime backed, AGA only
    "graph_sage.supervised.train",  # python-runtime backed, AGA only
    "graph_sage.supervised.stream",  # python-runtime backed, AGA only
    "graph_sage.supervised.write",  # python-runtime backed, AGA only
    "graph_sage.supervised.mutate",  # python-runtime backed, AGA only
    "graph_sage.unsupervised.train",  # python-runtime backed, AGA only
    "graph_sage.unsupervised.stream",  # python-runtime backed, AGA only
    "graph_sage.unsupervised.write",  # python-runtime backed, AGA only
    "graph_sage.unsupervised.mutate",  # python-runtime backed, AGA only
}

# Graph catalog endpoints the plugin surface does not map onto their own path. Drop
# entries one at a time as the client grows them.
PLUGIN_UNMAPPED_GRAPH_CATALOG_ENDPOINTS = {
    "graph.relationship.write",  # served by the client's relationships endpoints
}

# the compute mode is session-surface only in the client; the plugin API spec lists it
# for every algorithm. This shrinks as the client maps compute onto the plugin surface.
PLUGIN_COMPUTE_UNMAPPED_ENDPOINTS = {
    "all_shortest_paths.delta.compute",
    "all_shortest_paths.dijkstra.compute",
    "article_rank.compute",
    "articulation_points.compute",
    "bellman_ford.compute",
    "betweenness.compute",
    "bfs.compute",
    "closeness.compute",
    "closeness.harmonic.compute",
    "degree.compute",
    "dfs.compute",
    "eigenvector.compute",
    "fast_rp.compute",
    "graph_sage.compute",
    "hash_gnn.compute",
    "influence_maximization_celf.compute",
    "k1_coloring.compute",
    "k_spanning_tree.compute",
    "kcore.compute",
    "kmeans.compute",
    "knn.compute",
    "knn.filtered.compute",
    "label_propagation.compute",
    "leiden.compute",
    "local_clustering_coefficient.compute",
    "louvain.compute",
    "max_flow.compute",
    "max_flow.min_cost.compute",
    "max_k_cut.compute",
    "modularity_optimization.compute",
    "node2vec.compute",
    "node_similarity.compute",
    "node_similarity.filtered.compute",
    "page_rank.compute",
    "prize_steiner_tree.compute",
    "scc.compute",
    "shortest_path.a_star.compute",
    "shortest_path.dijkstra.compute",
    "shortest_path.yens.compute",
    "sllpa.compute",
    "spanning_tree.compute",
    "steiner_tree.compute",
    "triangle_count.compute",
    "wcc.compute",
}


def _plugin_endpoints(spec: ApiSpec) -> list[EndpointSpec]:
    # the plugin spec publishes procedure names (`gds.articleRank`); the client path drops the procedure prefix
    return [
        dataclasses.replace(endpoint, endpoint_name=endpoint.endpoint_name.removeprefix("gds."))
        for endpoint in spec.endpoints()
    ]


def test_plugin_api_spec_coverage(gds_api_spec: list[EndpointWithModesSpec], plugin_api_spec: ApiSpec) -> None:
    plugin_endpoints = _plugin_endpoints(plugin_api_spec)
    plugin_spec_endpoints = {pythonic_endpoint_name(endpoint.name) for endpoint in plugin_endpoints}

    # Algorithms the plugin API spec describes are validated against it. Everything else
    # the plugin surface exposes (graph, model, pipeline and utility procedures) is still
    # validated against the legacy plugin API spec.
    assert_api_spec_coverage(
        GraphDataScience,
        gds_api_spec,
        unmapped_endpoints=PLUGIN_UNMAPPED_ENDPOINTS,
        exclude_endpoints=plugin_spec_endpoints,
    )
    assert_endpoint_spec_coverage(
        GraphDataScience,
        plugin_endpoints,
        unmapped_endpoints=PLUGIN_COMPUTE_UNMAPPED_ENDPOINTS | PLUGIN_UNMAPPED_GRAPH_CATALOG_ENDPOINTS,
    )
