from collections import defaultdict

import pytest
from pydantic.alias_generators import to_snake

from graphdatascience import QueryRunner, ServerVersion
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.session_v2_endpoints import SessionV2Endpoints

MISSING_ALGO_ENDPOINTS = {
    "embeddings.graphSage.train.estimate",  # TODO fix this by moving behind shared interface
    "embeddings.graphSage.estimate",
    "similarity.knn.filtered",
    "similarity.knn.filtered.estimate",
    "similarity.nodeSimilarity.filtered",
    "similarity.nodeSimilarity.filtered.estimate",
    "similarity.nodeSimilarity",
    "similarity.knn",
    "similarity.knn.estimate",
    "similarity.nodeSimilarity.estimate",
    "pathfinding.sourceTarget.dijkstra.estimate",
    "pathfinding.sourceTarget.aStar",
    "pathfinding.prizeSteinerTree.estimate",
    "pathfinding.sourceTarget.yens",
    "pathfinding.singleSource.deltaStepping.estimate",
    "pathfinding.singleSource.deltaStepping",
    "pathfinding.steinerTree",
    "pathfinding.singleSource.dijkstra",
    "pathfinding.singleSource.bellmanFord",
    "pathfinding.steinerTree.estimate",
    "pathfinding.singleSource.bellmanFord.estimate",
    "pathfinding.singleSource.dijkstra.estimate",
    "pathfinding.prizeSteinerTree",
    "pathfinding.spanningTree.estimate",
    "pathfinding.sourceTarget.dijkstra",
    "pathfinding.kSpanningTree",
    "pathfinding.spanningTree",
    "pathfinding.sourceTarget.aStar.estimate",
    "pathfinding.sourceTarget.yens.estimate",
}

ENDPOINT_MAPPINGS = {
    # centrality algos
    "betweenness": "betweenness_centrality",
    "celf": "influence_maximization_celf",
    "celf.estimate": "influence_maximization_celf.estimate",
    "closeness": "closeness_centrality",
    "closeness.estimate": "closeness_centrality.estimate",
    "degree": "degree_centrality",
    "degree.estimate": "degree_centrality.estimate",
    "eigenvector": "eigenvector_centrality",
    "eigenvector.estimate": "eigenvector_centrality.estimate",
    "harmonic": "harmonic_centrality",
    "harmonic.estimate": "harmonic_centrality.estimate",
    "localClusteringCoefficient": "local_clustering_coefficient",
    "localClusteringCoefficient.estimate": "local_clustering_coefficient.estimate",
    # community algos
    "k1coloring": "k1_coloring",
    "k1coloring.estimate": "k1_coloring.estimate",
    "kcore": "k_core_decomposition",
    "kcore.estimate": "k_core_decomposition.estimate",
    "maxkcut": "max_k_cut",
    "maxkcut.estimate": "max_k_cut.estimate",
    "modularityOptimization": "modularity_optimization",
    "modularityOptimization.estimate": "modularity_optimization.estimate",
    "sllpa": "sllpa",
    "sllpa.estimate": "sllpa.estimate",
    "triangleCount": "triangle_count",
    "triangleCount.estimate": "triangle_count.estimate",
    # embedding algos
    "fastrp": "fast_rp",
    "fastrp.estimate": "fast_rp.estimate",
    "graphSage": "graphsage_predict",
    "graphSage.train": "graphsage_train",
    "hashgnn": "hash_gnn",
    "hashgnn.estimate": "hash_gnn.estimate",
}


@pytest.fixture
def gds(arrow_client: AuthenticatedArrowClient, db_query_runner: QueryRunner) -> AuraGraphDataScience:
    return AuraGraphDataScience(
        query_runner=db_query_runner,
        delete_fn=lambda: True,
        gds_version=ServerVersion.from_string("2.7.0"),
        v2_endpoints=SessionV2Endpoints(arrow_client, db_query_runner, show_progress=False),
    )


def check_gds_v2_availability(endpoints: SessionV2Endpoints, algo: str) -> bool:
    """Check if an algorithm is available through gds.v2 interface"""

    algo = ENDPOINT_MAPPINGS.get(algo, algo)

    algo_parts = algo.split(".")
    algo_parts = [to_snake(part) for part in algo_parts]

    callable_object = endpoints
    for algo_part in algo_parts:
        # Get the algorithm endpoint
        if not hasattr(callable_object, algo_part):
            return False

        callable_object = getattr(callable_object, algo_part)

    # if we can resolve an object for all parts of the algo endpoint we assume it is available
    return True


@pytest.mark.db_integration
def test_algo_coverage(gds: AuraGraphDataScience) -> None:
    """Test that all available Arrow actions are accessible through gds.v2"""
    arrow_client = gds.v2._arrow_client

    # Get all available Arrow actions
    available_v2_actions = [
        action.type.removeprefix("v2/") for action in arrow_client.list_actions() if action.type.startswith("v2/")
    ]

    algo_prefixes = ["pathfinding", "centrality", "community", "similarity", "embedding"]

    # Filter to only v2 algorithm actions (exclude graph, model, catalog operations)
    algorithm_actions: set[str] = {
        action for action in available_v2_actions if any(action.startswith(prefix) for prefix in algo_prefixes)
    }

    missing_endpoints: set[str] = set()
    available_endpoints: set[str] = set()

    algos_per_category = defaultdict(list)
    for action in algorithm_actions:
        category, algo_parts = action.split(".", maxsplit=1)
        algos_per_category[category].append(algo_parts)

    for category, algos in algos_per_category.items():
        for algo in algos:
            is_available = check_gds_v2_availability(
                gds.v2,
                algo,
            )
            action = f"{category}.{algo}"
            if is_available:
                available_endpoints.add(action)
            else:
                missing_endpoints.add(action)

    # Print summary
    print("\nArrow Action Coverage Summary:")
    print(f"Total algorithm actions found: {len(algorithm_actions)}")
    print(f"Available through gds.v2: {len(available_endpoints)}")

    # check if any previously missing algos are now available
    assert not available_endpoints.intersection(MISSING_ALGO_ENDPOINTS), (
        "Endpoints now available, please remove from MISSING_ALGO_ENDPOINTS"
    )

    # check missing endpoints against known missing algos
    assert missing_endpoints.difference(MISSING_ALGO_ENDPOINTS), "Unexpectedly missing endpoints"
