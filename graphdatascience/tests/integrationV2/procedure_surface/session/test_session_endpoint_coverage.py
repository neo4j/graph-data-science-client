import re
from collections import defaultdict

import pytest

from graphdatascience import QueryRunner, ServerVersion
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.session_v2_endpoints import SessionV2Endpoints

MISSING_ALGO_ENDPOINTS = {
    "similarity.knn.filtered",
    "similarity.knn.filtered.estimate",
    "similarity.nodeSimilarity.filtered",
    "similarity.nodeSimilarity.filtered.estimate",
    "similarity.nodeSimilarity",
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
    "closeness": "closeness_centrality",
    "degree": "degree_centrality",
    "eigenvector": "eigenvector_centrality",
    "harmonic": "harmonic_centrality",
    "localClusteringCoefficient": "local_clustering_coefficient",
    # community algos
    "cliquecounting": "clique_counting",
    "k1coloring": "k1_coloring",
    "kcore": "k_core_decomposition",
    "maxkcut": "max_k_cut",
    "modularityOptimization": "modularity_optimization",
    # embedding algos
    "fastrp": "fast_rp",
    "graphSage": "graphsage",
    "hashgnn": "hash_gnn",
}


@pytest.fixture
def gds(arrow_client: AuthenticatedArrowClient, db_query_runner: QueryRunner) -> AuraGraphDataScience:
    return AuraGraphDataScience(
        query_runner=db_query_runner,
        delete_fn=lambda: True,
        gds_version=ServerVersion.from_string("2.7.0"),
        v2_endpoints=SessionV2Endpoints(arrow_client, db_query_runner, show_progress=False),
    )


def to_snake(camel: str) -> str:
    # adjusted version of pydantic.alias_generators.to_snake (without digit handling)

    # Handle the sequence of uppercase letters followed by a lowercase letter
    snake = re.sub(r"([A-Z]+)([A-Z][a-z])", lambda m: f"{m.group(1)}_{m.group(2)}", camel)
    # Insert an underscore between a lowercase letter and an uppercase letter
    snake = re.sub(r"([a-z])([A-Z])", lambda m: f"{m.group(1)}_{m.group(2)}", snake)
    # Replace hyphens with underscores to handle kebab-case
    snake = snake.replace("-", "_")
    return snake.lower()


def check_gds_v2_availability(endpoints: SessionV2Endpoints, algo: str) -> bool:
    """Check if an algorithm is available through gds.v2 interface"""

    algo_parts = algo.split(".")
    algo_parts = [to_snake(part) for part in algo_parts]
    algo_parts = [ENDPOINT_MAPPINGS.get(part, part) for part in algo_parts]

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
    newly_available_endpoints = available_endpoints.intersection(MISSING_ALGO_ENDPOINTS)
    assert not newly_available_endpoints, "Endpoints now available, please remove from MISSING_ALGO_ENDPOINTS"

    # check missing endpoints against known missing algos
    missing_endpoints = missing_endpoints.difference(MISSING_ALGO_ENDPOINTS)
    assert not missing_endpoints, f"Unexpectedly missing endpoints {len(missing_endpoints)}"
