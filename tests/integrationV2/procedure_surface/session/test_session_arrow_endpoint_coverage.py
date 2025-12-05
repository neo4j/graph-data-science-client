import re
from collections import defaultdict

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.session.session_v2_endpoints import SessionV2Endpoints

# mapping for arrow endpoint name parts -> endpoint callable from SessionV2Endpoints
ENDPOINT_MAPPINGS = {
    # centrality algos
    "betweenness": "betweenness_centrality",
    "celf": "influence_maximization_celf",
    "closeness": "closeness_centrality",
    "degree": "degree_centrality",
    "eigenvector": "eigenvector_centrality",
    "harmonic": "harmonic_centrality",
    # community algos
    "cliquecounting": "clique_counting",
    "k1coloring": "k1_coloring",
    "kcore": "k_core_decomposition",
    "maxkcut": "max_k_cut",
    # embedding algos
    "fastrp": "fast_rp",
    "hashgnn": "hash_gnn",
    # pathfinding algos
    "sourceTarget": "shortest_path",
    "singleSource.bellmanFord": "bellman_ford",
    "singleSource": "all_shortest_paths",
    "deltaStepping": "delta",
    "kspanning_tree": "k_spanning_tree",
    "prizesteiner_tree": "prize_steiner_tree",
    "longestPath": "dag.longest_path",
}


@pytest.fixture
def endpoints(arrow_client: AuthenticatedArrowClient) -> SessionV2Endpoints:
    return SessionV2Endpoints(arrow_client, db_client=None, show_progress=False)


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

    for old, new in ENDPOINT_MAPPINGS.items():
        if old in algo:
            algo = algo.replace(old, new)

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


def test_algo_coverage(endpoints: SessionV2Endpoints) -> None:
    arrow_client = endpoints._arrow_client

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
                endpoints,
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

    # check missing endpoints against known missing algos
    assert not missing_endpoints, f"Unexpectedly missing endpoints {len(missing_endpoints)}"
