import re
from collections import defaultdict

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.pipeline.node_regression_pipeline_arrow_endpoints import (
    NodeRegressionPipelineArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.node_regression_predict_arrow_endpoints import (
    NodeRegressionPredictArrowEndpoints,
)
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
    "maxFlow.minCost": "max_flow.min_cost",
}

PIPELINE_NODE_REGRESSION_MAPPINGS = {
    "nodeRegression": ("pipeline.node_regression",),
    "nodeRegression.autoTuning.configure": ("pipeline.node_regression.configure_auto_tuning",),
    "nodeRegression.features.select": ("pipeline.node_regression.select_features",),
    "nodeRegression.modelCandidate.add": (
        "pipeline.node_regression.add_linear_regression",
        "pipeline.node_regression.add_random_forest",
    ),
    "nodeRegression.nodeProperty.add": ("pipeline.node_regression.add_node_property",),
    "nodeRegression.predict": ("pipeline.node_regression.predict",),
    "nodeRegression.split.configure": ("pipeline.node_regression.configure_split",),
    "nodeRegression.train": ("pipeline.node_regression.train",),
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


def _has_dotted_path(root: object, dotted_path: str) -> bool:
    current = root
    for part in dotted_path.split("."):
        if not hasattr(current, part):
            return False
        current = getattr(current, part)
    return True


def check_gds_v2_availability(endpoints: SessionV2Endpoints, algo: str) -> bool:
    """Check if an algorithm is available through gds.v2 interface"""
    if algo in PIPELINE_NODE_REGRESSION_MAPPINGS:
        return all(_has_dotted_path(endpoints, path) for path in PIPELINE_NODE_REGRESSION_MAPPINGS[algo])

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


def test_check_gds_v2_availability_for_node_regression_actions(endpoints: SessionV2Endpoints) -> None:
    assert check_gds_v2_availability(endpoints, "nodeRegression")
    assert check_gds_v2_availability(endpoints, "nodeRegression.autoTuning.configure")
    assert check_gds_v2_availability(endpoints, "nodeRegression.features.select")
    assert check_gds_v2_availability(endpoints, "nodeRegression.nodeProperty.add")
    assert check_gds_v2_availability(endpoints, "nodeRegression.predict")
    assert check_gds_v2_availability(endpoints, "nodeRegression.split.configure")
    assert check_gds_v2_availability(endpoints, "nodeRegression.train")


def test_check_gds_v2_availability_requires_both_model_candidate_methods(endpoints: SessionV2Endpoints) -> None:
    pipeline = endpoints.pipeline.node_regression

    assert hasattr(pipeline, "add_linear_regression")
    assert hasattr(pipeline, "add_random_forest")
    assert check_gds_v2_availability(endpoints, "nodeRegression.modelCandidate.add")


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


def test_pipeline_coverage(endpoints: SessionV2Endpoints) -> None:
    arrow_client = endpoints._arrow_client

    # Get all available Arrow actions
    available_v2_actions = [
        action.type.removeprefix("v2/") for action in arrow_client.list_actions() if action.type.startswith("v2/")
    ]

    # Filter to only v2 algorithm actions (exclude graph, model, catalog operations)
    pipeline_actions: set[str] = {action for action in available_v2_actions if action.startswith("pipeline")}

    missing_endpoints: set[str] = set()
    available_endpoints: set[str] = set()

    algos_per_category = defaultdict(list)
    for action in pipeline_actions:
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
    print(f"Total Pipeline actions found: {len(pipeline_actions)}")
    print(f"Available through gds.v2: {len(available_endpoints)}")

    missing_endpoints = {e for e in missing_endpoints if "pipeline.nodeRegression" in e}

    # check missing endpoints against known missing algos
    assert not missing_endpoints, f"Unexpectedly missing endpoints {len(missing_endpoints)}"


def test_session_pipeline_node_regression_predict_resolves(endpoints: SessionV2Endpoints) -> None:
    pipeline_endpoints = endpoints.pipeline.node_regression

    assert isinstance(pipeline_endpoints, NodeRegressionPipelineArrowEndpoints)
    assert isinstance(pipeline_endpoints.predict, NodeRegressionPredictArrowEndpoints)
