import re
import typing
from collections import OrderedDict
from typing import Any
from unittest import mock

from pandas import DataFrame
from pydantic import BaseModel

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.session.session_v2_endpoints import SessionV2Endpoints
from graphdatascience.tests.integrationV2.procedure_surface.session.gds_api_spec import (
    EndpointWithModesSpec,
    ReturnField,
)

MISSING_ENDPOINTS: set[str] = {
    "all_shortest_paths.stream",
    "bfs.stream",
    "bfs.mutate",
    "bfs.stats",
    "bfs.write",
    "bridges.stream",
    "conductance.stream",
    "dag.longest_path.stream",
    "dag.topological_sort.stream",
    "dfs.mutate",
    "dfs.stream",
    "hits.mutate",
    "hits.stream",
    "hits.stats",
    "hits.write",
    "max_flow.stream",
    "max_flow.write",
    "max_flow.stats",
    "max_flow.mutate",
    "modularity.stats",
    "modularity.stream",
    "random_walk.stats",
    "random_walk.stream",
    "random_walk.mutate",
    "ml.kge.predict.mutate",
    "ml.kge.predict.stream",
    "ml.kge.predict.write",
    "scale_properties.mutate",
    "scale_properties.stats",
    "scale_properties.stream",
    "scale_properties.write",
}


ENDPOINT_MAPPINGS = OrderedDict(
    [
        # centrality algos
        ("closeness.harmonic", "harmonic_centrality"),
        ("closeness", "closeness_centrality"),
        ("betweenness", "betweenness_centrality"),
        ("degree", "degree_centrality"),
        ("eigenvector", "eigenvector_centrality"),
        ("influenceMaximization.celf", "influence_maximization_celf"),
        # community algos
        ("cliquecounting", "clique_counting"),
        ("k1coloring", "k1_coloring"),
        ("kcore", "k_core_decomposition"),
        ("maxkcut", "max_k_cut"),
        # embedding algos
        ("fastrp", "fast_rp"),
        ("graphSage", "graphsage"),
        ("hashgnn", "hash_gnn"),
        # pathfinding algos
        ("astar", "a_star"),
        ("kspanning_tree", "k_spanning_tree"),
        ("prizesteiner_tree", "prize_steiner_tree"),
        ("spanning_tree", "spanning_tree"),
        ("steiner_tree", "steiner_tree"),
    ]
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


def pythonic_endpoint_name(endpoint: str) -> str:
    endpoint = endpoint.removeprefix("gds.")  # endpoints are called on a object called `gds`

    for old, new in ENDPOINT_MAPPINGS.items():
        if old in endpoint:
            endpoint = endpoint.replace(old, new)

    endpoint_parts = endpoint.split(".")
    endpoint_parts = [to_snake(part) for part in endpoint_parts]

    return ".".join(endpoint_parts)


def resolve_callable_object(endpoints: SessionV2Endpoints, endpoint: str) -> Any | None:
    """Check if an algorithm is available through gds.v2 interface"""
    endpoint_parts = endpoint.split(".")

    callable_object = endpoints
    for endpoint_part in endpoint_parts:
        # Get the algorithm endpoint
        if not hasattr(callable_object, endpoint_part):
            return None

        callable_object = getattr(callable_object, endpoint_part)

    if not callable(callable_object):
        raise ValueError(f"Resolved object {callable_object} for endpoint {endpoint} is not callable")

    return callable_object


def verify_return_fields(
    callable_object: Any,
    expected_return_fields: list[ReturnField],
) -> None:
    return_annotation: Any | None = typing.get_type_hints(callable_object).get("return")

    if not return_annotation:
        raise ValueError(f"Callable object {callable_object} has no return annotation. Mypy should complain :/")

    if return_annotation is DataFrame:
        return  # For DataFrames we dont know the columns in advance

    # TODO check type of return fields
    if issubclass(return_annotation, BaseModel):
        actual_return_fields: set[str] = return_annotation.model_fields.keys()
    else:
        raise ValueError(
            f"Return annotation {return_annotation} is not a Pydantic model. Please add support here for testing."
        )

    expected_return_field_keys = {to_snake(field.name) for field in expected_return_fields}

    missing_fields = expected_return_field_keys - actual_return_fields
    extra_fields = actual_return_fields - expected_return_field_keys

    if missing_fields or extra_fields:
        raise ValueError(
            f"Callable object {callable_object} has mismatching return fields. "
            f"Missing fields: {missing_fields}, Extra fields: {extra_fields}"
        )


def test_api_spec_coverage(gds_api_spec: list[EndpointWithModesSpec]) -> None:
    endpoints = SessionV2Endpoints(mock.Mock(speck=AuthenticatedArrowClient), db_client=None, show_progress=False)

    missing_endpoints: set[str] = set()
    available_endpoints: set[str] = set()

    for endpoint_with_modes_spec in gds_api_spec:
        if "alpha." in endpoint_with_modes_spec.name or "beta." in endpoint_with_modes_spec.name:
            # skip alpha/beta endpoints
            continue

        for endpoint_spec in endpoint_with_modes_spec.callable_modes():
            endpoint_name = pythonic_endpoint_name(endpoint_spec.name)
            callable_object = resolve_callable_object(
                endpoints,
                endpoint_name,
            )
            if not callable_object and endpoint_name not in MISSING_ENDPOINTS:
                missing_endpoints.add(endpoint_name)
            elif callable_object:
                # TODO verify against gds-api spec
                # returnFields = callable_object.__
                available_endpoints.add(endpoint_name)
                verify_return_fields(callable_object, expected_return_fields=endpoint_spec.returnFields)

    # Print summary
    print("\nGDS API Spec Coverage Summary:")
    print(f"Total endpoint specs found: {len(available_endpoints) + len(missing_endpoints)}")
    print(f"Available through gds.v2: {len(available_endpoints)}")

    # check if any previously missing algos are now available
    newly_available_endpoints = available_endpoints.intersection(MISSING_ENDPOINTS)
    assert not newly_available_endpoints, "Endpoints now available, please remove from MISSING_ENDPOINTS"

    # check missing endpoints against known missing algos
    assert not missing_endpoints, f"Unexpectedly missing endpoints {len(missing_endpoints)}"
