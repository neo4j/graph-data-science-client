import inspect
import re
import typing
from collections import OrderedDict
from types import MethodType
from typing import Any
from unittest import mock

from pandas import DataFrame
from pydantic import BaseModel

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.session.session_v2_endpoints import SessionV2Endpoints
from tests.integrationV2.procedure_surface.gds_api_spec import (
    EndpointSpec,
    EndpointWithModesSpec,
    Parameter,
    ReturnField,
)

MISSING_ENDPOINTS: set[str] = {
    "dag.topological_sort.stream",
    "dfs.mutate",
    "dfs.stream",
    "hits.mutate",
    "hits.stream",
    "hits.stats",
    "hits.write",
    "ml.kge.predict.mutate",
    "ml.kge.predict.stream",
    "ml.kge.predict.write",
    "random_walk.stats",
    "random_walk.stream",
    "random_walk.mutate",
    "split_relationships.mutate",
    "max_flow.min_cost.stream",
    "max_flow.min_cost.stats",
    "max_flow.min_cost.mutate",
    "max_flow.min_cost.write",
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

IGNORED_PARAMETERS = {
    r".*\.write": ["write_to_result_store"],  # writeToResultStore is not relevant for the user
    r".*shortest_path\.dijkstra.*": ["target_node"],  # marked as deprecated. targetNodes can be used
    r".*sllpa\.mutate.*": [
        "relationship_weight_property",
        "write_property",
        "write_concurrency",
        "write_to_result_store",
    ],
    r".*sllpa\.(stats|stream).*": [
        "write_property",
        "write_to_result_store",
        "mutate_property",
        "relationship_weight_property",
        "write_concurrency",
    ],
    r".*sllpa\.write.*": [
        "mutate_property",
        "relationship_weight_property",
    ],
    r".*scale_properties.*": [
        "relationship_types",  # not relevant for node properties scaling
    ],
}

ADJUSTED_PARAM_DEFAULT_VALUES: dict[str, dict[str, str | None]] = {
    ".*": {
        "concurrency": None,  # default value differs for Aura Graph Analytics compared to plugin (spec is off)
        "job_id": None,  # default value in spec is `random id`
        "write_concurrency": None,  # default value is an internal "value of concurrency"
    },
    ".*(knn|node_similarity).filtered.*": {
        "source_node_filter": None,
        "target_node_filter": None,
    },
    ".*sllpa.mutate": {
        "mutate_property": None,
    },
    ".*sllpa.(write)": {
        "write_property": None,
    },
    ".*triangle_count.*": {
        "max_degree": None,
    },
}


def method_str(s: MethodType) -> str:
    return f"{s.__self__.__class__.__name__}.{s.__name__}"


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


def resolve_callable_object(endpoints: SessionV2Endpoints, endpoint: str) -> MethodType | None:
    """Check if an algorithm is available through gds.v2 interface"""
    endpoint_parts = endpoint.split(".")

    callable_object: SessionV2Endpoints | MethodType = endpoints
    for endpoint_part in endpoint_parts:
        # Get the algorithm endpoint
        if not hasattr(callable_object, endpoint_part):
            return None

        callable_object = getattr(callable_object, endpoint_part)

    if not callable(callable_object):
        raise ValueError(f"Resolved object {callable_object} for endpoint {endpoint} is not callable")

    return callable_object


def verify_return_fields(
    callable_object: MethodType,
    expected_return_fields: list[ReturnField],
) -> None:
    return_annotation: Any | None = typing.get_type_hints(callable_object).get("return")

    if not return_annotation:
        raise ValueError(
            f"Callable object {method_str(callable_object)} has no return annotation. Mypy should complain :/"
        )

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


def verify_configuration_fields(callable_object: MethodType, endpoint_spec: EndpointSpec) -> None:
    expected_configuration: dict[str, Parameter] = {to_snake(param.name): param for param in endpoint_spec.parameters}
    py_endpoint = pythonic_endpoint_name(endpoint_spec.name)
    for endpoint_pattern, ignored_params in IGNORED_PARAMETERS.items():
        if re.match(endpoint_pattern, py_endpoint):
            for param in ignored_params:
                expected_configuration.pop(param)

    method_signature: Any | inspect.Signature = inspect.signature(callable_object)

    if not isinstance(method_signature, inspect.Signature):
        raise ValueError(f"Callable object {callable_object} has no signature. Actual type {type(method_signature)}")

    actual_parameters = method_signature.parameters

    # validate parameter names match
    actual_parameters_keys = set(actual_parameters.keys()) - {
        "G"
    }  # exclude Graph parameter as its not part of the spec
    expected_parameter_keys = expected_configuration.keys()

    missing_params = expected_parameter_keys - actual_parameters_keys
    extra_params = actual_parameters_keys - expected_parameter_keys

    if missing_params or extra_params:
        raise ValueError(
            f"Callable object {pythonic_endpoint_name(endpoint_spec.name)} has mismatching method parameters. "
            f"Missing parameters: {missing_params}, Extra parameters: {extra_params}"
        )

    # validate required parameters doesnt have a default value
    required_params_with_default = [
        name
        for name, param in actual_parameters.items()
        if param.default is not inspect.Parameter.empty and not expected_configuration[name].type.optional
    ]
    if required_params_with_default:
        raise ValueError(
            f"Callable object {pythonic_endpoint_name(endpoint_spec.name)} has required parameters with default "
            f"values: {required_params_with_default}"
        )

    # validate default values match
    default_adjustments: dict[str, str | None] = {}
    for endpoint_pattern, adjustments in ADJUSTED_PARAM_DEFAULT_VALUES.items():
        if re.match(endpoint_pattern, py_endpoint):
            default_adjustments.update(adjustments)

    for name, expected_param in expected_configuration.items():
        expected_default = default_adjustments.get(name, expected_param.defaultValue)
        if expected_default == []:
            expected_default = None  # empty list defaults to None in the python api

        actual_default = actual_parameters[name].default
        if actual_default is inspect.Parameter.empty:
            actual_default = None

        assert actual_default == expected_default, (
            f"Mismatching default value for parameter `{name}` at endpoint `{pythonic_endpoint_name(endpoint_spec.name)}`"
        )

    # validate optional parameters are keyword-only arguments
    # optional_positional_args = [
    #     name
    #     for name, param in actual_parameters.items()
    #     if param.kind is not inspect.Parameter.KEYWORD_ONLY and param.default is not inspect.Parameter.empty
    # ]
    # if optional_positional_args:
    #     raise ValueError(
    #         f"Callable object {pythonic_endpoint_name(endpoint_spec.name)} has optional positional arguments: "
    #         f"{optional_positional_args}. All optional parameters should be keyword-only."
    #     )


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
                verify_configuration_fields(callable_object, endpoint_spec=endpoint_spec)

    # Print summary
    print("\nGDS API Spec Coverage Summary:")
    print(f"Total endpoint specs found: {len(available_endpoints) + len(missing_endpoints)}")
    print(f"Available through gds.v2: {len(available_endpoints)}")

    # check if any previously missing algos are now available
    newly_available_endpoints = available_endpoints.intersection(MISSING_ENDPOINTS)
    assert not newly_available_endpoints, "Endpoints now available, please remove from MISSING_ENDPOINTS"

    # check missing endpoints against known missing algos
    assert not missing_endpoints, f"Unexpectedly missing endpoints {len(missing_endpoints)}"
