import inspect
import re
import typing
from collections import OrderedDict
from types import MethodType, UnionType
from typing import Any, NamedTuple

from pandas import DataFrame
from pydantic import BaseModel

from tests.integrationV2.procedure_surface.gds_api_spec import (
    EndpointSpec,
    EndpointWithModesSpec,
    Parameter,
    ReturnField,
    SourceKind,
)

# endpoints not mapped yet in the v2 endpoints of the python client
UNMAPPED_ENDPOINTS: set[str] = {
    "dag.topological_sort.stream",
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
    "memory.summary",
    "memory.list",
    "list",  # listing only available endpoints
    "user_log",
}

BASE_ENDPOINT_MAPPINGS = OrderedDict(
    [
        ("closeness.harmonic", "harmonic_centrality"),
        ("closeness", "closeness_centrality"),
        ("betweenness", "betweenness_centrality"),
        ("degree", "degree_centrality"),
        ("eigenvector", "eigenvector_centrality"),
        ("influenceMaximization.celf", "influence_maximization_celf"),
        ("cliquecounting", "clique_counting"),
        ("k1coloring", "k1_coloring"),
        ("kcore", "k_core_decomposition"),
        ("maxkcut", "max_k_cut"),
        ("fastrp", "fast_rp"),
        ("beta.graphSage", "graph_sage"),
        ("hashgnn", "hash_gnn"),
        ("astar", "a_star"),
        ("kspanning_tree", "k_spanning_tree"),
        ("prizesteiner_tree", "prize_steiner_tree"),
        ("spanning_tree", "spanning_tree"),
        ("steiner_tree", "steiner_tree"),
        ("beta.pipeline.nodeClassification", "pipeline.node_classification"),
        ("alpha.pipeline.nodeClassification", "pipeline.node_classification"),
        ("beta.pipeline.linkPrediction", "pipeline.link_prediction"),
        ("alpha.pipeline.linkPrediction", "pipeline.link_prediction"),
        ("alpha.pipeline.nodeRegression", "pipeline.node_regression"),
    ]
)

IGNORED_EXPECTED_PARAMETERS = {
    r".*\.write": ["write_to_result_store"],
    r".*shortest_path\.dijkstra.*": ["target_node"],
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
    r".*scale_properties.*": ["relationship_types"],
    r".*collapse_path.*": ["relationship_types"],
    r"graph.drop": ["username", "db_name"],
    r"graph.generate": ["relationship_count", "validate_relationships"],
}

EXPECTED_PARAMETER_NAME_ALIASES = {
    r"pipeline\.(node_classification|node_regression|link_prediction)\.create$": {
        "input": "pipeline_name",
    },
    r"pipeline\.(node_classification|node_regression|link_prediction)\.train$": {
        "pipeline": "pipeline_name",
    },
}

IGNORED_ACTUAL_PARAMETERS = {
    r"pipeline\.(node_classification|node_regression|link_prediction)\.add_node_property": ["config"],
}

ADJUSTED_PARAM_DEFAULT_VALUES: dict[str, dict[str, Any]] = {
    ".*": {
        "concurrency": None,
        "job_id": None,
        "write_concurrency": None,
    },
    ".*(knn|node_similarity).filtered.*": {
        "source_node_filter": None,
        "target_node_filter": None,
    },
    ".*pipeline.list": {
        "pipeline_name": None,
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
    ".*triangles$": {
        "max_degree": None,
    },
    "pipeline.drop": {
        "fail_if_missing": False,
    },
    "graph.generate": {
        "read_concurrency": None,
    },
    "graph.list": {"G": None},
}

# per endpoint from original to actual
ADJUSTED_RETURN_FIELDS: dict[str, dict[str, str]] = {
    "graph.drop": {"schema": "graph_schema", "schema_with_orientation": "graph_schema"},
    "graph.list": {"schema": "graph_schema", "schema_with_orientation": "graph_schema"},
}


def method_str(method: MethodType) -> str:
    return f"{method.__self__.__class__.__name__}.{method.__name__}"


def to_snake(camel: str) -> str:
    snake = re.sub(r"([A-Z]+)([A-Z][a-z])", lambda m: f"{m.group(1)}_{m.group(2)}", camel)
    snake = re.sub(r"([a-z])([A-Z])", lambda m: f"{m.group(1)}_{m.group(2)}", snake)
    snake = snake.replace("-", "_")
    return snake.lower()


def pythonic_endpoint_name(
    endpoint: str,
    endpoint_mappings: OrderedDict[str, str] | None = None,
) -> str:
    endpoint = endpoint.removeprefix("gds.")

    for old, new in (endpoint_mappings or BASE_ENDPOINT_MAPPINGS).items():
        if old in endpoint:
            endpoint = endpoint.replace(old, new)

    return ".".join(to_snake(part) for part in endpoint.split("."))


def resolve_callable_object(endpoints: object, endpoint: str) -> MethodType | None:
    resolved_object: Any = endpoints
    for endpoint_part in endpoint.split("."):
        if not hasattr(resolved_object, endpoint_part):
            return None

        resolved_object = getattr(resolved_object, endpoint_part)

    if not callable(resolved_object):
        raise ValueError(f"Resolved object {resolved_object} for endpoint {endpoint} is not callable")

    if isinstance(resolved_object, MethodType):
        return resolved_object

    call_method = getattr(resolved_object, "__call__", None)
    if isinstance(call_method, MethodType):
        return call_method

    raise ValueError(f"Resolved callable object {resolved_object} for endpoint {endpoint} is not a method")


def verify_return_fields(
    callable_object: MethodType, endpoint_name: str, expected_return_fields: list[ReturnField]
) -> None:
    return_annotation: Any | None = typing.get_type_hints(callable_object).get("return")

    if not return_annotation:
        raise ValueError(
            f"Callable object {method_str(callable_object)} has no return annotation. Mypy should complain :/"
        )

    if return_annotation is DataFrame:
        return

    result_type: type[BaseModel] | None = None
    if inspect.isclass(return_annotation) and issubclass(return_annotation, BaseModel):
        result_type = return_annotation
    else:
        origin = typing.get_origin(return_annotation)
        args = typing.get_args(return_annotation)

        if origin is None and inspect.isclass(return_annotation):
            field_hints = typing.get_type_hints(return_annotation)
            model_types = [t for t in field_hints.values() if inspect.isclass(t) and issubclass(t, BaseModel)]
            if len(model_types) == 1:
                result_type = model_types[0]

        elif origin is list:
            item_type = args[0] if args else None
            if inspect.isclass(item_type) and issubclass(item_type, BaseModel):
                result_type = item_type
        elif origin is tuple or origin is NamedTuple:
            model_types = [arg for arg in args if inspect.isclass(arg) and issubclass(arg, BaseModel)]
            if len(model_types) == 1:
                result_type = model_types[0]
        elif origin is UnionType:
            model_types = [arg for arg in args if inspect.isclass(arg) and issubclass(arg, BaseModel)]
            if len(model_types) == 1:
                result_type = model_types[0]

    if not result_type:
        raise ValueError(
            f"Return annotation {return_annotation} is not a supported result type. Please add support here for testing."
        )

    actual_return_fields = set(result_type.model_fields.keys())

    expected_return_field_keys = {to_snake(field.name) for field in expected_return_fields}
    field_mappings = ADJUSTED_RETURN_FIELDS.get(endpoint_name, {})
    for spec_field, new_field in field_mappings.items():
        if spec_field in expected_return_field_keys:
            expected_return_field_keys.remove(spec_field)
            expected_return_field_keys.add(new_field)

    missing_fields = expected_return_field_keys - actual_return_fields
    extra_fields = actual_return_fields - expected_return_field_keys

    if missing_fields or extra_fields:
        raise ValueError(
            f"Callable object {callable_object} has mismatching return fields. "
            f"Missing fields: {missing_fields}, Extra fields: {extra_fields}"
        )


def verify_configuration_fields(
    callable_object: MethodType,
    endpoint_spec: EndpointSpec,
    endpoint_mappings: OrderedDict[str, str] | None = None,
) -> None:
    expected_configuration: dict[str, Parameter] = {to_snake(param.name): param for param in endpoint_spec.parameters}
    py_endpoint = pythonic_endpoint_name(endpoint_spec.name, endpoint_mappings=endpoint_mappings)

    for endpoint_pattern, ignored_params in IGNORED_EXPECTED_PARAMETERS.items():
        if re.match(endpoint_pattern, py_endpoint):
            for param in ignored_params:
                expected_configuration.pop(param, None)

    method_signature = inspect.signature(callable_object)
    actual_parameters = dict(method_signature.parameters)

    for endpoint_pattern, aliases in EXPECTED_PARAMETER_NAME_ALIASES.items():
        if re.match(endpoint_pattern, py_endpoint):
            for old_name, new_name in aliases.items():
                if old_name in expected_configuration:
                    expected_configuration[new_name] = expected_configuration.pop(old_name)

    for endpoint_pattern, ignored_params in IGNORED_ACTUAL_PARAMETERS.items():
        if re.match(endpoint_pattern, py_endpoint):
            for ignored_param in ignored_params:
                actual_parameters.pop(ignored_param, None)

    if any(param.kind is inspect.Parameter.VAR_KEYWORD for param in actual_parameters.values()):
        actual_parameters = {
            name: param for name, param in actual_parameters.items() if param.kind is not inspect.Parameter.VAR_KEYWORD
        }
        expected_configuration = {
            name: param for name, param in expected_configuration.items() if param.sourceKind is not SourceKind.CONFIG
        }

    if (
        "graph_name" in expected_configuration
        and "from_graph_name" not in expected_configuration
        and "G" in actual_parameters
        and "graph_name" not in actual_parameters
    ):
        expected_configuration["G"] = expected_configuration.pop("graph_name")
    if "from_graph_name" in expected_configuration and "G" in actual_parameters:
        expected_configuration["G"] = expected_configuration.pop("from_graph_name")
    if "graph_name_or_list_of_graph_names" in expected_configuration and "G" in actual_parameters:
        expected_configuration["G"] = expected_configuration.pop("graph_name_or_list_of_graph_names")

    missing_params = expected_configuration.keys() - actual_parameters.keys()
    extra_params = actual_parameters.keys() - expected_configuration.keys()
    if missing_params or extra_params:
        raise ValueError(
            f"Callable object {pythonic_endpoint_name(endpoint_spec.name, endpoint_mappings=endpoint_mappings)} "
            f"has mismatching method parameters. Missing parameters: {missing_params}, Extra parameters: {extra_params}"
        )

    required_params_with_default = [
        name
        for name, param in actual_parameters.items()
        if param.default is not inspect.Parameter.empty
        and not expected_configuration[name].type.optional
        and expected_configuration[name].sourceKind == SourceKind.CONFIG
    ]
    if required_params_with_default:
        raise ValueError(
            f"Callable object {pythonic_endpoint_name(endpoint_spec.name, endpoint_mappings=endpoint_mappings)} "
            f"has required parameters with default values: {required_params_with_default}"
        )

    default_adjustments: dict[str, str | None] = {}
    for endpoint_pattern, adjustments in ADJUSTED_PARAM_DEFAULT_VALUES.items():
        if re.match(endpoint_pattern, py_endpoint):
            default_adjustments.update(adjustments)

    for name, expected_param in expected_configuration.items():
        expected_default = default_adjustments.get(name, expected_param.defaultValue)
        if expected_default == [] or expected_default == "" or expected_default == {}:
            expected_default = None

        actual_default = actual_parameters[name].default
        if actual_default is inspect.Parameter.empty or actual_default == [] or actual_default == {}:
            actual_default = None

        assert actual_default == expected_default, (
            f"Mismatching default value for parameter `{name}` (got: {actual_default}, spec: {expected_default}) at endpoint "
            f"`{pythonic_endpoint_name(endpoint_spec.name, endpoint_mappings=endpoint_mappings)}`"
        )


def assert_api_spec_coverage(
    endpoints: object,
    gds_api_spec: list[EndpointWithModesSpec],
    endpoint_mappings: OrderedDict[str, str] | None = None,
) -> None:
    missing_endpoints: set[str] = set()
    available_endpoints: set[str] = set()

    for endpoint_with_modes_spec in gds_api_spec:
        for endpoint_spec in endpoint_with_modes_spec.callable_modes():
            endpoint_name = pythonic_endpoint_name(
                endpoint_spec.name,
                endpoint_mappings=endpoint_mappings,
            )
            callable_object = resolve_callable_object(endpoints, endpoint_name)
            if not callable_object and endpoint_name not in UNMAPPED_ENDPOINTS:
                missing_endpoints.add(endpoint_name)
            elif callable_object:
                available_endpoints.add(endpoint_name)
                verify_return_fields(callable_object, endpoint_name, expected_return_fields=endpoint_spec.returnFields)
                verify_configuration_fields(
                    callable_object,
                    endpoint_spec=endpoint_spec,
                    endpoint_mappings=endpoint_mappings,
                )

    print("\nGDS API Spec Coverage Summary:")
    print(f"Total endpoint specs found: {len(available_endpoints) + len(missing_endpoints)}")
    print(f"Available through gds.v2: {len(available_endpoints)}")

    newly_available_endpoints = available_endpoints.intersection(UNMAPPED_ENDPOINTS)
    assert not newly_available_endpoints, (
        f"Endpoints {newly_available_endpoints} now available, please remove from MISSING_ENDPOINTS"
    )
    assert not missing_endpoints, f"Unexpectedly missing endpoints {len(missing_endpoints)} ({missing_endpoints})"
