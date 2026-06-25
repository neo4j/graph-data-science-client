import inspect
import re
import typing
from collections import OrderedDict
from types import FunctionType, MethodType, UnionType
from typing import Any

from neo4j.graph import Node
from pandas import DataFrame
from pydantic import BaseModel

from graphdatascience.procedure_surface.api.catalog import (
    GraphFilterResult,
    GraphGenerationStats,
    GraphInfo,
    GraphInfoWithDegrees,
    GraphSamplingResult,
    GraphWithFilterResult,
    GraphWithGenerationStats,
    GraphWithSamplingResult,
)
from graphdatascience.procedure_surface.api.debug_endpoints import DebugSysInfoResult
from graphdatascience.server_version.server_version import ServerVersion
from tests.integration.procedure_surface.gds_api_spec import (
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
    "split_relationships.mutate",
    "memory.summary",
    "memory.list",
    "list",  # listing only available endpoints
    "user_log",
    "graph.export",
    "internal.graph.size_of",
    "graph.node_property.stream",
    "graph.relationship_properties.write",
    "graph.exists",
    "graph.node_label.write",
    "graph.export.csv",
    "graph.relationship.write",
    "graph.relationship_property.stream",
    "graph.node_label.mutate",
    "graph.relationship_properties.stream",
    # newly added in the spec, not supported by the python client yet
    "fast_path.mutate",  # Arrow-only (AGA) endpoint
    "fast_path.stream",  # Arrow-only (AGA) endpoint
    "fast_path.write",  # Arrow-only (AGA) endpoint
    "util.infinity",  # built-in in python
    "util.is_finite",  # built-in in python
    "util.is_infinite",  # built-in in python
    "util.na_n",  # built-in in python
}

BASE_ENDPOINT_MAPPINGS = OrderedDict(
    [
        ("closeness.harmonic", "harmonic_centrality"),
        ("closeness", "closeness_centrality"),
        ("betweenness", "betweenness_centrality"),
        ("degree", "degree_centrality"),
        ("eigenvector", "eigenvector_centrality"),
        ("linkprediction", "topological_link_prediction"),
        ("influenceMaximization.celf", "influence_maximization_celf"),
        ("cliquecounting", "clique_counting"),
        ("k1coloring", "k1_coloring"),
        ("kcore", "k_core_decomposition"),
        ("maxkcut", "max_k_cut"),
        ("fastrp", "fast_rp"),
        ("beta.graphSage", "graph_sage"),
        ("ml.kge.predict", "kge.predict"),
        ("hashgnn", "hash_gnn"),
        ("astar", "a_star"),
        ("kspanning_tree", "k_spanning_tree"),
        ("prizesteiner_tree", "prize_steiner_tree"),
        ("spanning_tree", "spanning_tree"),
        ("steiner_tree", "steiner_tree"),
        ("version", "server_version"),
        ("beta.pipeline.nodeClassification", "pipeline.node_classification"),
        ("alpha.pipeline.nodeClassification", "pipeline.node_classification"),
        ("beta.pipeline.linkPrediction", "pipeline.link_prediction"),
        ("alpha.pipeline.linkPrediction", "pipeline.link_prediction"),
        ("alpha.pipeline.nodeRegression", "pipeline.node_regression"),
    ]
)


IGNORED_PARAMETERS = {
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
    r".*kge.predict.*": ["node_labels"],
    r".*scale_properties.*": ["relationship_types"],
    r".*collapse_path.*": ["relationship_types"],
    r".*graph.drop": ["username", "db_name"],
    r".*graph.filter": ["graph_name"],
    r".*cnarw": ["graph_name"],
    r".*rwr": ["graph_name"],
    r".*graph.generate": ["validate_relationships", "relationship_count", "graph_name"],
    r".*node_properties.drop": ["sudo", "log_progress"],
    r".*model.list": ["model_name"],
}

EXPECTED_PARAMETER_NAME_ALIASES = {
    r"pipeline\.(node_classification|node_regression|link_prediction)\.create$": {
        "input": "pipeline_name",
    },
    r"pipeline\.(node_classification|node_regression|link_prediction)\.train$": {
        "pipeline": "pipeline_name",
    },
    r".*graph.drop": {"graph_name_or_list_of_graph_names": "G"},
    r".*graph.filter": {"from_graph_name": "G"},
    r".*cnarw": {"from_graph_name": "G"},
    r".*rwr": {"from_graph_name": "G"},
}

IGNORED_ACTUAL_PARAMETERS = {
    r"pipeline\.(node_classification|node_regression|link_prediction)\.add_node_property": ["config"],
    r".*graph.node_properties.stream": ["job_id", "db_node_properties"],
    r".*graph.relationships.stream": ["relationship_properties"],
    r".*graph.relationships.drop": ["fail_if_missing"],
}

ADJUSTED_PARAM_DEFAULT_VALUES: dict[str, dict[str, Any]] = {
    ".*": {
        "concurrency": None,
        "job_id": None,
        "write_concurrency": None,
        "read_concurrency": None,
    },
    ".*(knn|node_similarity).filtered.*": {
        "source_node_filter": None,
        "target_node_filter": None,
    },
    ".*kge.predict.*": {
        "source_node_filter": None,
        "target_node_filter": None,
        "relationship_types": ["*"],
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
    r".*graph.filter": {"parameters": None},
    r".*graph.generate": {"relationship_property": None},
    r".*graph.list": {"G": None},
}


RETURN_CLASS_OVERRIDES = {
    GraphWithFilterResult: GraphFilterResult,
    GraphWithGenerationStats: GraphGenerationStats,
    GraphWithSamplingResult: GraphSamplingResult,
}

EXPECTED_RETURN_FIELD_ALIASES = {
    "schema_with_orientation": "graph_schema",
}

EXPECTED_IGNORED_RETURN_FIELDS = {GraphInfo: ["schema"], GraphInfoWithDegrees: ["schema"]}

# Return types whose fields intentionally don't map field-by-field to the spec's return fields
# (opaque value objects, or pivoted key/value results), so they can't be verified against the spec.
RETURN_VERIFICATION_SKIPPED_TYPES = {DebugSysInfoResult}

# Python return types that wrap a single scalar value and should be verified against the spec's scalar
# return type rather than structurally (e.g. ServerVersion wraps a version String).
SCALAR_RETURN_CLASS_OVERRIDES: dict[type, type] = {ServerVersion: str}

# Maps a scalar spec return-type name (lower-cased) to the Python type the endpoint is expected to return.
SCALAR_RETURN_TYPES: dict[str, type] = {
    "boolean": bool,
    "double": float,
    "float": float,
    "long": int,
    "int": int,
    "integer": int,
    "string": str,
    "node": Node,
}

# Spec return-type names that carry no usable Python type (opaque/dynamic), so any annotation is accepted.
OPAQUE_RETURN_TYPES = {"object"}


def method_str(method: MethodType | FunctionType) -> str:
    if isinstance(method, MethodType):
        return f"{method.__self__.__class__.__name__}.{method.__name__}"
    return method.__qualname__


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


def resolve_callable_object(endpoints: type, endpoint: str) -> MethodType | FunctionType | None:
    resolved_object: Any = endpoints

    for endpoint_part in endpoint.split("."):
        if inspect.isclass(resolved_object):
            static_attr = inspect.getattr_static(resolved_object, endpoint_part, None)
            if static_attr is None:
                return None
            if isinstance(static_attr, property):
                hints = typing.get_type_hints(static_attr.fget)
                next_type = hints.get("return")
                if next_type is None or not inspect.isclass(next_type):
                    return None
                resolved_object = next_type
            elif inspect.isfunction(static_attr):
                resolved_object = static_attr
            else:
                return None
        else:
            if not hasattr(resolved_object, endpoint_part):
                return None
            resolved_object = getattr(resolved_object, endpoint_part)

    if not callable(resolved_object):
        raise ValueError(f"Resolved object {resolved_object} for endpoint {endpoint} is not callable")

    if isinstance(resolved_object, MethodType):
        return resolved_object

    if inspect.isfunction(resolved_object):
        return resolved_object

    if inspect.isclass(resolved_object):
        static_call = inspect.getattr_static(resolved_object, "__call__", None)
        if inspect.isfunction(static_call):
            return static_call

    call_method = getattr(resolved_object, "__call__", None)
    if isinstance(call_method, MethodType):
        return call_method

    raise ValueError(f"Resolved callable object {resolved_object} for endpoint {endpoint} is not a method")


def verify_return_fields(callable_object: MethodType | FunctionType, expected_return_fields: list[ReturnField]) -> None:
    return_annotation: Any | None = typing.get_type_hints(callable_object).get("return")

    for expected, override in RETURN_CLASS_OVERRIDES.items():
        if return_annotation is expected:
            return_annotation = override

    if isinstance(return_annotation, type):
        return_annotation = SCALAR_RETURN_CLASS_OVERRIDES.get(return_annotation, return_annotation)

    if not return_annotation:
        raise ValueError(
            f"Callable object {method_str(callable_object)} has no return annotation. Mypy should complain :/"
        )

    if return_annotation is DataFrame or (
        inspect.isclass(return_annotation) and issubclass(return_annotation, DataFrame)
    ):
        return

    # Opaque value objects that don't map field-by-field to the spec's return fields.
    if return_annotation in RETURN_VERIFICATION_SKIPPED_TYPES:
        return

    result_type: type[BaseModel] | None = None
    if inspect.isclass(return_annotation) and issubclass(return_annotation, BaseModel):
        result_type = return_annotation
    else:
        origin = typing.get_origin(return_annotation)
        args = typing.get_args(return_annotation)

        if origin is list:
            item_type = args[0] if args else None
            if inspect.isclass(item_type) and issubclass(item_type, BaseModel):
                result_type = item_type
        elif origin is tuple:
            model_types = [arg for arg in args if inspect.isclass(arg) and issubclass(arg, BaseModel)]
            if len(model_types) == 1:
                result_type = model_types[0]
        elif origin is UnionType:
            model_types = [arg for arg in args if inspect.isclass(arg) and issubclass(arg, BaseModel)]
            if len(model_types) == 1:
                result_type = model_types[0]

    # No structured result type: the endpoint returns a single raw value (scalar, Node, list, ...),
    # so verify its type against the spec's scalar return field instead of matching field-by-field.
    if not result_type:
        verify_scalar_return_type(callable_object, return_annotation, expected_return_fields)
        return

    actual_return_fields = set(result_type.model_fields.keys())
    expected_return_field_keys = set({to_snake(field.name) for field in expected_return_fields})

    for aliased_field, alias in EXPECTED_RETURN_FIELD_ALIASES.items():
        if aliased_field in expected_return_field_keys:
            expected_return_field_keys.add(alias)
            expected_return_field_keys.remove(aliased_field)

    for klass, ignored_fields in EXPECTED_IGNORED_RETURN_FIELDS.items():
        if result_type == klass:
            expected_return_field_keys -= set(ignored_fields)

    missing_fields = expected_return_field_keys - actual_return_fields
    extra_fields = actual_return_fields - expected_return_field_keys

    if missing_fields or extra_fields:
        raise ValueError(
            f"Callable object {callable_object} has mismatching return fields. "
            f"Missing fields: {missing_fields}, Extra fields: {extra_fields}"
        )


def verify_scalar_return_type(
    callable_object: MethodType | FunctionType,
    return_annotation: Any,
    expected_return_fields: list[ReturnField],
) -> None:
    """Verify a raw-value return annotation against the spec's single scalar return field."""
    if len(expected_return_fields) != 1:
        raise ValueError(
            f"Expected exactly one return field for scalar-returning {method_str(callable_object)}, "
            f"got {len(expected_return_fields)}: {[f.name for f in expected_return_fields]}"
        )

    spec_type = expected_return_fields[0].type.typeName.lower()

    # Opaque/dynamic spec types accept any return annotation.
    if spec_type in OPAQUE_RETURN_TYPES:
        return

    if spec_type.startswith("list"):
        if typing.get_origin(return_annotation) is not list:
            raise ValueError(
                f"{method_str(callable_object)} returns {return_annotation}, but the spec expects a list "
                f"({spec_type})."
            )
        return

    expected_type = SCALAR_RETURN_TYPES.get(spec_type)
    if expected_type is None:
        raise ValueError(
            f"Unsupported scalar return spec type '{spec_type}' for {method_str(callable_object)}. "
            f"Please add support here for testing."
        )

    if not (inspect.isclass(return_annotation) and issubclass(return_annotation, expected_type)):
        raise ValueError(
            f"{method_str(callable_object)} return type {return_annotation} does not match "
            f"spec type '{spec_type}' (expected {expected_type})."
        )


def verify_configuration_fields(
    callable_object: MethodType | FunctionType,
    endpoint_spec: EndpointSpec,
    endpoint_mappings: OrderedDict[str, str] | None = None,
) -> None:
    expected_configuration: dict[str, Parameter] = {to_snake(param.name): param for param in endpoint_spec.parameters}
    py_endpoint = pythonic_endpoint_name(endpoint_spec.name, endpoint_mappings=endpoint_mappings)

    for endpoint_pattern, ignored_params in IGNORED_PARAMETERS.items():
        if re.match(endpoint_pattern, py_endpoint):
            for param in ignored_params:
                expected_configuration.pop(param, None)

    if "graph_name" in expected_configuration:
        expected_configuration["G"] = expected_configuration.pop("graph_name")

    method_signature = inspect.signature(callable_object)
    actual_parameters = dict(method_signature.parameters)
    actual_parameters.pop("self", None)

    for endpoint_pattern, ignored_params in IGNORED_PARAMETERS.items():
        if re.match(endpoint_pattern, py_endpoint):
            for param in ignored_params:
                actual_parameters.pop(param, None)

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
        if expected_default == []:
            expected_default = None

        actual_default = actual_parameters[name].default
        if actual_default is inspect.Parameter.empty:
            actual_default = None
        if actual_default == []:
            actual_default = None

        assert actual_default == expected_default, (
            f"Mismatching default value for parameter `{name}` (got: {actual_default}, spec: {expected_default}) at endpoint "
            f"`{pythonic_endpoint_name(endpoint_spec.name, endpoint_mappings=endpoint_mappings)}`"
        )


def assert_api_spec_coverage(
    endpoints: type,
    gds_api_spec: list[EndpointWithModesSpec],
    endpoint_mappings: OrderedDict[str, str] | None = None,
    unmapped_endpoints: set[str] | None = None,
) -> None:
    if unmapped_endpoints is None:
        unmapped_endpoints = UNMAPPED_ENDPOINTS

    missing_endpoints: set[str] = set()
    available_endpoints: set[str] = set()

    for endpoint_with_modes_spec in gds_api_spec:
        for endpoint_spec in endpoint_with_modes_spec.callable_modes():
            endpoint_name = pythonic_endpoint_name(
                endpoint_spec.name,
                endpoint_mappings=endpoint_mappings,
            )
            callable_object = resolve_callable_object(endpoints, endpoint_name)
            if not callable_object and endpoint_name not in unmapped_endpoints:
                missing_endpoints.add(endpoint_name)
            elif callable_object:
                available_endpoints.add(endpoint_name)
                verify_return_fields(callable_object, expected_return_fields=endpoint_spec.returnFields)
                verify_configuration_fields(
                    callable_object,
                    endpoint_spec=endpoint_spec,
                    endpoint_mappings=endpoint_mappings,
                )

    print("\nGDS API Spec Coverage Summary:")
    print(f"Total endpoint specs found: {len(available_endpoints) + len(missing_endpoints)}")
    print(f"Available through gds.v2: {len(available_endpoints)}")

    newly_available_endpoints = available_endpoints.intersection(unmapped_endpoints)
    assert not newly_available_endpoints, (
        f"Endpoints {newly_available_endpoints} now available, please remove from MISSING_ENDPOINTS"
    )
    assert not missing_endpoints, f"Unexpectedly missing endpoints {len(missing_endpoints)} ({missing_endpoints})"
