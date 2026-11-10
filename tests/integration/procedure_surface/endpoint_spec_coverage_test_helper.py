from __future__ import annotations

import inspect
import typing
from datetime import datetime
from types import UnionType
from typing import Any

from pandas import DataFrame
from pydantic import BaseModel

from graphdatascience.procedure_surface.api.catalog import (
    GraphFilterResult,
    GraphGenerationStats,
    GraphSamplingResult,
    GraphWithFilterResult,
    GraphWithGenerationStats,
    GraphWithSamplingResult,
)
from tests.integration.procedure_surface.api_spec import (
    EndpointSpec,
)
from tests.integration.procedure_surface.api_spec_coverage_test_helper import (
    pythonic_endpoint_name,
    resolve_callable_object,
    to_snake,
)

# Parameters the client's endpoint methods carry that the specs usually do not publish:
# execution and security infrastructure. A parameter is only ignored where the spec
# leaves it out; where a spec does publish it (the graph catalog publishes `username`
# on its plugin endpoints), the client's parameter is verified like any other.
CLIENT_ONLY_PARAMETERS = {"log_progress", "sudo", "username"}

# Endpoints whose client path differs from the published endpoint name. The session
# surface publishes the Arrow action names; the client exposes its own, nicer names.
CLIENT_PATH_ALIASES: dict[str, str] = {
    "graph.project.filter": "graph.filter",
    "graph.sample.cnarw.estimate": "graph.sample.estimate",
}

# Parameters naming the graph an endpoint operates on. The python client takes that
# graph as its positional `G` argument; a parameter naming a *new* graph keeps its
# published name (`graph.generate`, the output name of `graph.filter` and the samplers).
# The alias is python-specific: the spec keeps the name the server uses.
INPUT_GRAPH_PARAMETERS: dict[str, set[str]] = {
    "graph.drop": {"graph_name", "graph_name_or_list_of_graph_names"},
    "graph.export": {"graph_name"},
    "graph.export.csv": {"graph_name"},
    "graph.filter": {"from_graph_name"},
    "graph.list": {"graph_name"},
    "graph.node_labels.mutate": {"graph_name"},
    "graph.node_labels.write": {"graph_name"},
    "graph.node_properties.drop": {"graph_name"},
    "graph.node_properties.stream": {"graph_name"},
    "graph.node_properties.write": {"graph_name"},
    "graph.node_property.stream": {"graph_name"},
    "graph.relationship_properties.stream": {"graph_name"},
    "graph.relationship_properties.write": {"graph_name"},
    "graph.relationship_property.stream": {"graph_name"},
    "graph.relationships.drop": {"graph_name"},
    "graph.relationships.index_inverse": {"graph_name"},
    "graph.relationships.stream": {"graph_name"},
    "graph.relationships.to_undirected": {"graph_name"},
    "graph.sample.cnarw": {"from_graph_name"},
    "graph.sample.estimate": {"graph_name"},
    "graph.sample.rwr": {"from_graph_name"},
}

# Client parameters the spec does not publish: convenience arguments the client adds on
# top of the contract. Each is a candidate for the spec or for removal.
IGNORED_CLIENT_PARAMETERS: dict[str, set[str]] = {
    "graph.filter": {"overwrite"},
    "graph.generate": {"overwrite", "read_concurrency"},
    "graph.sample.cnarw": {"overwrite"},
    "graph.sample.rwr": {"overwrite"},
    "graph.node_properties.stream": {"db_node_properties"},
    "graph.node_property.stream": {"db_node_properties"},
    "graph.relationships.stream": {"relationship_properties"},
    "graph.relationships.drop": {"fail_if_missing"},
}

# Return fields the client names differently than the spec.
RETURN_FIELD_ALIASES = {"schema_with_orientation": "graph_schema"}

# Return fields the spec publishes that the client's result models deliberately leave out.
# `schema` is deprecated in the API and superseded by `schemaWithOrientation`.
IGNORED_RETURN_FIELDS = {"schema"}

# Return types the spec cannot be verified against field by field: the client returns a
# scalar where the spec models a row (e.g. `graph.exists` returns a bool).
RETURN_VERIFICATION_SKIPPED_TYPES: set[type] = {bool, type(None)}

# The client wraps some results in the graph they created; the spec models the rows.
RETURN_CLASS_OVERRIDES: dict[type, type] = {
    GraphWithFilterResult: GraphFilterResult,
    GraphWithGenerationStats: GraphGenerationStats,
    GraphWithSamplingResult: GraphSamplingResult,
}

# Parameters the spec exposes that the client does not map onto a method parameter.
IGNORED_SPEC_PARAMETERS: dict[str, set[str]] = {
    # The spec still publishes the deprecated singular `targetNode`; the client exposes
    # only the plural `target_nodes` (which accepts a single id or a list).
    "shortest_path.dijkstra": {"target_node"},
    # The client's stream endpoints do not expose the server's stream knobs yet.
    "graph.node_properties.stream": {"consecutive_ids", "batch_size"},
    "graph.relationships.stream": {"consecutive_ids", "batch_size"},
    # The client's sampling estimate takes no job id and no random seed.
    "graph.sample.estimate": {"job_id", "random_seed"},
}

# Maps a JSON Schema type to the Python type kinds an annotation may use for it. `number`
# accepts `int` because an integer is a valid JSON number.
_TYPE_COMPATIBILITY: dict[str, set[str]] = {
    "integer": {"integer", "number", "any"},
    "number": {"number", "integer", "any"},
    "string": {"string", "any"},
    "boolean": {"boolean", "any"},
    "object": {"object", "any"},
    "array": {"array", "any"},
    "null": {"null"},
}


def assert_endpoint_spec_coverage(
    endpoints: type,
    endpoint_specs: list[EndpointSpec],
    unmapped_endpoints: set[str] | None = None,
) -> None:
    if unmapped_endpoints is None:
        unmapped_endpoints = set()

    missing_endpoints: set[str] = set()
    available_endpoints: set[str] = set()

    for endpoint in endpoint_specs:
        client_path = _client_path(endpoint)
        if client_path in unmapped_endpoints:
            # endpoints the surface intentionally does not map
            continue
        callable_object = resolve_callable_object(endpoints, client_path)
        if callable_object is None:
            missing_endpoints.add(client_path)
            continue

        available_endpoints.add(client_path)
        verify_configuration_fields(callable_object, endpoint)
        verify_result_fields(callable_object, endpoint)

    print("\nAPI Spec Coverage Summary:")
    print(f"Total endpoint specs found: {len(endpoint_specs)}")
    print(f"Available through the client: {len(available_endpoints)}")

    assert not missing_endpoints, f"Unexpectedly missing endpoints {len(missing_endpoints)} ({missing_endpoints})"


def _client_path(endpoint: EndpointSpec) -> str:
    # The spec publishes the public endpoint name; the client maps it to its own path.
    path = pythonic_endpoint_name(endpoint.name)
    return CLIENT_PATH_ALIASES.get(path, path)


def verify_configuration_fields(
    callable_object: Any,
    endpoint: EndpointSpec,
) -> None:
    client_path = _client_path(endpoint)
    expected = _expected_parameters(endpoint)

    signature = inspect.signature(callable_object)
    actual = {
        name: parameter
        for name, parameter in signature.parameters.items()
        if name not in ("self", "G")
        and not (name in CLIENT_ONLY_PARAMETERS and name not in expected)
        and name not in IGNORED_CLIENT_PARAMETERS.get(client_path, set())
    }

    _verify_positional_graph(callable_object, endpoint, expected)

    missing = expected.keys() - actual.keys()
    extra = actual.keys() - expected.keys()
    if missing or extra:
        raise ValueError(
            f"Callable object {client_path} has mismatching method parameters. "
            f"Missing parameters: {missing}, Extra parameters: {extra}"
        )

    annotations = typing.get_type_hints(callable_object)
    for name, (schema, required) in expected.items():
        parameter = actual[name]
        if required:
            if parameter.default is not inspect.Parameter.empty:
                raise ValueError(
                    f"Parameter `{name}` is required in the spec but has a default value at `{client_path}`."
                )
        elif _normalise_default(parameter.default) != _normalise_default(schema.get("default")):
            raise ValueError(
                f"Mismatching default value for parameter `{name}` "
                f"(client: {_normalise_default(parameter.default)!r}, spec: {schema.get('default')!r}) "
                f"at endpoint `{client_path}`."
            )

        if not _types_compatible(schema, annotations.get(name)):
            raise ValueError(
                f"Mismatching type for parameter `{name}` "
                f"(client: {annotations.get(name)}, spec: {schema}) at endpoint `{client_path}`."
            )


def verify_result_fields(
    callable_object: Any,
    endpoint: EndpointSpec,
) -> None:
    if endpoint.result is None:
        # The compute-only mode returns a job handle, not modelled rows.
        return

    client_path = _client_path(endpoint)
    return_annotation = typing.get_type_hints(callable_object).get("return")
    if _is_dataframe(return_annotation) or return_annotation in RETURN_VERIFICATION_SKIPPED_TYPES:
        # Stream modes return a DataFrame, which is not modelled field by field.
        return

    result_type = _result_model(return_annotation)
    if result_type is None:
        raise ValueError(
            f"Could not determine a result model for `{client_path}` from return annotation `{return_annotation}`."
        )

    expected = {
        RETURN_FIELD_ALIASES.get(to_snake(name), to_snake(name)): schema
        for name, schema in endpoint.result.get("properties", {}).items()
        if to_snake(name) not in IGNORED_RETURN_FIELDS
    }
    actual = set(result_type.model_fields)

    missing = expected.keys() - actual
    extra = actual - expected.keys()
    if missing or extra:
        raise ValueError(
            f"Callable object {client_path} returns {result_type.__name__} with mismatching fields. "
            f"Missing fields: {missing}, Extra fields: {extra}"
        )

    annotations = typing.get_type_hints(result_type)
    for name, schema in expected.items():
        if not _types_compatible(schema, annotations.get(name)):
            raise ValueError(
                f"Mismatching type for result field `{name}` of {result_type.__name__} "
                f"(client: {annotations.get(name)}, spec: {schema}) at endpoint `{client_path}`."
            )


def _expected_parameters(endpoint: EndpointSpec) -> dict[str, tuple[dict[str, Any], bool]]:
    expected: dict[str, tuple[dict[str, Any], bool]] = {}
    input_graph_parameters = INPUT_GRAPH_PARAMETERS.get(_client_path(endpoint), set())

    for schema in (endpoint.config, endpoint.mode_config):
        if schema is None:
            continue
        required = set(schema.get("required", []))
        for name, property_schema in schema.get("properties", {}).items():
            if to_snake(name) in input_graph_parameters:
                # verified as the positional `G` argument instead
                continue
            expected[to_snake(name)] = (property_schema, name in required)

    for ignored in _ignored_spec_parameters(endpoint):
        expected.pop(ignored, None)

    return expected


def _ignored_spec_parameters(endpoint: EndpointSpec) -> set[str]:
    """Spec parameters to skip, looked up by client path.

    A table entry may name an endpoint without its mode, which then covers every mode
    of that endpoint.
    """
    client_path = _client_path(endpoint)
    ignored = set(IGNORED_SPEC_PARAMETERS.get(client_path, set()))

    if endpoint.mode:
        mode_suffix = f".{to_snake(endpoint.mode)}"
        ignored |= IGNORED_SPEC_PARAMETERS.get(client_path.removesuffix(mode_suffix), set())

    return ignored


def _verify_positional_graph(
    callable_object: Any, endpoint: EndpointSpec, expected: dict[str, tuple[dict[str, Any], bool]]
) -> None:
    """The graph the spec names is the client's positional `G` argument."""
    input_graph_parameters = INPUT_GRAPH_PARAMETERS.get(_client_path(endpoint), set())
    if not input_graph_parameters:
        return

    if "G" not in inspect.signature(callable_object).parameters:
        raise ValueError(
            f"`{_client_path(endpoint)}` names the graph as {sorted(input_graph_parameters)} "
            f"in the spec, but has no positional `G` parameter to map it onto."
        )


def _normalise_default(value: Any) -> Any:
    # an empty map or list in the spec maps to None in python
    if value is inspect.Parameter.empty or value == [] or value == {}:
        return None
    return value


def _types_compatible(schema: dict[str, Any], annotation: Any) -> bool:
    schema_kinds = _schema_kinds(schema)
    if "any" in schema_kinds:
        return True

    annotation_kinds = _annotation_kinds(annotation)
    return any(annotation_kinds & _TYPE_COMPATIBILITY.get(kind, {"any"}) for kind in schema_kinds)


def _schema_kinds(schema: dict[str, Any]) -> set[str]:
    if "anyOf" in schema:
        kinds: set[str] = set()
        for sub_schema in schema["anyOf"]:
            kinds |= _schema_kinds(sub_schema)
        return kinds
    if "enum" in schema:
        return {"string"}

    type_name = schema.get("type")
    if type_name == "array":
        return {"array"}
    if type_name == "object":
        return {"object"}
    return {type_name} if type_name else {"any"}


def _annotation_kinds(annotation: Any) -> set[str]:
    if annotation is None:
        return {"any"}

    origin = typing.get_origin(annotation)
    if origin is typing.Union or origin is UnionType:
        kinds: set[str] = set()
        for arg in typing.get_args(annotation):
            kinds |= _annotation_kinds(arg)
        return kinds

    if annotation is type(None):
        return {"null"}
    if origin in (list, tuple):
        return {"array"}
    if origin is dict:
        return {"object"}
    if annotation is bool:
        return {"boolean"}
    if annotation is int:
        return {"integer"}
    if annotation is float:
        return {"number"}
    if annotation is str or annotation is datetime:
        # the spec renders datetimes as RFC 3339 strings (`format: date-time`)
        return {"string"}
    if annotation is Any:
        return {"any"}
    if inspect.isclass(annotation) and issubclass(annotation, BaseModel):
        return {"object"}
    return {"object"}


def _is_dataframe(annotation: Any) -> bool:
    return annotation is DataFrame or (inspect.isclass(annotation) and issubclass(annotation, DataFrame))


def _result_model(annotation: Any) -> type[BaseModel] | None:
    annotation = RETURN_CLASS_OVERRIDES.get(annotation, annotation)

    if inspect.isclass(annotation) and issubclass(annotation, BaseModel):
        return annotation

    # The client wraps the spec's rows in a list or tuple where it returns several of
    # them (e.g. dropping a list of graphs); the rows themselves are what the spec models.
    if typing.get_origin(annotation) in (list, tuple):
        models = [arg for arg in typing.get_args(annotation) if inspect.isclass(arg) and issubclass(arg, BaseModel)]
        if len(models) == 1:
            return models[0]

    return None
