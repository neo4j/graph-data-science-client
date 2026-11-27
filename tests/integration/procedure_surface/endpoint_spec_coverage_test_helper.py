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
from graphdatascience.procedure_surface.cypher.catalog.projection_cypher_endpoints import (
    GraphProjectResult,
    GraphWithProjectResult,
)
from tests.integration.procedure_surface.api_spec import (
    EndpointSpec,
)
from tests.integration.procedure_surface.api_spec_coverage_test_helper import (
    pythonic_endpoint_name,
    resolve_callable_object,
    to_snake,
)

# Parameters the client's endpoint methods carry that the session spec does not publish:
# `gpu`, which no spec publishes yet. The specs take `sudo`, `username`, and `logProgress`
# as configuration, so those are compared.
SESSION_CLIENT_ONLY_PARAMETERS = {"gpu"}

# Endpoints whose client path differs from the published endpoint name. The session
# surface publishes the Arrow action names; the client exposes its own, nicer names.
CLIENT_PATH_ALIASES: dict[str, str] = {
    # A session projects through the remote-ops procedures, which run in the database the
    # session is attached to; the client calls them from its own projection methods.
    "arrow.project.cypher.v4": "graph.project.cypher",
    "arrow.project.store.v4": "graph.project.native",
    "graph.project": "graph.project.native",
    "graph.project.filter": "graph.filter",
}

# Client parameters the spec does not publish: convenience arguments the client adds on
# top of the contract. Each is a candidate for the spec or for removal.
IGNORED_CLIENT_PARAMETERS: dict[str, set[str]] = {
    "graph.filter": {"overwrite"},
    "graph.generate": {"overwrite", "read_concurrency"},
    "graph.sample.cnarw": {"overwrite"},
    "graph.sample.rwr": {"overwrite"},
    # The property operations read neither of these server-side, so the published
    # surface leaves them out while the client keeps offering them.
    "graph.node_properties.drop": {"log_progress", "sudo"},
    "graph.node_properties.stream": {"db_node_properties", "log_progress", "sudo"},
    "graph.node_properties.write": {"log_progress", "sudo"},
    "graph.node_property.stream": {"db_node_properties", "log_progress", "sudo"},
    "graph.relationship_properties.stream": {"log_progress", "sudo"},
    "graph.relationship_properties.write": {"log_progress", "sudo"},
    "graph.relationship_property.stream": {"log_progress", "sudo"},
    "graph.relationships.stream": {"relationship_properties"},
    "graph.relationships.drop": {"fail_if_missing"},
    # The client's native projection takes the properties alongside the projections, and
    # drops an existing graph on request.
    "graph.project.native": {"node_properties", "relationship_properties", "overwrite"},
    # The session's Cypher projection streams through the session's own transport.
    "gds.arrow.project.cypher.v4": {"batch_size", "logging", "overwrite"},
    # The session's store projection is driven with label and type filters instead of the
    # projections the procedure takes, and streams through the session's own transport.
    "gds.arrow.project.store.v4": {
        "node_label_filter",
        "relationship_type_filter",
        "logging",
        "overwrite",
    },
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
    GraphWithProjectResult: GraphProjectResult,
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
    "graph.relationship_properties.stream": {"consecutive_ids", "batch_size"},
    # The client's native projection does not expose the sizing and validation knobs.
    "graph.project.native": {
        "node_count",
        "relationship_count",
        "validate_relationships",
    },
    # The client's node-property drop takes `username` but not the other two.
    "graph.node_properties.drop": {"log_progress", "sudo"},
    # The store projection is driven with label and type filters instead of the
    # projections the procedure takes, and reads with the session's own concurrency.
    "gds.arrow.project.store.v4": {
        "node_projection",
        "relationship_projection",
        "read_concurrency",
    },
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
    client_only_parameters: set[str] | None = None,
) -> None:
    if unmapped_endpoints is None:
        unmapped_endpoints = set()
    if client_only_parameters is None:
        client_only_parameters = set()

    missing_endpoints: set[str] = set()
    available_endpoints: set[str] = set()

    for endpoint in endpoint_specs:
        if not endpoint.standalone:
            # A step of a larger operation: a client drives it through the operation that
            # starts it, so it is not a path of its own.
            continue
        client_path = _client_path(endpoint)
        if client_path in unmapped_endpoints:
            # endpoints the surface intentionally does not map
            continue
        callable_object = resolve_callable_object(endpoints, client_path)
        if callable_object is None:
            missing_endpoints.add(client_path)
            continue

        available_endpoints.add(client_path)
        verify_configuration_fields(callable_object, endpoint, client_only_parameters)
        verify_result_fields(callable_object, endpoint)

    print("\nAPI Spec Coverage Summary:")
    print(f"Total endpoint specs found: {len(endpoint_specs)}")
    print(f"Available through the client: {len(available_endpoints)}")

    assert not missing_endpoints, f"Unexpectedly missing endpoints {len(missing_endpoints)} ({missing_endpoints})"


def _client_path(endpoint: EndpointSpec) -> str:
    # The spec publishes the public endpoint name; the client maps it to its own path.
    path = pythonic_endpoint_name(endpoint.name)
    return CLIENT_PATH_ALIASES.get(path, path)


def _table_keys(endpoint: EndpointSpec) -> list[str]:
    """The keys a per-endpoint table may be keyed by.

    The published name first - the two surfaces publish some endpoints under different
    names, so the client path alone would not tell them apart - then the client path.
    """
    return [endpoint.endpoint_name, _client_path(endpoint)]


def verify_configuration_fields(
    callable_object: Any,
    endpoint: EndpointSpec,
    client_only_parameters: set[str] | None = None,
) -> None:
    client_path = _client_path(endpoint)
    expected = _expected_parameters(endpoint)
    excluded = client_only_parameters or set()

    signature = inspect.signature(callable_object)
    graph_argument = _graph_argument(endpoint, signature)
    actual = {
        name: parameter
        for name, parameter in signature.parameters.items()
        if name not in ("self", graph_argument)
        # A surface's client-only parameter is dropped where the spec leaves it out;
        # where the spec publishes it (the catalog publishes `username` on the plugin
        # endpoints), the client's parameter is verified like any other.
        and not (name in excluded and name not in expected)
        and not (name in _ignored_client_parameters(endpoint) and name not in expected)
    }

    _verify_positional_graph(endpoint, graph_argument)

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
        elif (
            # The spec declares a default for its configuration fields; the procedure
            # parameters it publishes by position carry none, so those are not compared.
            "default" in schema
            # a None client default defers to the server default
            and parameter.default is not None
            and _normalise_default(parameter.default) != _normalise_default(schema.get("default"))
        ):
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
    input_graph_parameters = _input_graph_parameters(endpoint)

    for schema in (endpoint.config, endpoint.mode_config):
        if schema is None:
            continue
        required = set(schema.get("required", []))
        for name, property_schema in schema.get("properties", {}).items():
            if to_snake(name) in input_graph_parameters:
                # verified as the positional `G` argument instead
                continue
            expected[to_snake(name)] = (property_schema, name in required)

    # The spec publishes the procedure's parameters by position with one coarse type - a
    # `String | List<String>` parameter renders as `string` - and without saying which the
    # server requires. Only the name is verified; the configuration carries the typed schema.
    for name, property_schema in endpoint.parameters.items():
        if to_snake(name) in input_graph_parameters:
            continue
        expected[to_snake(name)] = (
            {"description": property_schema.get("description", "")},
            False,
        )

    for ignored in _ignored_spec_parameters(endpoint):
        expected.pop(ignored, None)

    return expected


def _input_graph_parameters(endpoint: EndpointSpec) -> set[str]:
    """The spec parameter naming the graph the call runs on: the client's `G`.

    The spec publishes which parameter that is; endpoints that only name a graph they
    create (`graph.generate`, the samplers' output name) publish none.
    """
    if endpoint.input_graph_parameter is None:
        return set()

    return {to_snake(endpoint.input_graph_parameter)}


def _ignored_client_parameters(endpoint: EndpointSpec) -> set[str]:
    """Client parameters this endpoint's spec deliberately leaves out."""
    ignored: set[str] = set()
    for key in _table_keys(endpoint):
        ignored |= IGNORED_CLIENT_PARAMETERS.get(key, set())

    return ignored


def _ignored_spec_parameters(endpoint: EndpointSpec) -> set[str]:
    """Spec parameters to skip, looked up by client path.

    A table entry may name an endpoint without its mode, which then covers every mode
    of that endpoint.
    """
    client_path = _client_path(endpoint)
    ignored: set[str] = set()
    for key in _table_keys(endpoint):
        ignored |= IGNORED_SPEC_PARAMETERS.get(key, set())

    if endpoint.mode:
        mode_suffix = f".{to_snake(endpoint.mode)}"
        ignored |= IGNORED_SPEC_PARAMETERS.get(client_path.removesuffix(mode_suffix), set())

    return ignored


def _graph_argument(endpoint: EndpointSpec, signature: inspect.Signature) -> str | None:
    """The client parameter standing for the graph the spec names.

    The client takes it as its positional `G`; a method that already exposed the spec's
    own name keeps it (`exists(graph_name=...)`), so that name is accepted too.
    """
    if "G" in signature.parameters:
        return "G"

    if endpoint.input_graph_parameter is None:
        return None

    name = to_snake(endpoint.input_graph_parameter)
    return name if name in signature.parameters else None


def _verify_positional_graph(endpoint: EndpointSpec, graph_argument: str | None) -> None:
    """The graph the spec names is the client's own argument, not a configuration entry."""
    if endpoint.input_graph_parameter is None:
        return

    if graph_argument is None:
        raise ValueError(
            f"`{_client_path(endpoint)}` names the graph as "
            f"`{endpoint.input_graph_parameter}` in the spec, but the client has no "
            f"parameter that takes it."
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
