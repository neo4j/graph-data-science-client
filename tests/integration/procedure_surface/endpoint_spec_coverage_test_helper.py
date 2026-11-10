from __future__ import annotations

import inspect
import typing
from types import UnionType
from typing import Any

from pandas import DataFrame
from pydantic import BaseModel

from tests.integration.procedure_surface.api_spec import (
    EndpointSpec,
)
from tests.integration.procedure_surface.api_spec_coverage_test_helper import (
    pythonic_endpoint_name,
    resolve_callable_object,
    to_snake,
)

# Parameters the client's endpoint methods carry that neither spec publishes: both the
# session and the plugin API specs omit them.
CLIENT_ONLY_PARAMETERS = {"log_progress", "sudo", "username"}

# Parameters the spec exposes that the client does not map onto a method parameter. The
# spec still publishes the deprecated singular `targetNode`; the client exposes only the
# plural `target_nodes` (which accepts a single id or a list), matching `targetNodes`.
IGNORED_SPEC_PARAMETERS: dict[str, set[str]] = {
    "shortestPath.dijkstra": {"target_node"},
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
    return pythonic_endpoint_name(endpoint.name)


def verify_configuration_fields(
    callable_object: Any,
    endpoint: EndpointSpec,
) -> None:
    client_path = _client_path(endpoint)
    expected = _expected_parameters(endpoint)

    actual = {
        name: parameter
        for name, parameter in inspect.signature(callable_object).parameters.items()
        if name not in ("self", "G") and name not in CLIENT_ONLY_PARAMETERS
    }

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
    if _is_dataframe(return_annotation):
        # Stream modes return a DataFrame, which is not modelled field by field.
        return

    result_type = _result_model(return_annotation)
    if result_type is None:
        raise ValueError(
            f"Could not determine a result model for `{client_path}` from return annotation `{return_annotation}`."
        )

    expected = {to_snake(name): schema for name, schema in endpoint.result.get("properties", {}).items()}
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

    for schema in (endpoint.config, endpoint.mode_config):
        if schema is None:
            continue
        required = set(schema.get("required", []))
        for name, property_schema in schema.get("properties", {}).items():
            expected[to_snake(name)] = (property_schema, name in required)

    for ignored in IGNORED_SPEC_PARAMETERS.get(endpoint.endpoint_name, set()):
        expected.pop(ignored, None)

    return expected


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
    if annotation is str:
        return {"string"}
    if annotation is Any:
        return {"any"}
    if inspect.isclass(annotation) and issubclass(annotation, BaseModel):
        return {"object"}
    return {"object"}


def _is_dataframe(annotation: Any) -> bool:
    return annotation is DataFrame or (inspect.isclass(annotation) and issubclass(annotation, DataFrame))


def _result_model(annotation: Any) -> type[BaseModel] | None:
    if inspect.isclass(annotation) and issubclass(annotation, BaseModel):
        return annotation

    if typing.get_origin(annotation) is tuple:
        models = [arg for arg in typing.get_args(annotation) if inspect.isclass(arg) and issubclass(arg, BaseModel)]
        if len(models) == 1:
            return models[0]

    return None
