from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pandas import DataFrame
from pydantic import BaseModel

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from tests.integration.procedure_surface.session_api_spec import (
    SUPPORTED_API_SPEC_VERSION,
    SessionApiSpec,
    SessionEndpointSpec,
    resolve_spec_from_file,
)
from tests.integration.procedure_surface.session_api_spec_coverage_test_helper import (
    assert_session_api_spec_coverage,
    verify_configuration_fields,
    verify_result_fields,
)

SPEC: dict[str, Any] = {
    "apiSpecVersion": 1,
    "algorithms": {
        "centrality.foo": {
            "category": "centrality",
            "description": "Foo algorithm.",
            "endpointName": "foo",
            "config": {"$ref": "#/$defs/FooConfig"},
            "modes": {
                "compute": {},
                "mutate": {
                    "modeConfig": {"$ref": "#/$defs/FooMutateModeConfig"},
                    "result": {"$ref": "#/$defs/FooMutateResult"},
                },
                "stream": {"result": {"$ref": "#/$defs/FooStreamRow"}},
            },
        },
        "embeddings.train": {
            "category": "embeddings",
            "description": "Trains a model.",
            "endpointName": "train",
            "config": {"$ref": "#/$defs/FooConfig"},
            "modes": {},
        },
    },
    "defs": {
        "FooConfig": {
            "type": "object",
            "additionalProperties": False,
            "properties": {"maxIterations": {"type": "integer", "default": 20}},
        },
        "FooMutateModeConfig": {
            "type": "object",
            "additionalProperties": False,
            "properties": {"mutateProperty": {"type": "string"}},
            "required": ["mutateProperty"],
        },
        "FooMutateResult": {
            "type": "object",
            "additionalProperties": False,
            "properties": {"nodePropertiesWritten": {"type": "integer"}},
            "required": ["nodePropertiesWritten"],
        },
        "FooStreamRow": {
            "type": "object",
            "additionalProperties": False,
            "properties": {"nodeId": {"type": "integer"}},
            "required": ["nodeId"],
        },
    },
}


def _write_spec(tmp_path: Path, spec: dict[str, Any]) -> Path:
    spec_file = tmp_path / "gds-session-api-spec.json"
    spec_file.write_text(json.dumps(spec))
    return spec_file


def test_resolve_spec_from_file(tmp_path: Path) -> None:
    spec = resolve_spec_from_file(_write_spec(tmp_path, SPEC))

    assert isinstance(spec, SessionApiSpec)
    assert spec.apiSpecVersion == SUPPORTED_API_SPEC_VERSION


def test_resolve_spec_from_file_rejects_unknown_version(tmp_path: Path) -> None:
    spec_file = _write_spec(tmp_path, {**SPEC, "apiSpecVersion": SUPPORTED_API_SPEC_VERSION + 1})

    with pytest.raises(ValueError, match="Unsupported GDS Sessions API spec version"):
        resolve_spec_from_file(spec_file)


def test_resolve_spec_from_file_rejects_malformed_envelope(tmp_path: Path) -> None:
    malformed = json.loads(json.dumps(SPEC))
    malformed["algorithms"]["centrality.foo"]["unexpected"] = True

    with pytest.raises(ValueError, match="not a valid GDS Sessions API spec"):
        resolve_spec_from_file(_write_spec(tmp_path, malformed))


def test_resolve_rejects_unresolvable_ref() -> None:
    spec = SessionApiSpec.model_validate(SPEC)
    config = spec.algorithms["centrality.foo"].config

    config.ref = "#/$defs/DoesNotExist"
    with pytest.raises(ValueError, match="does not resolve"):
        spec.resolve(config)

    config.ref = "https://example.com/schema.json"
    with pytest.raises(ValueError, match="Unsupported schema reference"):
        spec.resolve(config)


def test_endpoints_are_listed_per_mode() -> None:
    spec = SessionApiSpec.model_validate(SPEC)
    endpoints = {endpoint.name: endpoint for endpoint in spec.endpoints()}

    assert set(endpoints) == {"foo.compute", "foo.mutate", "foo.stream", "train"}
    assert endpoints["foo.mutate"].mode == "mutate"
    assert endpoints["foo.mutate"].mode_config is not None
    assert endpoints["foo.compute"].result is None
    assert endpoints["train"].mode is None
    assert endpoints["train"].result is None


class FooMutateResult(BaseModel):
    node_properties_written: int


class WrongFieldsResult(BaseModel):
    wrong_field: int


class WrongTypesResult(BaseModel):
    node_properties_written: str


class FooEndpoints:
    @property
    def foo(self) -> FooEndpoints:
        return self

    def mutate(self, G: Graph, mutate_property: str, max_iterations: int = 20) -> FooMutateResult:
        return FooMutateResult(node_properties_written=0)

    def stream(self, G: Graph, max_iterations: int = 20) -> DataFrame:
        return DataFrame()

    def compute(self, G: Graph, max_iterations: int = 20) -> JobHandle:
        raise NotImplementedError

    def train(self, G: Graph, max_iterations: int = 20) -> None:
        return None


def _spec() -> SessionApiSpec:
    return SessionApiSpec.model_validate(SPEC)


def _endpoint(name: str) -> SessionEndpointSpec:
    return {endpoint.name: endpoint for endpoint in _spec().endpoints()}[name]


def test_coverage_accepts_a_matching_surface() -> None:
    assert_session_api_spec_coverage(FooEndpoints, _spec())


def test_coverage_rejects_missing_endpoint() -> None:
    raw = json.loads(json.dumps(SPEC))
    raw["algorithms"]["centrality.bar"] = {
        "category": "centrality",
        "description": "Bar algorithm.",
        "endpointName": "bar",
        "config": {"$ref": "#/$defs/FooConfig"},
        "modes": {},
    }

    with pytest.raises(AssertionError, match="Unexpectedly missing endpoints"):
        assert_session_api_spec_coverage(FooEndpoints, SessionApiSpec.model_validate(raw))


def test_verify_rejects_extra_parameter() -> None:
    def mutate(G: Graph, mutate_property: str, unexpected: int = 1, max_iterations: int = 20) -> FooMutateResult:
        return FooMutateResult(node_properties_written=0)

    with pytest.raises(ValueError, match="Extra parameters: {'unexpected'}"):
        verify_configuration_fields(mutate, _endpoint("foo.mutate"))


def test_verify_rejects_missing_parameter() -> None:
    def mutate(G: Graph, max_iterations: int = 20) -> FooMutateResult:
        return FooMutateResult(node_properties_written=0)

    with pytest.raises(ValueError, match="Missing parameters: {'mutate_property'}"):
        verify_configuration_fields(mutate, _endpoint("foo.mutate"))


def test_verify_rejects_wrong_default() -> None:
    def mutate(G: Graph, mutate_property: str, max_iterations: int = 42) -> FooMutateResult:
        return FooMutateResult(node_properties_written=0)

    with pytest.raises(ValueError, match="Mismatching default value"):
        verify_configuration_fields(mutate, _endpoint("foo.mutate"))


def test_verify_rejects_required_parameter_with_default() -> None:
    def mutate(G: Graph, mutate_property: str = "x", max_iterations: int = 20) -> FooMutateResult:
        return FooMutateResult(node_properties_written=0)

    with pytest.raises(ValueError, match="required in the spec but has a default value"):
        verify_configuration_fields(mutate, _endpoint("foo.mutate"))


def test_verify_rejects_wrong_parameter_type() -> None:
    def mutate(G: Graph, mutate_property: int, max_iterations: int = 20) -> FooMutateResult:
        return FooMutateResult(node_properties_written=0)

    with pytest.raises(ValueError, match="Mismatching type for parameter"):
        verify_configuration_fields(mutate, _endpoint("foo.mutate"))


def test_verify_rejects_wrong_result_field() -> None:
    def mutate(G: Graph, mutate_property: str, max_iterations: int = 20) -> WrongFieldsResult:
        return WrongFieldsResult(wrong_field=0)

    with pytest.raises(ValueError, match="mismatching fields"):
        verify_result_fields(mutate, _endpoint("foo.mutate"))


def test_verify_rejects_wrong_result_type() -> None:
    def mutate(G: Graph, mutate_property: str, max_iterations: int = 20) -> WrongTypesResult:
        return WrongTypesResult(node_properties_written="0")

    with pytest.raises(ValueError, match="Mismatching type for result field"):
        verify_result_fields(mutate, _endpoint("foo.mutate"))
