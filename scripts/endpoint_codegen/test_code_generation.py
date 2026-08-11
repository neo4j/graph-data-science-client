import importlib.util
import inspect
import sys
from abc import ABC
from enum import Enum
from pathlib import Path
from typing import Any, Literal

import pytest
from pydantic import BaseModel, Field

from .code_generation import NO_DESCRIPTION, generate_arrow_client_class, generate_client_class


class DummyColor(str, Enum):
    RED = "red"
    BLUE = "blue"


class DummyVariantA(BaseModel):
    variant_type: Literal["a"] = "a"
    value: int


class DummyVariantB(BaseModel):
    variant_type: Literal["b"] = "b"
    value: str


class DummyConfig(BaseModel):
    task_name: Literal["dummy_task"] = "dummy_task"
    graph_name: str = Field(description="Name of the graph.")
    variant: DummyVariantA | DummyVariantB
    color: DummyColor = DummyColor.RED
    limit: int | None = None
    random_seed: int = Field(default_factory=lambda: 42)


class ConfigWithoutGraphName(BaseModel):
    task_name: Literal["no_graph"] = "no_graph"
    limit: int | None = None


class DummyBaseConfig(BaseModel):
    concurrency: int | None = Field(default=None, description="How many threads to use.")
    log_progress: bool = True


class DummyMutateConfig(BaseModel):
    mutate_property: str = Field(description="Property to mutate.")


class DummyWriteConfig(BaseModel):
    write_property: str = Field(description="Property to write.")


MODE_CONFIGS: dict[str, type[BaseModel]] = {"mutate": DummyMutateConfig, "write": DummyWriteConfig}


class DummyMutateResult(BaseModel):
    node_properties_written: int = 0


class DummyWriteResult(BaseModel):
    properties_written: int = 0


class DummyStatsResult(BaseModel):
    compute_millis: int = 0


class FakeGraph:
    def name(self) -> str:
        return "g"


def _load_module(tmp_path: Path, module_name: str, code: str) -> Any:
    file_path = tmp_path / f"{module_name}.py"
    file_path.write_text(code)
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_generate_client_class_requires_graph_name_field() -> None:
    with pytest.raises(ValueError, match="graph_name"):
        generate_client_class(ConfigWithoutGraphName, "NoGraphEntrypoints", {"stream": None})


def test_generate_client_class_builds_abstract_signatures(tmp_path: Path) -> None:
    code = generate_client_class(
        DummyConfig,
        "DummyEntrypoints",
        {"stream": DummyMutateResult, "write": None},
        base_config=DummyBaseConfig,
        mode_configs=MODE_CONFIGS,
    )

    entrypoints = _load_module(tmp_path, "dummy_abc_module", code).DummyEntrypoints

    assert issubclass(entrypoints, ABC)
    with pytest.raises(TypeError):
        entrypoints()  # abstractmethods aren't implemented

    stream_params = inspect.signature(entrypoints.stream).parameters
    assert "task_name" not in stream_params  # single-literal tag field is excluded
    assert "graph_name" not in stream_params  # graph_name is filled from G.name() instead
    assert list(stream_params)[:2] == ["self", "G"]  # G is positional, like hand-written endpoints
    # the base config's parameters come along, and only the mode that declares one gets its extras
    assert set(stream_params) == {
        "self",
        "G",
        "variant",
        "color",
        "limit",
        "random_seed",
        *DummyBaseConfig.model_fields,
    }

    write_params = inspect.signature(entrypoints.write).parameters
    assert "write_property" in write_params
    assert "mutate_property" not in write_params
    # the mode's own parameter comes before the base config's, ahead of parameters that have defaults
    assert list(write_params).index("write_property") < list(write_params).index("concurrency")

    stream_doc = entrypoints.stream.__doc__
    assert stream_doc is not None
    assert "G\n            The graph to run the procedure on." in stream_doc
    assert "graph_name" not in stream_doc  # covered by the G entry instead
    assert f"variant\n            {NO_DESCRIPTION}" in stream_doc  # missing description falls back
    assert "concurrency\n            How many threads to use." in stream_doc  # base config is documented
    assert "task_name" not in stream_doc  # tag field has no place in the docstring either
    assert stream_doc.strip().endswith("DummyMutateResult")  # Returns section

    write_doc = entrypoints.write.__doc__
    assert write_doc is not None
    assert "write_property\n            Property to write." in write_doc  # mode config is documented
    assert "Returns" not in write_doc  # return type is None -> no Returns section


def test_generate_client_class_without_composed_configs(tmp_path: Path) -> None:
    code = generate_client_class(DummyConfig, "BareEntrypoints", {"stream": DummyMutateResult})

    entrypoints = _load_module(tmp_path, "bare_abc_module", code).BareEntrypoints

    stream_params = inspect.signature(entrypoints.stream).parameters
    assert set(stream_params) == {"self", "G", "variant", "color", "limit", "random_seed"}


class FakeEndpointsHelper:
    def __init__(self, arrow_client: Any, write_protocol: Any, show_progress: Any) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def run_job(self, graph: Any, endpoint: str, config: dict[str, Any]) -> str:
        self.calls.append(("compute", (graph, endpoint, config)))
        return "job-handle"

    def run_job_and_mutate(self, endpoint: str, config: dict[str, Any], mutate_property: str) -> dict[str, Any]:
        self.calls.append(("mutate", (endpoint, config, mutate_property)))
        return {"node_properties_written": 3}

    def run_job_and_write(
        self, endpoint: str, graph: Any, config: dict[str, Any], write_property: str
    ) -> dict[str, Any]:
        self.calls.append(("write", (endpoint, graph, config, write_property)))
        return {"properties_written": 5}

    def run_job_and_stream(self, endpoint: str, graph: Any, config: dict[str, Any]) -> str:
        self.calls.append(("stream", (endpoint, graph, config)))
        return "streamed"

    # backs both `stats` and `__call__`
    def run_job_and_get_summary(self, endpoint: str, config: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(("summary", (endpoint, config)))
        return {"compute_millis": 11}


def test_generate_arrow_client_class_builds_config_and_dispatches(tmp_path: Path) -> None:
    return_types = {
        "compute": str,
        "stream": DummyMutateResult,
        "mutate": DummyMutateResult,
        "stats": DummyStatsResult,
        "write": DummyWriteResult,
        "__call__": None,
    }
    abc_code = generate_client_class(
        DummyConfig, "DummyEntrypoints", return_types, base_config=DummyBaseConfig, mode_configs=MODE_CONFIGS
    )
    _load_module(tmp_path, "dummy_abc_module_arrow", abc_code)

    arrow_code = generate_arrow_client_class(
        DummyConfig,
        "DummyArrowEndpoints",
        entrypoints_module="dummy_abc_module_arrow",
        entrypoints_class_name="DummyEntrypoints",
        endpoint="v2/dummy.op",
        return_types=return_types,
        endpoints_helper_class=FakeEndpointsHelper,
        base_config=DummyBaseConfig,
        mode_configs=MODE_CONFIGS,
    )
    arrow_module = _load_module(tmp_path, "dummy_arrow_module", arrow_code)

    endpoints = arrow_module.DummyArrowEndpoints(arrow_client=None)
    G = FakeGraph()
    variant = DummyVariantA(value=1)

    # default_factory field: omitted when None, included when explicit
    omitted = endpoints._build_config(G, variant=variant, color=DummyColor.RED, limit=None, random_seed=None)
    assert omitted["graph_name"] == "g"  # filled from G.name()
    assert omitted["random_seed"] == 42
    assert "limit" not in omitted  # exclude_none

    explicit = endpoints._build_config(G, variant=variant, color=DummyColor.RED, limit=None, random_seed=7)
    assert explicit["random_seed"] == 7

    # the base config is dumped into the same payload as the procedure's own config
    assert omitted["log_progress"] is True  # default carried over from the model
    assert "concurrency" not in omitted  # exclude_none
    with_base = endpoints._build_config(
        G, variant=variant, color=DummyColor.RED, limit=None, random_seed=None, concurrency=4, log_progress=False
    )
    assert (with_base["concurrency"], with_base["log_progress"]) == (4, False)

    # compute hands back whatever the helper's job handle is, unwrapped
    assert endpoints.compute(G, variant=variant, color=DummyColor.RED) == "job-handle"
    kind, (graph, endpoint, _config) = endpoints._endpoints_helper.calls[-1]
    assert (kind, graph, endpoint) == ("compute", G, "v2/dummy.op")

    mutate_result = endpoints.mutate(G, variant=variant, color=DummyColor.RED, mutate_property="p")
    assert isinstance(mutate_result, DummyMutateResult)
    assert mutate_result.node_properties_written == 3
    assert endpoints._endpoints_helper.calls[-1][0] == "mutate"

    stats_result = endpoints.stats(G, variant=variant, color=DummyColor.RED)
    assert isinstance(stats_result, DummyStatsResult)
    assert stats_result.compute_millis == 11  # summary dict is unpacked into the result type
    kind, (endpoint, _config) = endpoints._endpoints_helper.calls[-1]
    assert (kind, endpoint) == ("summary", "v2/dummy.op")

    write_result = endpoints.write(G, variant=variant, color=DummyColor.RED, write_property="p", concurrency=2)
    assert isinstance(write_result, DummyWriteResult)
    kind, (endpoint, graph, config, write_property) = endpoints._endpoints_helper.calls[-1]
    assert (kind, endpoint, graph, write_property) == ("write", "v2/dummy.op", G, "p")
    # the mode's parameter goes to the helper, the base config's into the payload
    assert "write_property" not in config
    assert config["concurrency"] == 2

    stream_result = endpoints.stream(G, variant=variant, color=DummyColor.RED)
    assert stream_result == "streamed"
    kind, (endpoint, graph, _config) = endpoints._endpoints_helper.calls[-1]
    assert (kind, graph) == ("stream", G)

    # same helper call as stats, but the summary is discarded rather than wrapped
    assert endpoints(G, variant=variant, color=DummyColor.RED) is None
    assert endpoints._endpoints_helper.calls[-1][0] == "summary"


def test_generate_arrow_client_class_requires_a_mode_config_for_write() -> None:
    with pytest.raises(ValueError, match="`write`"):
        generate_arrow_client_class(
            DummyConfig,
            "DummyArrowEndpoints",
            entrypoints_module="dummy_abc_module_arrow",
            entrypoints_class_name="DummyEntrypoints",
            endpoint="v2/dummy.op",
            return_types={"write": DummyWriteResult},
            endpoints_helper_class=FakeEndpointsHelper,
        )
