from unittest import mock

import pytest

from graphdatascience.job import JobConfig, JobExecutionError, JobExecutor
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience


def _job_config(*, projections, steps) -> JobConfig:  # type: ignore[no-untyped-def]
    return JobConfig.model_validate({"projections": projections, "steps": steps})


def _algorithm_step(name, *, algorithm, graph, mode="stream", configuration=None):  # type: ignore[no-untyped-def]
    params = {"algorithm": algorithm, "graph": graph, "mode": mode}
    if configuration is not None:
        params["configuration"] = configuration
    return {"name": name, "type": "algorithm", "params": params}


def _gds_mock() -> mock.Mock:
    return mock.Mock(spec=AuraGraphDataScience)


def _call_names(gds: mock.Mock) -> list[str]:
    """Names of all recorded calls, in order, e.g. 'graph.project.native', 'graph.drop'."""
    return [".".join(call[0].split(".")) for call in gds.mock_calls if call[0]]


def test_run_projects_native_graph_lazily_before_first_referencing_step() -> None:
    gds = _gds_mock()
    job_config = _job_config(
        projections=[{"native": {"graph": "g1", "node_labels": ["Person"], "relationship_types": ["KNOWS"]}}],
        steps=[_algorithm_step("Run PageRank", algorithm="pageRank", graph="g1")],
    )

    JobExecutor(gds).run(job_config)

    gds.graph.project.native.assert_called_once_with(
        "g1",
        node_label_filter=["Person"],
        relationship_type_filter=["KNOWS"],
        node_properties=None,
        relationship_properties=None,
        undirected_relationship_types=None,
        inverse_indexed_relationship_types=None,
    )


def test_run_native_projection_defaults_filters_to_all() -> None:
    gds = _gds_mock()
    job_config = _job_config(
        projections=[{"native": {"graph": "g1"}}],
        steps=[_algorithm_step("Run PageRank", algorithm="pageRank", graph="g1")],
    )

    JobExecutor(gds).run(job_config)

    gds.graph.project.native.assert_called_once_with(
        "g1",
        node_label_filter=["*"],
        relationship_type_filter=["*"],
        node_properties=None,
        relationship_properties=None,
        undirected_relationship_types=None,
        inverse_indexed_relationship_types=None,
    )


def test_run_projects_cypher_graph_via_project_cypher() -> None:
    gds = _gds_mock()
    job_config = _job_config(
        projections=[{"cypher": {"graph": "g1", "query": "MATCH (n) RETURN n"}}],
        steps=[_algorithm_step("Run PageRank", algorithm="pageRank", graph="g1")],
    )

    JobExecutor(gds).run(job_config)

    gds.graph.project.cypher.assert_called_once_with("g1", "MATCH (n) RETURN n")


def test_run_algorithm_stream_mode_calls_stream_with_converted_config() -> None:
    gds = _gds_mock()
    job_config = _job_config(
        projections=[{"native": {"graph": "g1"}}],
        steps=[
            _algorithm_step(
                "Run PageRank", algorithm="pageRank", graph="g1", configuration={"dampingFactor": 0.85}
            )
        ],
    )

    JobExecutor(gds).run(job_config)

    gds.graph.get.assert_called_once_with("g1")
    graph = gds.graph.get.return_value
    gds.page_rank.stream.assert_called_once_with(graph, damping_factor=0.85)


def test_run_algorithm_write_mode_passes_write_property() -> None:
    gds = _gds_mock()
    job_config = _job_config(
        projections=[{"native": {"graph": "g1"}}],
        steps=[
            _algorithm_step(
                "Run Louvain",
                algorithm="louvain",
                graph="g1",
                mode={"name": "write", "property": "community"},
                configuration={"maxLevels": 5},
            )
        ],
    )

    JobExecutor(gds).run(job_config)

    graph = gds.graph.get.return_value
    gds.louvain.write.assert_called_once_with(graph, write_property="community", max_levels=5)


def test_run_algorithm_resolves_irregular_endpoint_alias() -> None:
    gds = _gds_mock()
    job_config = _job_config(
        projections=[{"native": {"graph": "g1"}}],
        steps=[
            _algorithm_step(
                "Run K1", algorithm="k1coloring", graph="g1", mode={"name": "mutate", "property": "c"}
            )
        ],
    )

    JobExecutor(gds).run(job_config)

    graph = gds.graph.get.return_value
    gds.k1_coloring.mutate.assert_called_once_with(graph, mutate_property="c")


def test_run_write_back_writes_node_and_relationship_properties() -> None:
    gds = _gds_mock()
    job_config = _job_config(
        projections=[{"native": {"graph": "g1"}}],
        steps=[
            _algorithm_step("Run PageRank", algorithm="pageRank", graph="g1"),
            {
                "name": "Write back",
                "type": "write-back",
                "params": {
                    "graph": "g1",
                    "node_properties": ["rank"],
                    "relationship_properties": ["weight"],
                    "relationship_types": ["REL", "REL2"],
                },
            },
        ],
    )

    JobExecutor(gds).run(job_config)

    graph = gds.graph.get.return_value
    gds.graph.node_properties.write.assert_called_once_with(graph, ["rank"])
    assert gds.graph.relationships.write.call_args_list == [
        mock.call(graph, "REL", ["weight"]),
        mock.call(graph, "REL2", ["weight"]),
    ]


def test_run_raises_on_unknown_algorithm_name() -> None:
    gds = _gds_mock()
    job_config = _job_config(
        projections=[{"native": {"graph": "g1"}}],
        steps=[_algorithm_step("Broken", algorithm="notARealAlgorithm", graph="g1")],
    )

    with pytest.raises(JobExecutionError, match="notARealAlgorithm"):
        JobExecutor(gds).run(job_config)


def test_run_raises_when_step_references_undeclared_graph() -> None:
    gds = _gds_mock()
    job_config = _job_config(
        projections=[{"native": {"graph": "g1"}}],
        steps=[_algorithm_step("Run PageRank", algorithm="pageRank", graph="does-not-exist")],
    )

    with pytest.raises(JobExecutionError, match="does-not-exist"):
        JobExecutor(gds).run(job_config)


def test_run_does_not_project_unreferenced_graphs() -> None:
    gds = _gds_mock()
    job_config = _job_config(
        projections=[
            {"native": {"graph": "g1"}},
            {"cypher": {"graph": "unused", "query": "MATCH (n) RETURN n"}},
        ],
        steps=[_algorithm_step("Run PageRank", algorithm="pageRank", graph="g1")],
    )

    JobExecutor(gds).run(job_config)

    gds.graph.project.native.assert_called_once_with(
        "g1",
        node_label_filter=["*"],
        relationship_type_filter=["*"],
        node_properties=None,
        relationship_properties=None,
        undirected_relationship_types=None,
        inverse_indexed_relationship_types=None,
    )
    gds.graph.project.cypher.assert_not_called()
    gds.graph.drop.assert_called_once_with("g1")


def test_run_projects_then_drops_each_graph_around_its_reference_window() -> None:
    gds = _gds_mock()
    job_config = _job_config(
        projections=[{"native": {"graph": "g1"}}, {"native": {"graph": "g2"}}],
        steps=[
            _algorithm_step("g1 first", algorithm="pageRank", graph="g1"),
            _algorithm_step("g2 only", algorithm="louvain", graph="g2"),
            _algorithm_step("g1 last", algorithm="pageRank", graph="g1"),
        ],
    )

    JobExecutor(gds).run(job_config)

    # g1 is projected once, kept alive across the g2 step, and dropped only after its last use.
    assert gds.graph.project.native.call_args_list == [mock.call("g1", **_ALL_FILTERS), mock.call("g2", **_ALL_FILTERS)]
    # g2 is dropped after its single step; g1 survives until the end and is dropped last.
    assert gds.graph.drop.call_args_list == [mock.call("g2"), mock.call("g1")]

    # g1 is not dropped before g2 was even projected: only one g1 projection happens overall.
    names = _call_names(gds)
    assert names.count("graph.project.native") == 2
    assert names.index("graph.drop") > names.index("page_rank.stream")


def test_run_drops_projected_graph_even_if_a_step_fails() -> None:
    gds = _gds_mock()
    gds.page_rank.stream.side_effect = RuntimeError("boom")
    job_config = _job_config(
        projections=[{"native": {"graph": "g1"}}],
        steps=[_algorithm_step("Run PageRank", algorithm="pageRank", graph="g1")],
    )

    with pytest.raises(RuntimeError, match="boom"):
        JobExecutor(gds).run(job_config)

    gds.graph.drop.assert_called_once_with("g1", fail_if_missing=False)


_ALL_FILTERS = {
    "node_label_filter": ["*"],
    "relationship_type_filter": ["*"],
    "node_properties": None,
    "relationship_properties": None,
    "undirected_relationship_types": None,
    "inverse_indexed_relationship_types": None,
}
