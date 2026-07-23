import json
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
from gds_cli.session.config import (
    ComputeSpec,
    JobsConfig,
    JobSpec,
    MutateSpec,
    ProjectSpec,
    SessionConfig,
    WriteSpec,
)
from gds_cli.session.steps import job_graph_name, run_all


def _session() -> SessionConfig:
    return SessionConfig(memory="2GB", ttl="30m")


def _named_graph(graph_name: str) -> MagicMock:
    graph = MagicMock()
    graph.name.return_value = graph_name
    graph.node_count.return_value = 10
    graph.relationship_count.return_value = 20
    return graph


def _mock_gds() -> MagicMock:
    gds = MagicMock()
    gds.graph.project.cypher.side_effect = lambda graph_name, query, undirected_relationship_types=None: MagicMock(
        graph=_named_graph(graph_name)
    )
    gds.graph.project.native.side_effect = (
        lambda graph_name, node_label_filter, relationship_type_filter, node_properties=None, relationship_properties=None, undirected_relationship_types=None: (
            MagicMock(  # noqa: E501
                graph=_named_graph(graph_name)
            )
        )
    )
    gds.graph.get.side_effect = lambda graph_name: _named_graph(graph_name)
    return gds


def _cypher(query: str = "q") -> ProjectSpec:
    return ProjectSpec(type="cypher", query=query)


def test_run_all_projects_computes_writes_drops_each_job() -> None:
    gds = _mock_gds()
    cfg = JobsConfig(
        session=_session(),
        jobs=[
            JobSpec(
                project=_cypher("q1"),
                compute=[ComputeSpec(compute="louvain", config={"resultProperty": "community"})],
                write=[WriteSpec(node_property="community")],
            ),
            JobSpec(
                project=_cypher("q2"),
                compute=[ComputeSpec(compute="pageRank", config={"resultProperty": "pagerank"})],
                write=[WriteSpec(node_property="pagerank")],
            ),
        ],
    )

    result = run_all(gds, cfg)

    # Each job gets its own internal graph name, projected and dropped once.
    assert gds.graph.project.cypher.call_args_list == [
        (("job-0", "q1"), {"undirected_relationship_types": None}),
        (("job-1", "q2"), {"undirected_relationship_types": None}),
    ]
    assert gds.graph.drop.call_args_list == [
        (("job-0",), {"fail_if_missing": False}),
        (("job-1",), {"fail_if_missing": False}),
    ]
    assert [c["algorithm"] for c in result["computes"]] == ["louvain", "pageRank"]
    assert [c["graph_name"] for c in result["computes"]] == ["job-0", "job-1"]
    assert [w["node_property"] for w in result["writes"]] == ["community", "pagerank"]


def test_write_of_mutated_property_uses_node_properties_write() -> None:
    gds = _mock_gds()
    cfg = JobsConfig(
        session=_session(),
        jobs=[
            JobSpec(
                project=_cypher(),
                compute=[ComputeSpec(compute="louvain", config={"resultProperty": "community"})],
                mutate=[MutateSpec(node_property="community")],
                write=[WriteSpec(node_property="community", write_property="community_id")],
            )
        ],
    )

    run_all(gds, cfg)

    # mutated -> materialized via handle.mutate, written back via graph.node_properties.write (with rename).
    gds.louvain.compute.return_value.mutate.assert_called_once_with(mutate_property="community")
    gds.louvain.compute.return_value.write.assert_not_called()
    gds.graph.node_properties.write.assert_called_once()
    args = gds.graph.node_properties.write.call_args.args
    assert args[0].name() == "job-0"
    assert args[1] == {"community": "community_id"}


def test_write_of_computed_only_property_uses_direct_handle_write() -> None:
    gds = _mock_gds()
    cfg = JobsConfig(
        session=_session(),
        jobs=[
            JobSpec(
                project=_cypher(),
                compute=[ComputeSpec(compute="louvain", config={"resultProperty": "community"})],
                # no mutate -> direct write, skipping the mutate step
                write=[WriteSpec(node_property="community")],
            )
        ],
    )

    run_all(gds, cfg)

    handle = gds.louvain.compute.return_value
    handle.mutate.assert_not_called()
    handle.write.assert_called_once_with(write_properties={"community": "community"})
    gds.graph.node_properties.write.assert_not_called()
    # Regression: the compute job must be waited on *before* the direct write, so
    # its result is in the session result store when the remote write-back fetches
    # it (otherwise: "No entry with job id ... was found in result store").
    handle.wait.assert_called_once()
    call_names = [c[0] for c in handle.mock_calls]
    assert call_names.index("wait") < call_names.index("write")


def test_compute_step_waits_for_the_job_to_finish() -> None:
    gds = _mock_gds()
    cfg = JobsConfig(
        session=_session(),
        jobs=[
            JobSpec(
                project=_cypher(),
                # compute only: nothing downstream would otherwise wait on the job
                compute=[ComputeSpec(compute="louvain", config={"resultProperty": "community"})],
            )
        ],
    )

    run_all(gds, cfg)

    handle = gds.louvain.compute.return_value
    # the "Running" step must block on completion even when there's no mutate/write
    handle.wait.assert_called_once()
    handle.mutate.assert_not_called()
    handle.write.assert_not_called()


def test_compute_params_collapsed_to_snake_case() -> None:
    gds = _mock_gds()
    cfg = JobsConfig(
        session=_session(),
        jobs=[
            JobSpec(
                project=_cypher(),
                compute=[ComputeSpec(compute="pageRank", config={"resultProperty": "pagerank", "maxIterations": 20})],
                write=[WriteSpec(node_property="pagerank")],
            )
        ],
    )

    run_all(gds, cfg)

    gds.page_rank.compute.assert_called_once()
    _, kwargs = gds.page_rank.compute.call_args
    assert kwargs == {"max_iterations": 20}


def test_downstream_feature_auto_mutates_producer_before_dependent_compute() -> None:
    gds = _mock_gds()
    # No explicit `mutate`: pagerank is auto-materialized because fastRP lists it in
    # featureProperties.
    cfg = JobsConfig(
        session=_session(),
        jobs=[
            JobSpec(
                project=_cypher(),
                compute=[
                    ComputeSpec(compute="pageRank", config={"resultProperty": "pagerank"}),
                    ComputeSpec(
                        compute="fastRP",
                        config={"resultProperty": "fastRP", "featureProperties": ["pagerank"]},
                    ),
                ],
                write=[WriteSpec(node_property="fastRP")],
            )
        ],
    )

    run_all(gds, cfg)

    # pagerank must be mutated into the graph *before* fastRP is computed (it reads it as a feature).
    names = [str(c) for c in gds.mock_calls]
    pagerank_mutate = next(i for i, c in enumerate(names) if "page_rank.compute().mutate(" in c)
    fastrp_compute = next(i for i, c in enumerate(names) if "fast_rp.compute(" in c)
    assert pagerank_mutate < fastrp_compute


def test_standalone_construct_job_builds_from_file_and_streams_grouped(tmp_path: Path) -> None:
    graph_file = tmp_path / "g.json"
    graph_file.write_text(
        json.dumps(
            {
                "nodes": [[{"nodeId": 1, "labels": "Person"}, {"nodeId": 2, "labels": "Person"}]],
                "relationships": [[{"sourceNodeId": 1, "targetNodeId": 2, "relationshipType": "KNOWS"}]],
            }
        )
    )
    gds = _mock_gds()
    gds.graph.construct.side_effect = lambda name, nodes, relationships, undirected_relationship_types=None: (
        _named_graph(name)
    )
    gds.graph.node_properties.stream.return_value = pd.DataFrame(
        {"nodeId": [1, 2], "pagerank": [0.6, 0.4], "community": [0, 1]}
    )

    cfg = JobsConfig(
        session=SessionConfig(memory="2GB", ttl="30m", cloud="gcp", region="europe-west1"),
        jobs=[
            JobSpec(
                project=ProjectSpec(type="construct", file="g.json"),  # relative to base_dir
                compute=[
                    ComputeSpec(compute="pageRank", config={"resultProperty": "pagerank"}),
                    ComputeSpec(compute="louvain", config={"resultProperty": "community"}),
                ],
                # both share one output file -> streamed together, once
                write=[
                    WriteSpec(node_property="pagerank", output_file="result.csv"),
                    WriteSpec(node_property="community", output_file="result.csv"),
                ],
            )
        ],
    )

    run_all(gds, cfg, base_dir=str(tmp_path))

    # graph built from the file via construct (not a DB projection), file resolved vs base_dir
    gds.graph.construct.assert_called_once()
    gds.graph.project.cypher.assert_not_called()
    gds.graph.project.native.assert_not_called()
    # standalone: no DB write-back; the two properties streamed together (one call, one file)
    gds.graph.node_properties.write.assert_not_called()
    gds.graph.node_properties.stream.assert_called_once()
    assert gds.graph.node_properties.stream.call_args.args[1] == ["pagerank", "community"]
    out = tmp_path / "result.csv"
    assert out.exists()
    header = out.read_text().splitlines()[0]
    assert "pagerank" in header and "community" in header


def test_standalone_streams_json_by_extension(tmp_path: Path) -> None:
    (tmp_path / "g.json").write_text(json.dumps({"nodes": [[{"nodeId": 1, "labels": "Person"}]], "relationships": []}))
    gds = _mock_gds()
    gds.graph.construct.side_effect = lambda name, nodes, relationships, undirected_relationship_types=None: (
        _named_graph(name)
    )
    gds.graph.node_properties.stream.return_value = pd.DataFrame({"nodeId": [1], "pagerank": [1.0]})

    cfg = JobsConfig(
        session=SessionConfig(memory="2GB", ttl="30m", cloud="gcp", region="europe-west1"),
        jobs=[
            JobSpec(
                project=ProjectSpec(type="construct", file="g.json"),
                compute=[ComputeSpec(compute="pageRank", config={"resultProperty": "pagerank"})],
                write=[WriteSpec(node_property="pagerank", output_file="result.json")],
            )
        ],
    )

    run_all(gds, cfg, base_dir=str(tmp_path))

    out = tmp_path / "result.json"
    assert out.exists()
    # mirrors the construct input file: rows under a `computedNodeProperties` section
    assert json.loads(out.read_text()) == {"computedNodeProperties": [{"nodeId": 1, "pagerank": 1.0}]}


def test_job_graph_name_is_index_based() -> None:
    assert job_graph_name(0) == "job-0"
    assert job_graph_name(3) == "job-3"
