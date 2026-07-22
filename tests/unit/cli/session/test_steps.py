from unittest.mock import MagicMock

from gds_cli.session.config import (
    AlgorithmConfig,
    JobConfig,
    ProjectionConfig,
    SessionConfig,
    WritebackConfig,
)
from gds_cli.session.steps import drop_all, project_all, run_algorithms, run_all, run_writebacks


def _session() -> SessionConfig:
    return SessionConfig(name="gds-examples", memory="2GB", ttl_minutes=30)


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


def test_run_all_groups_interleaved_algorithms_by_graph() -> None:
    gds = _mock_gds()
    cfg = JobConfig(
        session=_session(),
        projections=[
            ProjectionConfig(graph_name="social", query="q1"),
            ProjectionConfig(graph_name="pages", query="q2"),
        ],
        algorithms=[
            AlgorithmConfig(name="louvain", graph_name="social", mode="mutate", mutate_property="community"),
            AlgorithmConfig(name="pageRank", graph_name="pages", mode="mutate", mutate_property="pagerank"),
            AlgorithmConfig(name="wcc", graph_name="social", mode="mutate", mutate_property="componentId"),
        ],
        writebacks=[
            WritebackConfig(graph_name="social", node_properties=["community", "componentId"]),
            WritebackConfig(graph_name="pages", node_properties=["pagerank"]),
        ],
    )

    result = run_all(gds, cfg)

    # `social`'s two algorithms run together - projected and dropped exactly
    # once each - even though `pages` was interleaved between them in the
    # source list.
    assert gds.graph.project.cypher.call_args_list == [
        (("social", "q1"), {"undirected_relationship_types": None}),
        (("pages", "q2"), {"undirected_relationship_types": None}),
    ]
    assert gds.graph.drop.call_args_list == [
        (("social",), {"fail_if_missing": False}),
        (("pages",), {"fail_if_missing": False}),
    ]
    assert [a["name"] for a in result["algorithms"]] == ["louvain", "wcc", "pageRank"]
    assert [a["graph_name"] for a in result["algorithms"]] == ["social", "social", "pages"]
    assert [w["graph_name"] for w in result["writebacks"]] == ["social", "pages"]


def test_run_all_writeback_happens_before_drop_on_the_projected_graph() -> None:
    gds = _mock_gds()
    cfg = JobConfig(
        session=_session(),
        projections=[ProjectionConfig(graph_name="social", query="q1")],
        algorithms=[AlgorithmConfig(name="louvain", graph_name="social", mode="mutate", mutate_property="community")],
        writebacks=[WritebackConfig(graph_name="social", node_properties=["community"])],
    )

    run_all(gds, cfg)

    assert gds.graph.project.cypher.call_count == 1
    gds.graph.node_properties.write.assert_called_once()
    written_graph = gds.graph.node_properties.write.call_args.args[0]
    assert written_graph.name() == "social"
    gds.graph.drop.assert_called_once_with("social", fail_if_missing=False)


def test_run_all_overwrite_graph_drops_before_projecting() -> None:
    gds = _mock_gds()
    cfg = JobConfig(
        session=_session(),
        projections=[ProjectionConfig(graph_name="social", query="q1")],
        algorithms=[AlgorithmConfig(name="louvain", graph_name="social", mode="mutate", mutate_property="community")],
    )

    run_all(gds, cfg, overwrite_graph=True)

    assert gds.graph.drop.call_args_list[0] == (("social",), {"fail_if_missing": False})
    assert gds.graph.project.cypher.call_args_list == [(("social", "q1"), {"undirected_relationship_types": None})]


def test_run_all_native_projection_calls_project_native() -> None:
    gds = _mock_gds()
    cfg = JobConfig(
        session=_session(),
        projections=[
            ProjectionConfig(
                graph_name="social",
                node_labels=["Person"],
                relationship_types=["KNOWS"],
                node_properties=["age"],
                undirected_relationship_types=["KNOWS"],
            )
        ],
        algorithms=[AlgorithmConfig(name="wcc", graph_name="social", mode="mutate", mutate_property="componentId")],
    )

    run_all(gds, cfg)

    # native projection -> project.native, never the remote project.cypher()
    gds.graph.project.cypher.assert_not_called()
    gds.graph.project.native.assert_called_once_with(
        "social",
        node_label_filter=["Person"],
        relationship_type_filter=["KNOWS"],
        node_properties=["age"],
        relationship_properties=None,
        undirected_relationship_types=["KNOWS"],
    )


def test_project_all_projects_every_graph() -> None:
    gds = _mock_gds()
    cfg = JobConfig(
        session=_session(),
        projections=[
            ProjectionConfig(graph_name="social", query="q1"),
            ProjectionConfig(graph_name="pages", query="q2"),
        ],
        algorithms=[AlgorithmConfig(name="louvain", graph_name="social", mode="mutate", mutate_property="community")],
    )

    graphs = project_all(gds, cfg)

    assert [g.name() for g in graphs] == ["social", "pages"]


def test_run_algorithms_only_filters_by_name() -> None:
    gds = _mock_gds()
    cfg = JobConfig(
        session=_session(),
        projections=[ProjectionConfig(graph_name="social", query="q1")],
        algorithms=[
            AlgorithmConfig(name="louvain", graph_name="social", mode="mutate", mutate_property="community"),
            AlgorithmConfig(name="wcc", graph_name="social", mode="mutate", mutate_property="componentId"),
        ],
    )

    results = run_algorithms(gds, cfg, only="wcc")

    assert [r["name"] for r in results] == ["wcc"]
    gds.graph.get.assert_called_once_with("social")


def test_run_writebacks_writes_every_configured_graph() -> None:
    gds = _mock_gds()
    cfg = JobConfig(
        session=_session(),
        projections=[
            ProjectionConfig(graph_name="social", query="q1"),
            ProjectionConfig(graph_name="pages", query="q2"),
        ],
        algorithms=[
            AlgorithmConfig(name="louvain", graph_name="social", mode="mutate", mutate_property="community"),
            AlgorithmConfig(name="pageRank", graph_name="pages", mode="mutate", mutate_property="pagerank"),
        ],
        writebacks=[
            WritebackConfig(graph_name="social", node_properties=["community"]),
            WritebackConfig(graph_name="pages", node_properties=["pagerank"]),
        ],
    )

    results = run_writebacks(gds, cfg)

    assert [r["graph_name"] for r in results] == ["social", "pages"]
    assert gds.graph.node_properties.write.call_count == 2


def test_drop_all_drops_every_projected_graph() -> None:
    gds = _mock_gds()
    cfg = JobConfig(
        session=_session(),
        projections=[
            ProjectionConfig(graph_name="social", query="q1"),
            ProjectionConfig(graph_name="pages", query="q2"),
        ],
        algorithms=[AlgorithmConfig(name="louvain", graph_name="social", mode="mutate", mutate_property="community")],
    )

    drop_all(gds, cfg)

    assert gds.graph.drop.call_args_list == [
        (("social",), {"fail_if_missing": False}),
        (("pages",), {"fail_if_missing": False}),
    ]
