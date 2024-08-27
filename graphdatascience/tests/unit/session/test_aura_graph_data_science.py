from graphdatascience import ServerVersion
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.tests.unit.conftest import CollectingQueryRunner


def test_remote_projection_configuration() -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        delete_fn=lambda: True,
        gds_version=v,
    )

    g = gds.graph.project(
        "foo",
        "RETURN gds.graph.project(0, 1)",
        concurrency=3,
        batch_size=99,
        undirected_relationship_types=["FOO"],
        inverse_indexed_relationship_types=["BAR"],
    )

    assert g.graph.name() == "foo"
    assert (
        query_runner.last_query()
        == "CALL gds.arrow.project($graph_name, $query, $concurrency, $undirected_relationship_types, $inverse_indexed_relationship_types, $arrow_configuration)"
    )
    assert query_runner.last_params() == {
        "graph_name": "foo",
        "query": "RETURN gds.graph.project(0, 1)",
        "concurrency": 3,
        "undirected_relationship_types": ["FOO"],
        "inverse_indexed_relationship_types": ["BAR"],
        "arrow_configuration": {"batchSize": 99},
    }


def test_remote_projection_defaults() -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        delete_fn=lambda: True,
        gds_version=v,
    )

    g = gds.graph.project("foo", "RETURN gds.graph.project(0, 1)")

    assert g.graph.name() == "foo"
    assert (
        query_runner.last_query()
        == "CALL gds.arrow.project($graph_name, $query, $concurrency, $undirected_relationship_types, $inverse_indexed_relationship_types, $arrow_configuration)"
    )
    assert query_runner.last_params() == {
        "graph_name": "foo",
        "query": "RETURN gds.graph.project(0, 1)",
        "concurrency": 4,
        "undirected_relationship_types": [],
        "inverse_indexed_relationship_types": [],
        "arrow_configuration": {},
    }
