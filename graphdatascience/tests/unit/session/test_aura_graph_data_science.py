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
        job_id="test_job",
        concurrency=3,
        batch_size=99,
        undirected_relationship_types=["FOO"],
        inverse_indexed_relationship_types=["BAR"],
    )

    assert g.graph.name() == "foo"
    assert (
        query_runner.last_query()
        == "CALL gds.arrow.project($graph_name, $query, $job_id, $concurrency, $undirected_relationship_types, $inverse_indexed_relationship_types, $arrow_configuration)"
    )
    assert query_runner.last_params() == {
        "graph_name": "foo",
        "query": "RETURN gds.graph.project(0, 1)",
        "job_id": "test_job",
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
        == "CALL gds.arrow.project($graph_name, $query, $job_id, $concurrency, $undirected_relationship_types, $inverse_indexed_relationship_types, $arrow_configuration)"
    )
    assert query_runner.last_params() == {
        "graph_name": "foo",
        "query": "RETURN gds.graph.project(0, 1)",
        "job_id": None,
        "concurrency": 4,
        "undirected_relationship_types": [],
        "inverse_indexed_relationship_types": [],
        "arrow_configuration": {},
    }


def test_remote_algo_write() -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        delete_fn=lambda: True,
        gds_version=v,
    )

    G, _ = gds.graph.project("foo", "RETURN gds.graph.project(0, 1)")
    gds.pageRank.write(G, writeProperty="pr")

    assert query_runner.last_query() == "CALL gds.pageRank.write($graph_name, $config)"
    jobId = query_runner.last_params().get("config", {}).get("jobId", "")
    assert query_runner.last_params() == {
        "graph_name": "foo",
        "config": {"writeProperty": "pr", "jobId": jobId},
    }


def test_remote_algo_write_configuration() -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        delete_fn=lambda: True,
        gds_version=v,
    )

    G, _ = gds.graph.project("foo", "RETURN gds.graph.project(0, 1)")
    gds.pageRank.write(G, writeProperty="pr", concurrency=12, arrowConfiguration={"batch_size": 98})

    assert query_runner.last_query() == "CALL gds.pageRank.write($graph_name, $config)"
    jobId = query_runner.last_params().get("config", {}).get("jobId", "")
    assert query_runner.last_params() == {
        "graph_name": "foo",
        "config": {
            "writeProperty": "pr",
            "concurrency": 12,
            "jobId": jobId,
            "arrowConfiguration": {"batch_size": 98},
        },
    }


def test_remote_graph_write() -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        delete_fn=lambda: True,
        gds_version=v,
    )

    G, _ = gds.graph.project("foo", "RETURN gds.graph.project(0, 1)")
    gds.graph.nodeProperties.write(G, node_properties="pr")

    assert (
        query_runner.last_query() == "CALL gds.graph.nodeProperties.write($graph_name, $properties, $entities, $config)"
    )
    assert query_runner.last_params() == {
        "graph_name": "foo",
        "properties": "pr",
        "entities": ["*"],
        "config": {},
    }


def test_remote_graph_write_configuration() -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        delete_fn=lambda: True,
        gds_version=v,
    )

    G, _ = gds.graph.project("foo", "RETURN gds.graph.project(0, 1)")
    gds.graph.nodeProperties.write(G, node_properties="pr", concurrency=13, arrowConfiguration={"batchSize": 99})

    assert (
        query_runner.last_query() == "CALL gds.graph.nodeProperties.write($graph_name, $properties, $entities, $config)"
    )
    assert query_runner.last_params() == {
        "graph_name": "foo",
        "properties": "pr",
        "entities": ["*"],
        "config": {
            "concurrency": 13,
            "arrowConfiguration": {"batchSize": 99},
        },
    }
