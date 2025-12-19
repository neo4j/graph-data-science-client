from pytest_mock import MockerFixture

from graphdatascience import ServerVersion
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.session_lifecycle_manager import SessionLifecycleManager
from graphdatascience.session.session_v2_endpoints import SessionV2Endpoints
from tests.unit.conftest import CollectingQueryRunner


def test_remote_projection_configuration(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        session_lifecycle_manager=mocker.Mock(spec=SessionLifecycleManager),
        gds_version=v,
        v2_endpoints=mocker.Mock(),
        authenticated_arrow_client=mocker.Mock(),
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


def test_remote_projection_defaults(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        session_lifecycle_manager=mocker.Mock(spec=SessionLifecycleManager),
        gds_version=v,
        v2_endpoints=mocker.Mock(),
        authenticated_arrow_client=mocker.Mock(),
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


def test_remote_algo_write(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        session_lifecycle_manager=mocker.Mock(spec=SessionLifecycleManager),
        gds_version=v,
        v2_endpoints=mocker.Mock(),
        authenticated_arrow_client=mocker.Mock(),
    )

    G, _ = gds.graph.project("foo", "RETURN gds.graph.project(0, 1)")
    gds.pageRank.write(G, writeProperty="pr")

    assert query_runner.last_query() == "CALL gds.pageRank.write($graph_name, $config)"
    jobId = query_runner.last_params().get("config", {}).get("jobId", "")
    assert query_runner.last_params() == {
        "graph_name": "foo",
        "config": {"writeProperty": "pr", "jobId": jobId},
    }


def test_remote_algo_write_configuration(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        session_lifecycle_manager=mocker.Mock(spec=SessionLifecycleManager),
        gds_version=v,
        v2_endpoints=mocker.Mock(),
        authenticated_arrow_client=mocker.Mock(),
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


def test_remote_graph_write(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        session_lifecycle_manager=mocker.Mock(spec=SessionLifecycleManager),
        gds_version=v,
        v2_endpoints=mocker.Mock(),
        authenticated_arrow_client=mocker.Mock(),
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


def test_remote_graph_write_configuration(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        session_lifecycle_manager=mocker.Mock(spec=SessionLifecycleManager),
        gds_version=v,
        v2_endpoints=mocker.Mock(),
        authenticated_arrow_client=mocker.Mock(),
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


def test_run_cypher_write(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        session_lifecycle_manager=mocker.Mock(spec=SessionLifecycleManager),
        gds_version=v,
        v2_endpoints=mocker.Mock(),
        authenticated_arrow_client=mocker.Mock(),
    )

    gds.run_cypher("RETURN 1", params={"foo": 1}, mode=QueryMode.WRITE, database="bar", retryable=True)

    assert query_runner.last_query() == "RETURN 1"
    assert query_runner.last_params() == {"foo": 1}
    assert query_runner.run_args[-1] == {"custom_error": False, "db": "bar", "mode": QueryMode.WRITE, "retryable": True}


def test_run_cypher_read(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        session_lifecycle_manager=mocker.Mock(spec=SessionLifecycleManager),
        gds_version=v,
        v2_endpoints=mocker.Mock(),
        authenticated_arrow_client=mocker.Mock(),
    )

    gds.run_cypher("RETURN 1", params={"foo": 1}, mode=QueryMode.READ, retryable=False)

    assert query_runner.last_query() == "RETURN 1"
    assert query_runner.last_params() == {"foo": 1}
    assert query_runner.run_args[-1] == {
        "custom_error": False,
        "db": None,
        "mode": QueryMode.READ,
        "retryable": False,
    }


def test_verify_connectivity(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = mocker.Mock(spec=Neo4jQueryRunner)
    session_lifecycle_manager = mocker.Mock(spec=SessionLifecycleManager)
    v2_endpoints = mocker.Mock(spec=SessionV2Endpoints)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        session_lifecycle_manager=session_lifecycle_manager,
        gds_version=v,
        v2_endpoints=v2_endpoints,
        authenticated_arrow_client=mocker.Mock(),
    )

    gds.verify_connectivity()

    session_lifecycle_manager.verify_health.assert_called_once()
    v2_endpoints.verify_session_connectivity.assert_called_once()
    v2_endpoints.verify_db_connectivity.assert_called_once()


def test_delete(mocker: MockerFixture) -> None:
    v = ServerVersion(9, 9, 9)
    query_runner = CollectingQueryRunner(v)
    session_lifecycle_manager = mocker.Mock(spec=SessionLifecycleManager)
    gds = AuraGraphDataScience(
        query_runner=query_runner,
        session_lifecycle_manager=session_lifecycle_manager,
        gds_version=v,
        v2_endpoints=mocker.Mock(),
        authenticated_arrow_client=mocker.Mock(),
    )

    gds.delete()

    session_lifecycle_manager.delete.assert_called_once()
