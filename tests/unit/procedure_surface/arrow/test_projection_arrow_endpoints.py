from unittest import mock

from pytest_mock import MockerFixture

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.catalog.projection_arrow_endpoints import ProjectArrowEndpoints
from graphdatascience.session.dbms.protocol_version import ProtocolVersion

PROJECTION_MODULE = "graphdatascience.procedure_surface.arrow.catalog.projection_arrow_endpoints"


def _endpoints_with_mocked_projection(
    mocker: MockerFixture, show_progress: bool
) -> tuple[ProjectArrowEndpoints, mock.Mock]:
    """A ProjectArrowEndpoints wired up to run cypher() without any real network/protocol calls,
    with `ProjectionRunner` mocked so the test can inspect what `logging` value it was given."""
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)
    query_runner = mocker.Mock()

    mocker.patch(f"{PROJECTION_MODULE}.ProtocolVersionResolver").return_value.resolve.return_value = ProtocolVersion.V4
    mocker.patch(f"{PROJECTION_MODULE}.ProjectProtocol")
    runner_cls = mocker.patch(f"{PROJECTION_MODULE}.ProjectionRunner")
    mocker.patch(
        f"{PROJECTION_MODULE}.JobClient.get_summary",
        return_value={
            "graph_name": "g",
            "node_count": 1,
            "relationship_count": 1,
            "project_millis": 1,
            "configuration": {},
            "query": "MATCH (n) RETURN gds.graph.project.remote(n, n)",
        },
    )
    mocker.patch(f"{PROJECTION_MODULE}.get_graph", return_value=mocker.Mock())

    endpoints = ProjectArrowEndpoints(arrow_client=arrow_client, query_runner=query_runner, show_progress=show_progress)
    return endpoints, runner_cls.return_value


def test_cypher_logs_when_show_progress_enabled(mocker: MockerFixture) -> None:
    endpoints, runner = _endpoints_with_mocked_projection(mocker, show_progress=True)

    endpoints.cypher("g", "MATCH (n) RETURN gds.graph.project.remote(n, n)")

    logging_arg = runner.run_cypher_projection.call_args.args[-1]
    assert logging_arg is True


def test_cypher_does_not_log_when_show_progress_disabled(mocker: MockerFixture) -> None:
    endpoints, runner = _endpoints_with_mocked_projection(mocker, show_progress=False)

    endpoints.cypher("g", "MATCH (n) RETURN gds.graph.project.remote(n, n)")

    logging_arg = runner.run_cypher_projection.call_args.args[-1]
    assert logging_arg is False


def test_cypher_logging_false_disables_logging(mocker: MockerFixture) -> None:
    endpoints, runner = _endpoints_with_mocked_projection(mocker, show_progress=True)

    endpoints.cypher("g", "MATCH (n) RETURN gds.graph.project.remote(n, n)", logging=False)

    logging_arg = runner.run_cypher_projection.call_args.args[-1]
    assert logging_arg is False


def test_native_overwrite_drops_existing_graph(mocker: MockerFixture) -> None:
    endpoints, _ = _endpoints_with_mocked_projection(mocker, show_progress=False)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")

    endpoints.native("g", ["A"], ["REL"], overwrite=True)

    drop_spy.assert_called_once_with("g", fail_if_missing=False)


def test_native_does_not_drop_by_default(mocker: MockerFixture) -> None:
    endpoints, _ = _endpoints_with_mocked_projection(mocker, show_progress=False)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")

    endpoints.native("g", ["A"], ["REL"])

    drop_spy.assert_not_called()


def test_native_async_overwrite_drops_existing_graph(mocker: MockerFixture) -> None:
    endpoints, _ = _endpoints_with_mocked_projection(mocker, show_progress=False)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")
    mocker.patch.object(
        endpoints._project_protocol,
        "start_store_projection",
        return_value=("job-1", mocker.Mock()),
    )

    endpoints.native_async("g", ["A"], ["REL"], overwrite=True)

    drop_spy.assert_called_once_with("g", fail_if_missing=False)


def test_cypher_overwrite_drops_existing_graph(mocker: MockerFixture) -> None:
    endpoints, _ = _endpoints_with_mocked_projection(mocker, show_progress=False)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")

    endpoints.cypher("g", "MATCH (n) RETURN gds.graph.project.remote(n, n)", overwrite=True)

    drop_spy.assert_called_once_with("g", fail_if_missing=False)


def test_cypher_does_not_drop_by_default(mocker: MockerFixture) -> None:
    endpoints, _ = _endpoints_with_mocked_projection(mocker, show_progress=False)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")

    endpoints.cypher("g", "MATCH (n) RETURN gds.graph.project.remote(n, n)")

    drop_spy.assert_not_called()


def test_cypher_async_overwrite_drops_existing_graph(mocker: MockerFixture) -> None:
    endpoints, _ = _endpoints_with_mocked_projection(mocker, show_progress=False)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")
    mocker.patch.object(
        endpoints._project_protocol,
        "start_cypher_projection",
        return_value=("job-1", mocker.Mock()),
    )

    endpoints.cypher_async("g", "MATCH (n) RETURN gds.graph.project.remote(n, n)", overwrite=True)

    drop_spy.assert_called_once_with("g", fail_if_missing=False)
