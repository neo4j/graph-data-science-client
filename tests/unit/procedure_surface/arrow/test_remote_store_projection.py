from __future__ import annotations

import pytest
from pandas import DataFrame
from pytest_mock import MockerFixture

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.catalog.remote_store_projection import (
    RemoteStoreProjection,
    StoreProjectionResult,
)
from graphdatascience.server_version.server_version import ServerVersion
from tests.unit.arrow_client.arrow_test_utils import ArrowTestResult
from tests.unit.conftest import CollectingQueryRunner

DEFAULT_SERVER_VERSION = ServerVersion(2, 10, 0)


@pytest.fixture()
def arrow_client(mocker: MockerFixture) -> AuthenticatedArrowClient:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = iter(
        [
            ArrowTestResult(
                {
                    "graphName": "g",
                    "nodeCount": 10,
                    "relationshipCount": 5,
                }
            )
        ]
    )

    return arrow_client  # type: ignore


def test_run_projection_submits_apoc_job(arrow_client: AuthenticatedArrowClient) -> None:
    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {
            "apoc.periodic.submit": DataFrame([{}]),
            "apoc.periodic.list": DataFrame([{"done": True, "cancelled": False}]),
        },
    )

    projection = RemoteStoreProjection(arrow_client=arrow_client, query_runner=runner)
    result = projection.run_projection(
        graph_name="g",
        node_label_filter=["Person"],
        relationship_type_filter=["KNOWS"],
    )

    submit_query = next(q for q in runner.queries if "apoc.periodic.submit" in q)
    assert submit_query is not None

    assert isinstance(result, StoreProjectionResult)
    assert result.graph_name == "g"
    assert result.node_count == 10
    assert result.relationship_count == 5


def test_run_projection_uses_provided_job_id(arrow_client: AuthenticatedArrowClient) -> None:
    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {
            "apoc.periodic.submit": DataFrame([{}]),
            "apoc.periodic.list": DataFrame([{"done": True, "cancelled": False}]),
        },
    )

    job_id = "my-custom-job-id"
    projection = RemoteStoreProjection(arrow_client=arrow_client, query_runner=runner)
    projection.run_projection(
        graph_name="g",
        node_label_filter=["Person"],
        relationship_type_filter=["KNOWS"],
        job_id=job_id,
    )

    submit_params = next(p for q, p in zip(runner.queries, runner.params) if "apoc.periodic.submit" in q)
    assert submit_params["jobId"] == job_id


def test_run_projection_passes_graph_name_and_filters(arrow_client: AuthenticatedArrowClient) -> None:
    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {
            "apoc.periodic.submit": DataFrame([{}]),
            "apoc.periodic.list": DataFrame([{"done": True, "cancelled": False}]),
        },
    )

    projection = RemoteStoreProjection(arrow_client=arrow_client, query_runner=runner)
    projection.run_projection(
        graph_name="graph",
        node_label_filter=["Person", "City"],
        relationship_type_filter=["LIVES_IN"],
    )

    submit_params = next(p for q, p in zip(runner.queries, runner.params) if "apoc.periodic.submit" in q)
    inner_params = submit_params["params"]
    assert inner_params["graphName"] == "graph"
    assert inner_params["nodeProjection"] == ["Person", "City"]
    assert inner_params["relationshipProjection"] == ["LIVES_IN"]


def test_await_projection_raises_on_cancellation(mocker: MockerFixture) -> None:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)

    runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {
            "apoc.periodic.submit": DataFrame([{}]),
            "apoc.periodic.list": DataFrame([{"done": False, "cancelled": True}]),
        },
    )

    projection = RemoteStoreProjection(arrow_client=arrow_client, query_runner=runner)

    with pytest.raises(RuntimeError, match="cancelled"):
        projection.run_projection(
            graph_name="g",
            node_label_filter=["Person"],
            relationship_type_filter=["KNOWS"],
        )
