from __future__ import annotations

from unittest import mock

import pytest
from pytest_mock import MockerFixture

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.arrow.catalog.graph_sampling_arrow_endpoints import (
    GraphSamplingArrowEndpoints,
)

SAMPLING_MODULE = "graphdatascience.procedure_surface.arrow.catalog.graph_sampling_arrow_endpoints"


def _sample_summary() -> dict[str, object]:
    return {
        "graphName": "sampled",
        "fromGraphName": "g",
        "nodeCount": 2,
        "relationshipCount": 1,
        "startNodeCount": 1,
        "projectMillis": 7,
    }


def _endpoints(mocker: MockerFixture) -> tuple[GraphSamplingArrowEndpoints, mock.Mock]:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)
    mocker.patch(f"{SAMPLING_MODULE}.JobClient.run_job_and_wait", return_value="job-1")
    mocker.patch(f"{SAMPLING_MODULE}.JobClient.get_summary", return_value=_sample_summary())
    mocker.patch(f"{SAMPLING_MODULE}.get_graph", return_value=mocker.Mock())
    endpoints = GraphSamplingArrowEndpoints(arrow_client=arrow_client)
    return endpoints, arrow_client


def test_rwr_overwrite_drops_existing_graph(mocker: MockerFixture) -> None:
    endpoints, _ = _endpoints(mocker)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")
    G = Graph("g", mocker.Mock())

    endpoints.rwr(G, "sampled", overwrite=True)

    drop_spy.assert_called_once_with("sampled", fail_if_missing=False)


def test_rwr_rejects_name_equal_to_source_graph(mocker: MockerFixture) -> None:
    endpoints, _ = _endpoints(mocker)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")
    G = Graph("g", mocker.Mock())

    with pytest.raises(ValueError, match="must not equal the source graph name"):
        endpoints.rwr(G, "g", overwrite=True)

    drop_spy.assert_not_called()


def test_cnarw_rejects_name_equal_to_source_graph(mocker: MockerFixture) -> None:
    endpoints, _ = _endpoints(mocker)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")
    G = Graph("g", mocker.Mock())

    with pytest.raises(ValueError, match="must not equal the source graph name"):
        endpoints.cnarw(G, "g", overwrite=True)

    drop_spy.assert_not_called()


def test_rwr_does_not_drop_by_default(mocker: MockerFixture) -> None:
    endpoints, _ = _endpoints(mocker)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")
    G = Graph("g", mocker.Mock())

    endpoints.rwr(G, "sampled")

    drop_spy.assert_not_called()


def test_cnarw_overwrite_drops_existing_graph(mocker: MockerFixture) -> None:
    endpoints, _ = _endpoints(mocker)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")
    G = Graph("g", mocker.Mock())

    endpoints.cnarw(G, "sampled", overwrite=True)

    drop_spy.assert_called_once_with("sampled", fail_if_missing=False)


def test_cnarw_does_not_drop_by_default(mocker: MockerFixture) -> None:
    endpoints, _ = _endpoints(mocker)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")
    G = Graph("g", mocker.Mock())

    endpoints.cnarw(G, "sampled")

    drop_spy.assert_not_called()
