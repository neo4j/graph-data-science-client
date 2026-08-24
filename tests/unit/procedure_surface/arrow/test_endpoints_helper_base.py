from typing import Any
from unittest import mock

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.write_job_handle import WriteBackResult
from graphdatascience.procedure_surface.arrow.endpoints_helper_base import EndpointsHelperBase
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol


@pytest.fixture
def helper() -> EndpointsHelperBase:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    write_protocol = mock.Mock(spec=WriteProtocol)
    return EndpointsHelperBase(arrow_client, write_protocol=write_protocol, show_progress=False)


def _make_write_result(node_props: int = 5) -> WriteBackResult:
    return WriteBackResult(
        writtenNodeProperties=node_props,
        writtenNodeLabels=0,
        writtenRelationships=0,
        writeMillis=10,
        status="COMPLETED",
        progress=1.0,
    )


def test_run_job_and_write_uses_nodePropertiesWritten_key(helper: EndpointsHelperBase) -> None:
    graph = mock.Mock()
    graph.name.return_value = "g"

    fake_summary: dict[str, Any] = {
        "computeMillis": 1,
        "configuration": {},
    }

    with (
        mock.patch(
            "graphdatascience.procedure_surface.arrow.endpoints_helper_base.JobClient.run_job_and_wait",
            return_value="job-1",
        ),
        mock.patch(
            "graphdatascience.procedure_surface.arrow.endpoints_helper_base.JobClient.get_summary",
            return_value=fake_summary,
        ),
        mock.patch(
            "graphdatascience.procedure_surface.arrow.endpoints_helper_base.WriteJobHandle.create"
        ) as mock_create,
    ):
        mock_handle = mock_create.return_value
        mock_handle.result.return_value = _make_write_result(node_props=7)

        result = helper._run_job_and_write(
            "v2/centrality.pageRank",
            graph,
            {"graphName": "g"},
            property_overwrites="score",
            write_concurrency=4,
            concurrency=None,
        )

    assert "nodePropertiesWritten" in result
    assert result["nodePropertiesWritten"] == 7
    assert "propertiesWritten" not in result


def test_run_job_and_write_relationship_property_does_not_set_nodePropertiesWritten(
    helper: EndpointsHelperBase,
) -> None:
    graph = mock.Mock()
    graph.name.return_value = "g"

    fake_summary: dict[str, Any] = {
        "computeMillis": 1,
        "configuration": {},
    }

    with (
        mock.patch(
            "graphdatascience.procedure_surface.arrow.endpoints_helper_base.JobClient.run_job_and_wait",
            return_value="job-1",
        ),
        mock.patch(
            "graphdatascience.procedure_surface.arrow.endpoints_helper_base.JobClient.get_summary",
            return_value=fake_summary,
        ),
        mock.patch(
            "graphdatascience.procedure_surface.arrow.endpoints_helper_base.WriteJobHandle.create"
        ) as mock_create,
    ):
        mock_handle = mock_create.return_value
        mock_handle.result.return_value = WriteBackResult(
            writtenNodeProperties=0,
            writtenNodeLabels=0,
            writtenRelationships=3,
            writeMillis=10,
            status="COMPLETED",
            progress=1.0,
        )

        result = helper._run_job_and_write(
            "v2/pathfinding.maxFlow",
            graph,
            {"graphName": "g"},
            relationship_type_overwrite="FLOW",
            property_overwrites="cost",
            write_concurrency=4,
            concurrency=None,
        )

    assert "relationshipsWritten" in result
    assert result["relationshipsWritten"] == 3
    assert "nodePropertiesWritten" not in result
    assert "propertiesWritten" not in result
