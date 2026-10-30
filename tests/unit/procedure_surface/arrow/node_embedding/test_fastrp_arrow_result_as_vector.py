from typing import Any
from unittest import mock

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.node_embedding.fastrp_arrow_endpoints import FastRPArrowEndpoints


def _graph() -> mock.Mock:
    G = mock.Mock()
    G.name.return_value = "g"
    return G


def _compute_config(result_as_vector: bool) -> dict[str, Any]:
    endpoints = FastRPArrowEndpoints(arrow_client=mock.Mock(spec=AuthenticatedArrowClient), write_protocol=None)

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.endpoints_helper_base.JobClient.run_job",
        return_value="job-123",
    ) as run_job:
        endpoints.compute(G=_graph(), embedding_dimension=8, result_as_vector=result_as_vector)

    config: dict[str, Any] = run_job.call_args.args[2]
    return config


def test_compute_sends_result_as_vector_when_enabled() -> None:
    assert _compute_config(result_as_vector=True)["resultAsVector"] is True


def test_compute_sends_result_as_vector_when_disabled() -> None:
    # GDS Sessions support the key, so it is always sent.
    assert _compute_config(result_as_vector=False)["resultAsVector"] is False
