from unittest import mock

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.jobs_arrow_endpoints import JobsArrowEndpoints


def _endpoints() -> JobsArrowEndpoints:
    return JobsArrowEndpoints(mock.Mock(spec=AuthenticatedArrowClient))


def test_is_projection_matches_job_named_after_graph() -> None:
    assert _endpoints()._is_projection("some-graph", "some-graph")


def test_is_projection_matches_projection_endpoint_names() -> None:
    endpoints = _endpoints()

    for job_name in [
        "v2/graph.project.fromTables",
        "v2/graph.project.fromTriplets",
        "v2/graph.project.filter",
        "v2/graph.sample.rwr",
        "v2/graph.sample.cnarw",
        "v2/graph.generate",
        "v2/graph.create.fromTriplets",
    ]:
        assert endpoints._is_projection(job_name, "some-graph")


def test_is_projection_rejects_other_jobs() -> None:
    assert not _endpoints()._is_projection("v2/centrality.pageRank", "some-graph")
