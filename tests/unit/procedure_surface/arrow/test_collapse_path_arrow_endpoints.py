from unittest import mock

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.catalog.relationship_arrow_endpoints import RelationshipArrowEndpoints
from graphdatascience.procedure_surface.arrow.collapse_path_arrow_endpoints import CollapsePathArrowEndpoints
from graphdatascience.session.session_v2_endpoints import SessionV2Endpoints


def test_session_exposes_top_level_collapse_path() -> None:
    endpoints = SessionV2Endpoints(
        mock.Mock(spec=AuthenticatedArrowClient),
        db_client=None,
        show_progress=False,
    )

    assert isinstance(endpoints.collapse_path, CollapsePathArrowEndpoints)


def test_collapse_path_mutate_runs_arrow_job() -> None:
    graph = mock.Mock()
    graph.name.return_value = "g"
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)

    with (
        mock.patch(
            "graphdatascience.procedure_surface.arrow.collapse_path_arrow_endpoints.JobClient.run_job_and_wait",
            return_value="job-1",
        ) as run_job_and_wait,
        mock.patch(
            "graphdatascience.procedure_surface.arrow.collapse_path_arrow_endpoints.JobClient.get_summary",
            return_value={
                "preProcessingMillis": 1,
                "computeMillis": 2,
                "mutateMillis": 3,
                "relationshipsWritten": 4,
                "configuration": {"jobId": "job-1"},
            },
        ) as get_summary,
    ):
        result = CollapsePathArrowEndpoints(arrow_client, show_progress=True).mutate(
            G=graph,
            path_templates=[["REL", "REL"]],
            mutate_relationship_type="FoF",
            allow_self_loops=True,
            concurrency=4,
            job_id="job-1",
            sudo=True,
            log_progress=False,
            username="neo4j",
        )

    assert result.relationshipsWritten == 4
    run_job_and_wait.assert_called_once_with(
        arrow_client,
        "v2/graph.relationships.collapsePath",
        {
            "graphName": "g",
            "pathTemplates": [["REL", "REL"]],
            "mutateRelationshipType": "FoF",
            "allowSelfLoops": True,
            "concurrency": 4,
            "jobId": "job-1",
            "sudo": True,
            "logProgress": False,
            "username": "neo4j",
        },
        show_progress=False,
    )
    get_summary.assert_called_once_with(arrow_client, "job-1")


def test_relationship_collapse_path_delegates_to_dedicated_endpoint() -> None:
    graph = mock.Mock()

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.catalog.relationship_arrow_endpoints.CollapsePathArrowEndpoints",
        create=True,
    ) as collapse_path_endpoints:
        collapse_path_endpoints.return_value.mutate.return_value = mock.sentinel.result

        result = RelationshipArrowEndpoints(
            arrow_client=mock.Mock(spec=AuthenticatedArrowClient),
            write_back_client=None,
        ).collapse_path(
            G=graph,
            path_templates=[["REL", "REL"]],
            mutate_relationship_type="FoF",
            allow_self_loops=True,
            concurrency=4,
            job_id="job-1",
            sudo=True,
            log_progress=False,
            username="neo4j",
        )

    assert result is mock.sentinel.result
    collapse_path_endpoints.assert_called_once()
    collapse_path_endpoints.return_value.mutate.assert_called_once_with(
        G=graph,
        path_templates=[["REL", "REL"]],
        mutate_relationship_type="FoF",
        allow_self_loops=True,
        concurrency=4,
        job_id="job-1",
        sudo=True,
        log_progress=False,
        username="neo4j",
    )
