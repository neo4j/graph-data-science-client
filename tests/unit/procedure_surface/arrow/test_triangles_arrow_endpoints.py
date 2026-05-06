from unittest import mock

import pandas as pd

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.session.session_v2_endpoints import SessionV2Endpoints


def test_session_exposes_top_level_triangles() -> None:
    endpoints = SessionV2Endpoints(
        mock.Mock(spec=AuthenticatedArrowClient),
        db_client=None,
        show_progress=False,
    )

    from graphdatascience.procedure_surface.arrow.community.triangles_arrow_endpoints import TrianglesArrowEndpoints

    assert isinstance(endpoints.triangles, TrianglesArrowEndpoints)


def test_triangles_call_runs_arrow_job() -> None:
    graph = mock.Mock()
    graph.name.return_value = "g"
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    result_df = pd.DataFrame({"nodeA": [0], "nodeB": [1], "nodeC": [2]})

    with (
        mock.patch(
            "graphdatascience.procedure_surface.arrow.community.triangles_arrow_endpoints.JobClient.run_job_and_wait",
            return_value="job-1",
        ) as run_job_and_wait,
        mock.patch(
            "graphdatascience.procedure_surface.arrow.community.triangles_arrow_endpoints.JobClient.stream_results",
            return_value=result_df,
        ) as stream_results,
    ):
        from graphdatascience.procedure_surface.arrow.community.triangles_arrow_endpoints import TrianglesArrowEndpoints

        result = TrianglesArrowEndpoints(arrow_client, show_progress=True)(
            G=graph,
            concurrency=4,
            job_id="job-1",
            label_filter=["Node"],
            log_progress=False,
            max_degree=8,
            node_labels=["Person"],
            relationship_types=["KNOWS"],
            sudo=True,
            username="neo4j",
        )

    assert result.equals(result_df)
    run_job_and_wait.assert_called_once_with(
        arrow_client,
        "v2/community.triangles",
        {
            "graphName": "g",
            "concurrency": 4,
            "jobId": "job-1",
            "labelFilter": ["Node"],
            "logProgress": False,
            "maxDegree": 8,
            "nodeLabels": ["Person"],
            "relationshipTypes": ["KNOWS"],
            "sudo": True,
            "username": "neo4j",
        },
        show_progress=False,
    )
    stream_results.assert_called_once_with(arrow_client, "g", "job-1")
