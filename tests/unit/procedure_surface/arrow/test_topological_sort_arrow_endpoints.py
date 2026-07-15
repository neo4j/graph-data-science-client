from unittest import mock

import pandas as pd

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.pathfinding.dag_arrow_endpoints import DagArrowEndpoints
from graphdatascience.procedure_surface.arrow.pathfinding.topological_sort_arrow_endpoints import (
    TopologicalSortArrowEndpoints,
)
from graphdatascience.session import AuraGraphDataScience
from graphdatascience.session.session_lifecycle_manager import Noop


def test_session_exposes_topological_sort() -> None:
    endpoints = AuraGraphDataScience(
        mock.Mock(spec=AuthenticatedArrowClient),
        db_query_runner=None,
        session_lifecycle_manager=Noop(),
        show_progress=False,
    )

    assert isinstance(endpoints.dag, DagArrowEndpoints)
    assert isinstance(endpoints.dag.topological_sort, TopologicalSortArrowEndpoints)


def test_topological_sort_stream_runs_arrow_job() -> None:
    graph = mock.Mock()
    graph.name.return_value = "g"
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    # Deliberately out of order to verify sorting by the `index` column.
    result_df = pd.DataFrame({"nodeId": [2, 0, 1], "index": [2, 0, 1], "maxDistanceFromSource": [1.0, 0.0, 1.0]})

    with (
        mock.patch(
            "graphdatascience.procedure_surface.arrow.endpoints_helper_base.JobClient.run_job_and_wait",
            return_value="job-1",
        ) as run_job_and_wait,
        mock.patch(
            "graphdatascience.procedure_surface.arrow.endpoints_helper_base.JobClient.stream_results",
            return_value=result_df,
        ) as stream_results,
    ):
        result = TopologicalSortArrowEndpoints(arrow_client, show_progress=True).stream(
            G=graph,
            compute_max_distance_from_source=True,
            concurrency=4,
            job_id="job-1",
            log_progress=False,
            node_labels=["Person"],
            relationship_types=["KNOWS"],
            sudo=True,
            username="neo4j",
        )

    # The `index` column is used to order the rows and then dropped.
    assert list(result.columns) == ["nodeId", "maxDistanceFromSource"]
    assert list(result["nodeId"]) == [0, 1, 2]
    run_job_and_wait.assert_called_once_with(
        arrow_client,
        "v2/pathfinding.topologicalSort",
        {
            "graphName": "g",
            "computeMaxDistanceFromSource": True,
            "concurrency": 4,
            "jobId": "job-1",
            "logProgress": False,
            "nodeLabels": ["Person"],
            "relationshipTypes": ["KNOWS"],
            "sudo": True,
            "username": "neo4j",
        },
        show_progress=False,
    )
    stream_results.assert_called_once_with(arrow_client, "g", "job-1")
