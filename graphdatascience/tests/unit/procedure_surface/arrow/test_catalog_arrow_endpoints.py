from contextlib import ExitStack
from unittest import mock

from pandas import DataFrame
from pytest_mock import MockerFixture

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.api_types import JobStatus
from graphdatascience.procedure_surface.arrow.catalog.catalog_arrow_endpoints import CatalogArrowEndpoints
from graphdatascience.tests.unit.arrow_client.arrow_test_utils import ArrowTestResult


def test_construct_with_no_rels(mocker: MockerFixture) -> None:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)
    job_id = "job-123"

    relationship_loading_done_status = JobStatus(
        jobId=job_id,
        status="RELATIONSHIP_LOADING",
        progress=-1,
        description="",
    )
    construct_done_status = JobStatus(
        jobId=job_id,
        status="Done",
        progress=-1,
        description="",
    )

    do_action_with_retry = mocker.Mock()
    do_action_with_retry.side_effect = [
        iter([ArrowTestResult(relationship_loading_done_status.dump_camel())]),
        iter([ArrowTestResult(construct_done_status.dump_camel())]),
    ]

    arrow_client.do_action_with_retry = do_action_with_retry

    endpoints = CatalogArrowEndpoints(arrow_client=arrow_client)

    nodes = DataFrame(
        {
            "nodeId": [0, 1],
            "labels": [["A"], ["B"]],
            "propA": [1337, 42.1],
        }
    )
    with patch_gds_arrow_client(job_id):
        G = endpoints.construct(graph_name="g", nodes=nodes, relationships=[])
        assert G.name() == "g"


def test_construct_with_df_lists(mocker: MockerFixture) -> None:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)
    job_id = "foo"
    relationship_loading_done_status = JobStatus(
        jobId=job_id,
        status="RELATIONSHIP_LOADING",
        progress=-1,
        description="",
    )
    construct_done_status = JobStatus(
        jobId=job_id,
        status="Done",
        progress=-1,
        description="",
    )

    do_action_with_retry = mocker.Mock()
    do_action_with_retry.side_effect = [
        iter([ArrowTestResult(relationship_loading_done_status.dump_camel())]),
        iter([ArrowTestResult(construct_done_status.dump_camel())]),
    ]
    arrow_client.do_action_with_retry = do_action_with_retry

    endpoints = CatalogArrowEndpoints(arrow_client=arrow_client)

    nodes = [
        DataFrame({"nodeId": [0, 1], "labels": ["a", "a"], "property": [6.0, 7.0]}),
        DataFrame({"nodeId": [2, 3], "labels": ["b", "b"], "q": [-500, -400]}),
    ]
    relationships = [
        DataFrame(
            {"sourceNodeId": [0, 1], "targetNodeId": [1, 2], "relationshipType": ["A", "A"], "weights": [0.2, 0.3]}
        ),
        DataFrame({"sourceNodeId": [2, 3], "targetNodeId": [3, 0], "relationshipType": ["B", "B"]}),
    ]
    with patch_gds_arrow_client(job_id):
        G = endpoints.construct(graph_name="g", nodes=nodes, relationships=relationships)
        assert G.name() == "g"


def patch_gds_arrow_client(create_graph_job_id: str) -> ExitStack:
    exit_stack = ExitStack()
    patches = [
        mock.patch(
            "graphdatascience.arrow_client.v2.gds_arrow_client.GdsArrowClient.create_graph",
            return_value=create_graph_job_id,
        ),
        mock.patch(
            "graphdatascience.arrow_client.v2.gds_arrow_client.GdsArrowClient.upload_nodes",
            return_value=None,
        ),
        mock.patch(
            "graphdatascience.arrow_client.v2.gds_arrow_client.GdsArrowClient.upload_relationships",
            return_value=None,
        ),
        mock.patch(
            "graphdatascience.arrow_client.v2.gds_arrow_client.GdsArrowClient.node_load_done",
            return_value=None,
        ),
        mock.patch(
            "graphdatascience.arrow_client.v2.gds_arrow_client.GdsArrowClient.relationship_load_done",
            return_value=None,
        ),
    ]

    for p in patches:
        exit_stack.enter_context(p)

    return exit_stack
