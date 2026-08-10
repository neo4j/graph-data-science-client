from contextlib import ExitStack
from unittest import mock

import pytest
from pandas import DataFrame
from pytest_mock import MockerFixture

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.api_types import JobStatus
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.arrow.catalog.catalog_arrow_endpoints import CatalogArrowEndpoints
from tests.unit.arrow_client.arrow_test_utils import ArrowTestResult

CATALOG_MODULE = "graphdatascience.procedure_surface.arrow.catalog.catalog_arrow_endpoints"


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


def test_node_properties_endpoint_inherits_show_progress(mocker: MockerFixture) -> None:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)

    quiet_endpoints = CatalogArrowEndpoints(arrow_client=arrow_client, show_progress=False)
    loud_endpoints = CatalogArrowEndpoints(arrow_client=arrow_client, show_progress=True)

    # `.node_properties` is typed as the abstract endpoint; the concrete arrow impl carries _show_progress.
    assert getattr(quiet_endpoints.node_properties, "_show_progress") is False
    assert getattr(loud_endpoints.node_properties, "_show_progress") is True


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


def _generate_summary() -> dict[str, object]:
    return {
        "name": "g",
        "nodes": 4,
        "relationships": 5,
        "generateMillis": 42,
        "relationshipSeed": 123,
        "averageDegree": 2.5,
        "relationshipDistribution": "UNIFORM",
        "relationshipProperty": None,
    }


def _filter_summary() -> dict[str, object]:
    return {
        "graphName": "filtered",
        "fromGraphName": "g",
        "nodeFilter": "true",
        "relationshipFilter": "true",
        "nodeCount": 2,
        "relationshipCount": 1,
        "projectMillis": 7,
    }


def _construct_nodes() -> DataFrame:
    return DataFrame({"nodeId": [0, 1], "labels": [["A"], ["B"]]})


def _construct_endpoints(mocker: MockerFixture, job_id: str) -> CatalogArrowEndpoints:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)

    relationship_loading_done_status = JobStatus(
        jobId=job_id, status="RELATIONSHIP_LOADING", progress=-1, description=""
    )
    construct_done_status = JobStatus(jobId=job_id, status="Done", progress=-1, description="")

    do_action_with_retry = mocker.Mock()
    do_action_with_retry.side_effect = [
        iter([ArrowTestResult(relationship_loading_done_status.dump_camel())]),
        iter([ArrowTestResult(construct_done_status.dump_camel())]),
    ]
    arrow_client.do_action_with_retry = do_action_with_retry

    return CatalogArrowEndpoints(arrow_client=arrow_client)


def test_construct_overwrite_drops_existing_graph(mocker: MockerFixture) -> None:
    endpoints = _construct_endpoints(mocker, "job-123")
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")

    with patch_gds_arrow_client("job-123"):
        G = endpoints.construct(graph_name="g", nodes=_construct_nodes(), relationships=[], overwrite=True)

    drop_spy.assert_called_once_with("g", fail_if_missing=False)
    assert G.name() == "g"


def test_construct_does_not_drop_by_default(mocker: MockerFixture) -> None:
    endpoints = _construct_endpoints(mocker, "job-123")
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")

    with patch_gds_arrow_client("job-123"):
        endpoints.construct(graph_name="g", nodes=_construct_nodes(), relationships=[])

    drop_spy.assert_not_called()


def test_generate_overwrite_drops_existing_graph(mocker: MockerFixture) -> None:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)
    endpoints = CatalogArrowEndpoints(arrow_client=arrow_client)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")
    mocker.patch(f"{CATALOG_MODULE}.JobClient.run_job_and_wait", return_value="job-1")
    mocker.patch(f"{CATALOG_MODULE}.JobClient.get_summary", return_value=_generate_summary())
    mocker.patch(f"{CATALOG_MODULE}.get_graph", return_value=mocker.Mock())

    endpoints.generate("g", 4, 2.5, overwrite=True)

    drop_spy.assert_called_once_with("g", fail_if_missing=False)


def test_generate_does_not_drop_by_default(mocker: MockerFixture) -> None:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)
    endpoints = CatalogArrowEndpoints(arrow_client=arrow_client)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")
    mocker.patch(f"{CATALOG_MODULE}.JobClient.run_job_and_wait", return_value="job-1")
    mocker.patch(f"{CATALOG_MODULE}.JobClient.get_summary", return_value=_generate_summary())
    mocker.patch(f"{CATALOG_MODULE}.get_graph", return_value=mocker.Mock())

    endpoints.generate("g", 4, 2.5)

    drop_spy.assert_not_called()


def test_generate_async_overwrite_drops_existing_graph(mocker: MockerFixture) -> None:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)
    endpoints = CatalogArrowEndpoints(arrow_client=arrow_client)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")
    mocker.patch(f"{CATALOG_MODULE}.JobClient.run_job", return_value="job-1")

    endpoints.generate_async("g", 4, 2.5, overwrite=True)

    drop_spy.assert_called_once_with("g", fail_if_missing=False)


def test_filter_overwrite_drops_existing_graph(mocker: MockerFixture) -> None:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)
    endpoints = CatalogArrowEndpoints(arrow_client=arrow_client)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")
    mocker.patch(f"{CATALOG_MODULE}.JobClient.run_job_and_wait", return_value="job-1")
    mocker.patch(f"{CATALOG_MODULE}.JobClient.get_summary", return_value=_filter_summary())
    mocker.patch(f"{CATALOG_MODULE}.get_graph", return_value=mocker.Mock())

    G = Graph("g", mocker.Mock())

    endpoints.filter(G, "filtered", "true", "true", overwrite=True)

    drop_spy.assert_called_once_with("filtered", fail_if_missing=False)


def test_filter_does_not_drop_by_default(mocker: MockerFixture) -> None:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)
    endpoints = CatalogArrowEndpoints(arrow_client=arrow_client)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")
    mocker.patch(f"{CATALOG_MODULE}.JobClient.run_job_and_wait", return_value="job-1")
    mocker.patch(f"{CATALOG_MODULE}.JobClient.get_summary", return_value=_filter_summary())
    mocker.patch(f"{CATALOG_MODULE}.get_graph", return_value=mocker.Mock())

    G = Graph("g", mocker.Mock())

    endpoints.filter(G, "filtered", "true", "true")

    drop_spy.assert_not_called()


def test_filter_async_overwrite_drops_existing_graph(mocker: MockerFixture) -> None:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)
    endpoints = CatalogArrowEndpoints(arrow_client=arrow_client)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")
    mocker.patch(f"{CATALOG_MODULE}.JobClient.run_job", return_value="job-1")

    G = Graph("g", mocker.Mock())

    endpoints.filter_async(G, "filtered", "true", "true", overwrite=True)

    drop_spy.assert_called_once_with("filtered", fail_if_missing=False)


def test_filter_rejects_name_equal_to_source_graph(mocker: MockerFixture) -> None:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)
    endpoints = CatalogArrowEndpoints(arrow_client=arrow_client)
    drop_spy = mocker.patch.object(endpoints._graph_backend, "drop")
    mocker.patch(f"{CATALOG_MODULE}.JobClient.run_job_and_wait", return_value="job-1")
    mocker.patch(f"{CATALOG_MODULE}.JobClient.get_summary", return_value=_filter_summary())
    mocker.patch(f"{CATALOG_MODULE}.get_graph", return_value=mocker.Mock())

    G = Graph("g", mocker.Mock())

    with pytest.raises(ValueError, match="must not equal the source graph name"):
        endpoints.filter(G, "g", "true", "true", overwrite=True)

    drop_spy.assert_not_called()
