from unittest import mock

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.plugin_v2_endpoints import PluginV2Endpoints
from graphdatascience.procedure_surface.cypher.collapse_path_cypher_endpoints import CollapsePathCypherEndpoints
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


def test_plugin_exposes_top_level_collapse_path() -> None:
    endpoints = PluginV2Endpoints(
        arrow_client=mock.Mock(spec=AuthenticatedArrowClient),
        db_client=mock.Mock(spec=Neo4jQueryRunner),
    )

    assert isinstance(endpoints.collapse_path, CollapsePathCypherEndpoints)


def test_collapse_path_mutate_runs_query() -> None:
    graph = mock.Mock()
    graph.name.return_value = "g"
    row = mock.Mock()
    row.to_dict.return_value = {
        "preProcessingMillis": 1,
        "computeMillis": 2,
        "mutateMillis": 3,
        "relationshipsWritten": 4,
        "configuration": {"jobId": "job-1"},
    }
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value.squeeze.return_value = row

    result = CollapsePathCypherEndpoints(query_runner).mutate(
        G=graph,
        path_templates=[["REL", "REL"]],
        mutate_relationship_type="FoF",
        node_labels=["Node"],
        allow_self_loops=True,
        concurrency=4,
        job_id="job-1",
        sudo=True,
        log_progress=False,
        username="neo4j",
    )

    assert result.relationships_written == 4
    query_runner.call_procedure.assert_called_once_with(
        endpoint="gds.collapsePath.mutate",
        params=mock.ANY,
    )
    params = query_runner.call_procedure.call_args.kwargs["params"]
    assert params["graph_name"] == "g"
    assert params["config"] == {
        "pathTemplates": [["REL", "REL"]],
        "mutateRelationshipType": "FoF",
        "nodeLabels": ["Node"],
        "allowSelfLoops": True,
        "concurrency": 4,
        "jobId": "job-1",
        "sudo": True,
        "logProgress": False,
        "username": "neo4j",
    }
