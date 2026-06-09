from unittest import mock

import pandas as pd


def test_triangles_call_runs_query() -> None:
    graph = mock.Mock()
    graph.name.return_value = "g"
    result_df = pd.DataFrame({"nodeA": [0], "nodeB": [1], "nodeC": [2]})
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = result_df

    from graphdatascience.procedure_surface.cypher.community.triangles_cypher_endpoints import TrianglesCypherEndpoints

    result = TrianglesCypherEndpoints(query_runner)(
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
    query_runner.call_procedure.assert_called_once_with(
        endpoint="gds.triangles",
        params=mock.ANY,
        logging=False,
    )
    params = query_runner.call_procedure.call_args.kwargs["params"]
    assert params["graph_name"] == "g"
    assert params["config"] == {
        "concurrency": 4,
        "jobId": "job-1",
        "labelFilter": ["Node"],
        "logProgress": False,
        "maxDegree": 8,
        "nodeLabels": ["Person"],
        "relationshipTypes": ["KNOWS"],
        "sudo": True,
        "username": "neo4j",
    }
