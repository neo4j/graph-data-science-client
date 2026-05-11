from uuid import uuid4

from graphdatascience.procedure_surface.cypher.pipeline.node_regression_pipeline_cypher_endpoints import (
    NodeRegressionPipelineCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pipeline.pipeline_cypher_endpoints import PipelineCypherEndpoints
from graphdatascience.query_runner import QueryType
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


def test_pipeline_list_returns_created_pipeline(query_runner: Neo4jQueryRunner) -> None:
    pipeline_name = f"pipeline-list-cypher-{uuid4().hex[:8]}"

    try:
        NodeRegressionPipelineCypherEndpoints(query_runner).create(pipeline_name)

        pipelines = PipelineCypherEndpoints(query_runner).list(pipeline_name)

        assert len(pipelines) == 1
        assert pipelines[0].pipeline_name == pipeline_name
        assert pipelines[0].pipeline_type == "Node regression training pipeline"
    finally:
        query_runner.run_cypher(
            "CALL gds.pipeline.drop($name, false)",
            query_type=QueryType.USER_ACTION,
            params={"name": pipeline_name},
        )
