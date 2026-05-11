from uuid import uuid4

import pytest

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


def test_pipeline_cypher_exists_and_drop_round_trip(query_runner: Neo4jQueryRunner) -> None:
    pipeline_name = f"pipeline-exists-cypher-{uuid4().hex[:8]}"
    pipeline_surface = PipelineCypherEndpoints(query_runner)

    try:
        NodeRegressionPipelineCypherEndpoints(query_runner).create(pipeline_name)

        exists_result = pipeline_surface.exists(pipeline_name)
        assert exists_result is not None
        assert exists_result.pipeline_name == pipeline_name
        assert exists_result.exists is True

        drop_result = pipeline_surface.drop(pipeline_name)
        assert drop_result is not None
        assert drop_result.pipeline_name == pipeline_name
        assert pipeline_surface.exists(pipeline_name) is None
        assert pipeline_surface.drop(pipeline_name) is None

        with pytest.raises(Exception):
            pipeline_surface.drop(pipeline_name, fail_if_missing=True)
    finally:
        query_runner.run_cypher(
            "CALL gds.pipeline.drop($name, false)",
            query_type=QueryType.USER_ACTION,
            params={"name": pipeline_name},
        )
