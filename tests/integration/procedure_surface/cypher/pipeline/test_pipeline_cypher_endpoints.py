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


def test_pipeline_cypher_get_returns_created_pipeline(query_runner: Neo4jQueryRunner) -> None:
    pipeline_name = f"pipeline-get-cypher-{uuid4().hex[:8]}"
    pipeline_surface = PipelineCypherEndpoints(query_runner)

    try:
        NodeRegressionPipelineCypherEndpoints(query_runner).create(pipeline_name)

        entry = pipeline_surface.get(pipeline_name)
        assert entry.pipeline_name == pipeline_name
        assert entry.pipeline_type == "Node regression training pipeline"
        assert entry.pipeline_info is not None

        with pytest.raises(ValueError, match="There is no"):
            pipeline_surface.get(f"missing-{uuid4().hex[:8]}")
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

        assert pipeline_surface.exists(pipeline_name) is True

        drop_result = pipeline_surface.drop(pipeline_name)
        assert drop_result is not None
        assert drop_result.pipeline_name == pipeline_name
        assert pipeline_surface.exists(pipeline_name) is False
        assert pipeline_surface.drop(pipeline_name) is None

        with pytest.raises(Exception):
            pipeline_surface.drop(pipeline_name, fail_if_missing=True)
    finally:
        query_runner.run_cypher(
            "CALL gds.pipeline.drop($name, false)",
            query_type=QueryType.USER_ACTION,
            params={"name": pipeline_name},
        )
