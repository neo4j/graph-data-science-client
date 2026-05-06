from typing import Generator
from uuid import uuid4

import pytest

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.pipeline.node_regression_pipeline_cypher_endpoints import (
    NodeRegressionPipelineCypherEndpoints,
)
from graphdatascience.query_runner import QueryType
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: Neo4jQueryRunner) -> Generator[GraphV2, None, None]:
    graph_name = f"nr-cypher-g-{uuid4().hex[:8]}"
    create_statement = """
    CREATE
    (a: Node {feature: 1.0, target: 1.0}),
    (b: Node {feature: 2.0, target: 2.0}),
    (c: Node {feature: 3.0, target: 3.0}),
    (d: Node {feature: 4.0, target: 4.0}),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(d),
    (d)-[:REL]->(a)
    """

    projection_query = f"""
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('{graph_name}', n, m, {{sourceNodeProperties: properties(n), targetNodeProperties: properties(m)}}) AS G
        RETURN G
    """

    with create_graph(query_runner, graph_name, create_statement, projection_query) as G:
        yield G


@pytest.fixture
def endpoints(query_runner: Neo4jQueryRunner) -> NodeRegressionPipelineCypherEndpoints:
    return NodeRegressionPipelineCypherEndpoints(query_runner)


def test_node_regression_train_cypher_pipeline(
    query_runner: Neo4jQueryRunner,
    endpoints: NodeRegressionPipelineCypherEndpoints,
    sample_graph: GraphV2,
) -> None:
    pipeline_name = f"nr-cypher-pipe-{uuid4().hex[:8]}"
    model_name = f"nr-cypher-model-{uuid4().hex[:8]}"

    try:
        pipeline, create_result = endpoints.create(pipeline_name)
        node_property_result = pipeline.add_node_property("pageRank", mutate_property="pr")
        feature_result = pipeline.select_features(["pr"])
        candidate_result = pipeline.add_linear_regression(max_epochs=1, min_epochs=1)
        model, train_result = pipeline.train(
            sample_graph,
            metrics=["MEAN_ABSOLUTE_ERROR"],
            model_name=model_name,
            target_property="target",
        )

        assert create_result.name == pipeline_name
        assert node_property_result.name == pipeline_name
        assert feature_result.name == pipeline_name
        assert candidate_result.name == pipeline_name
        assert train_result.train_millis is not None
        assert train_result.train_millis >= 0
        assert model.name() == model_name
    finally:
        query_runner.run_cypher(
            "CALL gds.model.drop($name, false)",
            query_type=QueryType.USER_ACTION,
            params={"name": model_name},
        )
        query_runner.run_cypher(
            "CALL gds.pipeline.drop($name, false)",
            query_type=QueryType.USER_ACTION,
            params={"name": pipeline_name},
        )
