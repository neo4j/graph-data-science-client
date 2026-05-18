from typing import Generator
from uuid import uuid4

import pytest

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.pipeline.node_classification_pipeline_cypher_endpoints import (
    NodeClassificationPipelineCypherEndpoints,
)
from graphdatascience.query_runner import QueryType
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: Neo4jQueryRunner) -> Generator[GraphV2, None, None]:
    graph_name = f"nc-cypher-g-{uuid4().hex[:8]}"
    create_statement = """
    CREATE
    (a: Node {feature: 1.0, target: 0}),
    (b: Node {feature: 2.0, target: 1}),
    (c: Node {feature: 3.0, target: 0}),
    (d: Node {feature: 4.0, target: 1}),
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
def endpoints(query_runner: Neo4jQueryRunner) -> NodeClassificationPipelineCypherEndpoints:
    return NodeClassificationPipelineCypherEndpoints(query_runner)


def test_node_classification_train_estimate_cypher_pipeline(
    query_runner: Neo4jQueryRunner,
    endpoints: NodeClassificationPipelineCypherEndpoints,
    sample_graph: GraphV2,
) -> None:
    pipeline_name = f"nc-cypher-pipe-{uuid4().hex[:8]}"
    model_name = f"nc-cypher-model-{uuid4().hex[:8]}"

    try:
        pipeline, _ = endpoints.create(pipeline_name)
        pipeline.select_features(node_properties=["feature"])
        pipeline.add_logistic_regression(max_epochs=1, min_epochs=1)
        estimate = pipeline.train_estimate(
            sample_graph,
            metrics=["F1_WEIGHTED"],
            model_name=model_name,
            target_property="target",
        )

        assert estimate.required_memory is not None
        assert estimate.bytes_max is None or estimate.bytes_max >= 0
    finally:
        query_runner.run_cypher(
            "CALL gds.pipeline.drop($name, false)",
            query_type=QueryType.USER_ACTION,
            params={"name": pipeline_name},
        )


def test_node_classification_predict_estimate_cypher_pipeline(
    query_runner: Neo4jQueryRunner,
    endpoints: NodeClassificationPipelineCypherEndpoints,
    sample_graph: GraphV2,
) -> None:
    pipeline_name = f"nc-cypher-pipe-{uuid4().hex[:8]}"
    model_name = f"nc-cypher-model-{uuid4().hex[:8]}"

    try:
        pipeline, _ = endpoints.create(pipeline_name)
        pipeline.select_features(node_properties=["feature"])
        pipeline.add_logistic_regression(max_epochs=1, min_epochs=1)
        model, _ = pipeline.train(
            sample_graph,
            metrics=["F1_WEIGHTED"],
            model_name=model_name,
            target_property="target",
        )
        estimate = model.predict_estimate(sample_graph)

        assert estimate.required_memory is not None
        assert estimate.bytes_max is None or estimate.bytes_max >= 0
    finally:
        query_runner.run_cypher(
            "CALL gds.pipeline.drop($name, false)",
            query_type=QueryType.USER_ACTION,
            params={"name": pipeline_name},
        )


def test_node_classification_pipeline_object_supports_exists_and_drop(
    query_runner: Neo4jQueryRunner,
    endpoints: NodeClassificationPipelineCypherEndpoints,
) -> None:
    pipeline_name = f"nc-cypher-pipe-{uuid4().hex[:8]}"

    try:
        pipeline, _ = endpoints.create(pipeline_name)

        assert pipeline.exists() is True
        dropped = pipeline.drop()
        assert dropped is not None
        assert dropped.pipeline_name == pipeline_name
        assert pipeline.exists() is False
    finally:
        query_runner.run_cypher(
            "CALL gds.pipeline.drop($name, false)",
            query_type=QueryType.USER_ACTION,
            params={"name": pipeline_name},
        )
