from typing import Generator
from uuid import uuid4

import pytest

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.model.model_catalog_cypher_endpoints import ModelCatalogCypherEndpoints
from graphdatascience.procedure_surface.cypher.pipeline.link_prediction_pipeline_cypher_endpoints import (
    LinkPredictionPipelineCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pipeline.pipeline_catalog_cypher_endpoints import (
    PipelineCatalogCypherEndpoints,
)
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: Neo4jQueryRunner) -> Generator[GraphV2, None, None]:
    graph_name = f"lp-cypher-g-{uuid4().hex[:8]}"
    create_statement = """
    CREATE
    (a: Node {feature: 1.0}),
    (b: Node {feature: 2.0}),
    (c: Node {feature: 3.0}),
    (d: Node {feature: 4.0}),
    (e: Node {feature: 5.0}),
    (f: Node {feature: 6.0}),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(d),
    (d)-[:REL]->(e),
    (e)-[:REL]->(f),
    (f)-[:REL]->(a),
    (a)-[:CONTEXTREL]->(c),
    (b)-[:CONTEXTREL]->(d),
    (c)-[:CONTEXTREL]->(e),
    (d)-[:CONTEXTREL]->(f)
    """

    projection_query = f"""
        MATCH (n)-[r]->(m)
        WITH gds.graph.project(
            '{graph_name}',
            n,
            m,
            {{
                sourceNodeLabels: labels(n),
                targetNodeLabels: labels(m),
                sourceNodeProperties: properties(n),
                targetNodeProperties: properties(m),
                relationshipType: type(r)
            }}
            ,
            {{
                undirectedRelationshipTypes: ['REL']
            }}
        ) AS G
        RETURN G
    """

    with create_graph(query_runner, graph_name, create_statement, projection_query) as G:
        yield G


@pytest.fixture
def endpoints(query_runner: Neo4jQueryRunner) -> LinkPredictionPipelineCypherEndpoints:
    return LinkPredictionPipelineCypherEndpoints(query_runner)


def test_link_prediction_train_and_predict_stream_cypher_pipeline(
    query_runner: Neo4jQueryRunner,
    endpoints: LinkPredictionPipelineCypherEndpoints,
    sample_graph: GraphV2,
) -> None:
    pipeline_name = f"lp-cypher-pipe-{uuid4().hex[:8]}"
    model_name = f"lp-cypher-model-{uuid4().hex[:8]}"

    try:
        pipeline, create_result = endpoints.create(pipeline_name)
        node_property_result = pipeline.add_node_property("degree", mutate_property="rank")
        feature_result = pipeline.add_feature("l2", node_properties=["rank"])
        split_result = pipeline.configure_split(train_fraction=0.7, test_fraction=0.2, validation_folds=2)
        candidate_result = pipeline.add_logistic_regression(max_epochs=1, min_epochs=1)
        model, train_result = pipeline.train(
            sample_graph,
            model_name=model_name,
            source_node_label="Node",
            target_node_label="Node",
            target_relationship_type="REL",
        )
        stream_result = model.predict_stream(
            sample_graph,
            source_node_label="Node",
            target_node_label="Node",
            top_n=2,
        )
        fetched_pipeline = endpoints.get(pipeline_name)

        assert create_result.name == pipeline_name
        assert node_property_result.name == pipeline_name
        assert feature_result.name == pipeline_name
        assert split_result.name == pipeline_name
        assert candidate_result.name == pipeline_name
        assert train_result.train_millis is not None
        assert train_result.train_millis >= 0
        assert model.name() == model_name
        assert fetched_pipeline.name() == pipeline_name
        assert "node1" in stream_result.columns or "sourceNodeId" in stream_result.columns
        assert "node2" in stream_result.columns or "targetNodeId" in stream_result.columns
        assert "probability" in stream_result.columns
        assert len(stream_result) > 0
    finally:
        ModelCatalogCypherEndpoints(query_runner).drop(model_name)
        PipelineCatalogCypherEndpoints(query_runner).drop(pipeline_name)


def test_link_prediction_train_estimate_cypher_pipeline(
    query_runner: Neo4jQueryRunner,
    endpoints: LinkPredictionPipelineCypherEndpoints,
    sample_graph: GraphV2,
) -> None:
    pipeline_name = f"lp-cypher-pipe-{uuid4().hex[:8]}"
    model_name = f"lp-cypher-model-{uuid4().hex[:8]}"

    try:
        pipeline, _ = endpoints.create(pipeline_name)
        pipeline.add_feature("l2", node_properties=["feature"])
        pipeline.configure_split(train_fraction=0.5, test_fraction=0.3, validation_folds=2)
        pipeline.add_logistic_regression(max_epochs=1, min_epochs=1)
        estimate = pipeline.train_estimate(
            sample_graph,
            model_name=model_name,
            source_node_label="Node",
            target_node_label="Node",
            target_relationship_type="REL",
        )

        assert estimate.required_memory is not None
        assert estimate.bytes_max is None or estimate.bytes_max >= 0
    finally:
        ModelCatalogCypherEndpoints(query_runner).drop(model_name)
        PipelineCatalogCypherEndpoints(query_runner).drop(pipeline_name)


def test_link_prediction_predict_estimate_cypher_pipeline(
    query_runner: Neo4jQueryRunner,
    endpoints: LinkPredictionPipelineCypherEndpoints,
    sample_graph: GraphV2,
) -> None:
    pipeline_name = f"lp-cypher-pipe-{uuid4().hex[:8]}"
    model_name = f"lp-cypher-model-{uuid4().hex[:8]}"

    try:
        pipeline, _ = endpoints.create(pipeline_name)
        pipeline.add_feature("l2", node_properties=["feature"])
        pipeline.configure_split(train_fraction=0.5, test_fraction=0.3, validation_folds=2)
        pipeline.add_logistic_regression(max_epochs=1, min_epochs=1)
        model, _ = pipeline.train(
            sample_graph,
            model_name=model_name,
            source_node_label="Node",
            target_node_label="Node",
            target_relationship_type="REL",
        )
        estimate = model.predict_estimate(
            sample_graph,
            source_node_label="Node",
            target_node_label="Node",
            top_n=2,
        )

        assert estimate.required_memory is not None
        assert estimate.bytes_max is None or estimate.bytes_max >= 0
    finally:
        ModelCatalogCypherEndpoints(query_runner).drop(model_name)
        PipelineCatalogCypherEndpoints(query_runner).drop(pipeline_name)


def test_link_prediction_predict_mutate_cypher_pipeline(
    query_runner: Neo4jQueryRunner,
    endpoints: LinkPredictionPipelineCypherEndpoints,
    sample_graph: GraphV2,
) -> None:
    pipeline_name = f"lp-cypher-pipe-{uuid4().hex[:8]}"
    model_name = f"lp-cypher-model-{uuid4().hex[:8]}"

    try:
        pipeline, _ = endpoints.create(pipeline_name)
        pipeline.add_feature("l2", node_properties=["feature"])
        pipeline.configure_split(train_fraction=0.5, test_fraction=0.3, validation_folds=2)
        pipeline.add_logistic_regression(max_epochs=1, min_epochs=1)
        model, _ = pipeline.train(
            sample_graph,
            model_name=model_name,
            source_node_label="Node",
            target_node_label="Node",
            target_relationship_type="REL",
        )

        mutate_result = model.predict_mutate(
            sample_graph,
            mutate_relationship_type="PREDICTED_REL",
            source_node_label="Node",
            target_node_label="Node",
            top_n=2,
        )

        assert mutate_result.relationships_written is not None
        assert mutate_result.relationships_written > 0
        assert mutate_result.mutate_millis is not None
        assert mutate_result.mutate_millis >= 0
    finally:
        ModelCatalogCypherEndpoints(query_runner).drop(model_name)
        PipelineCatalogCypherEndpoints(query_runner).drop(pipeline_name)


def test_link_prediction_pipeline_object_supports_exists_and_drop(
    query_runner: Neo4jQueryRunner,
    endpoints: LinkPredictionPipelineCypherEndpoints,
) -> None:
    pipeline_name = f"lp-cypher-pipe-{uuid4().hex[:8]}"

    try:
        pipeline, _ = endpoints.create(pipeline_name)

        assert pipeline.exists() is True
        dropped = pipeline.drop()
        assert dropped is not None
        assert dropped.pipeline_name == pipeline_name
        assert pipeline.exists() is False
    finally:
        PipelineCatalogCypherEndpoints(query_runner).drop(pipeline_name)


def test_link_prediction_configure_auto_tuning_cypher_pipeline(
    query_runner: Neo4jQueryRunner,
    endpoints: LinkPredictionPipelineCypherEndpoints,
) -> None:
    pipeline_name = f"lp-cypher-pipe-{uuid4().hex[:8]}"

    try:
        pipeline, _ = endpoints.create(pipeline_name)
        result = pipeline.configure_auto_tuning(max_trials=42)

        assert result.name == pipeline_name
    finally:
        PipelineCatalogCypherEndpoints(query_runner).drop(pipeline_name)
