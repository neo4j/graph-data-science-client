from typing import Generator

import pytest
from neo4j.exceptions import Neo4jError

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import Graph
from graphdatascience.procedure_surface.cypher.model_api_cypher import ModelApiCypher
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_statement = """
    CREATE
    (a: Node {age: 1}),
    (b: Node {age: 2}),
    (c: Node {age: 3}),
    (a)-[:REL]->(c),
    (b)-[:REL]->(c)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {sourceNodeProperties: properties(n), targetNodeProperties: properties(m)}) AS G
        RETURN G
    """

    with create_graph(
        query_runner,
        "g",
        create_statement,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def gs_model(query_runner: QueryRunner, sample_graph: Graph) -> Generator[str, None, None]:
    train_result = query_runner.run_cypher(
        "CALL gds.beta.graphSage.train($graph, {modelName: 'gs-model', featureProperties:['age'], embeddingDimension: 1, sampleSizes: [1], maxIterations: 1, searchDepth: 1})",
        {"graph": sample_graph.name()},
    )

    model_name = train_result.iloc[0]["modelInfo"]["modelName"]

    yield model_name  # type: ignore

    query_runner.run_cypher("CALL gds.model.drop($name, false)", {"name": model_name})


@pytest.fixture
def model_api(query_runner: QueryRunner) -> Generator[ModelApiCypher, None, None]:
    yield ModelApiCypher(query_runner)


def test_model_get(gs_model: str, model_api: ModelApiCypher) -> None:
    model = model_api.get(gs_model)

    assert model.name == gs_model
    assert model.type == "graphSage"

    with pytest.raises(ValueError, match="There is no 'nonexistent-model' in the model catalog"):
        model_api.get("nonexistent-model")


def test_model_exists(gs_model: str, model_api: ModelApiCypher) -> None:
    assert model_api.exists(gs_model)
    assert not model_api.exists("nonexistent-model")


def test_model_delete(gs_model: str, model_api: ModelApiCypher) -> None:
    model_details = model_api.drop(gs_model, fail_if_missing=False)

    assert model_details is not None
    assert model_details.name == gs_model

    # Check that the model no longer exists
    assert not model_api.exists(gs_model)

    # Attempt to drop a non-existing model
    assert model_api.drop("nonexistent-model", fail_if_missing=False) is None

    with pytest.raises(Neo4jError, match="Model with name `nonexistent-model` does not exist"):
        model_api.drop("nonexistent-model", fail_if_missing=True)
