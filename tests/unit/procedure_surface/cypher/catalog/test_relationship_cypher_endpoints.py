import pandas as pd
import pytest

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.catalog.relationships_endpoints import (
    Aggregation,
    RelationshipsToUndirectedResult,
)
from graphdatascience.procedure_surface.cypher.catalog.relationship_cypher_endpoints import (
    RelationshipCypherEndpoints,
)
from tests.unit.conftest import CollectingQueryRunner


def to_undirected_result() -> dict[str, object]:
    return {
        "preProcessingMillis": 1,
        "computeMillis": 2,
        "mutateMillis": 3,
        "postProcessingMillis": 4,
        "inputRelationships": 5,
        "relationshipsWritten": 6,
        "configuration": {},
    }


def test_to_undirected_aggregation_str(query_runner: CollectingQueryRunner, graph: Graph) -> None:
    query_runner.add__mock_result("gds.graph.relationships.toUndirected", pd.DataFrame([to_undirected_result()]))

    result = RelationshipCypherEndpoints(query_runner).to_undirected(graph, "REL", "NEW_REL", aggregation="SUM")

    assert isinstance(result, RelationshipsToUndirectedResult)
    config = query_runner.last_params()["config"]
    assert config["aggregation"] == "SUM"
    assert config["relationshipType"] == "REL"
    assert config["mutateRelationshipType"] == "NEW_REL"


def test_to_undirected_aggregation_enum(query_runner: CollectingQueryRunner, graph: Graph) -> None:
    query_runner.add__mock_result("gds.graph.relationships.toUndirected", pd.DataFrame([to_undirected_result()]))

    RelationshipCypherEndpoints(query_runner).to_undirected(graph, "REL", "NEW_REL", aggregation=Aggregation.MIN)

    config = query_runner.last_params()["config"]
    assert config["aggregation"] == "MIN"


def test_to_undirected_aggregation_dict_of_str(query_runner: CollectingQueryRunner, graph: Graph) -> None:
    query_runner.add__mock_result("gds.graph.relationships.toUndirected", pd.DataFrame([to_undirected_result()]))

    RelationshipCypherEndpoints(query_runner).to_undirected(
        graph, "REL", "NEW_REL", aggregation={"weight": "MAX", "score": Aggregation.COUNT}
    )

    config = query_runner.last_params()["config"]
    assert config["aggregation"] == {"weight": "MAX", "score": "COUNT"}


def test_to_undirected_aggregation_invalid(query_runner: CollectingQueryRunner, graph: Graph) -> None:
    endpoints = RelationshipCypherEndpoints(query_runner)

    with pytest.raises(ValueError, match="Invalid aggregation: 'MEAN'"):
        endpoints.to_undirected(graph, "REL", "NEW_REL", aggregation="MEAN")


def test_aggregation_of() -> None:
    assert Aggregation.of(Aggregation.SUM) is Aggregation.SUM
    assert Aggregation.of("SINGLE") is Aggregation.SINGLE

    with pytest.raises(ValueError, match="Invalid aggregation: 'sum'"):
        Aggregation.of("sum")


def write_result() -> dict[str, object]:
    return {
        "graphName": "g",
        "relationshipType": "REL",
        "relationshipProperty": "weight",
        "relationshipsWritten": 2,
        "propertiesWritten": 2,
        "writeMillis": 1,
        "configuration": {},
    }


def test_write_forwards_write_concurrency(query_runner: CollectingQueryRunner, graph: Graph) -> None:
    query_runner.add__mock_result("gds.graph.relationship.write", pd.DataFrame([write_result()]))

    RelationshipCypherEndpoints(query_runner).write(graph, "REL", ["weight"], concurrency=4, write_concurrency=2)

    assert "gds.graph.relationship.write" in query_runner.last_query()
    config = query_runner.last_params()["config"]
    assert config["concurrency"] == 4
    assert config["writeConcurrency"] == 2


def test_write_properties_forwards_write_concurrency(query_runner: CollectingQueryRunner, graph: Graph) -> None:
    query_runner.add__mock_result("gds.graph.relationshipProperties.write", pd.DataFrame([write_result()]))

    RelationshipCypherEndpoints(query_runner).write(graph, "REL", ["weight", "score"], write_concurrency=2)

    assert "gds.graph.relationshipProperties.write" in query_runner.last_query()
    assert query_runner.last_params()["config"]["writeConcurrency"] == 2
