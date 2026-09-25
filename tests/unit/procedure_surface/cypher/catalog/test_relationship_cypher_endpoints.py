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
