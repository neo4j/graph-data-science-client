from typing import Callable, Generator

import pytest
from pandas import DataFrame

from graphdatascience.graph_construction.cypher_graph_constructor import CypherGraphConstructor
from graphdatascience.graph_construction.graph_constructor import GraphConstructor
from graphdatascience.procedure_surface.cypher.catalog.catalog_cypher_endpoints import CatalogCypherEndpoints
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner

from .graph_constructor_test_base import GraphConstructorTestBase


@pytest.fixture
def constructor_factory(query_runner: Neo4jQueryRunner) -> Callable[..., GraphConstructor]:
    def factory(graph_name: str, **kwargs: object) -> GraphConstructor:
        return CypherGraphConstructor(query_runner=query_runner, graph_name=graph_name, **kwargs)  # type: ignore[arg-type]

    return factory


@pytest.fixture
def catalog(query_runner: Neo4jQueryRunner) -> Generator[CatalogCypherEndpoints, None, None]:
    yield CatalogCypherEndpoints(query_runner)


@pytest.mark.filterwarnings("ignore: .*use Apache Arrow.*")
class TestCypherGraphConstructor(GraphConstructorTestBase):
    graph_name_prefix = "cypher"

    def test_overlapping_column_names_raises(
        self,
        constructor_factory: Callable[..., GraphConstructor],
    ) -> None:
        nodes = DataFrame({"nodeId": [0, 1], "weight": [1.0, 2.0]})
        relationships = DataFrame({"sourceNodeId": [0], "targetNodeId": [1], "weight": [0.5]})

        with pytest.raises(ValueError, match="disjoint column names"):
            constructor_factory("cypher_overlapping_cols").run([nodes], [relationships])
