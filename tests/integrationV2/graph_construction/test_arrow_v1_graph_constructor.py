from typing import Callable, Generator

import pytest

from graphdatascience.arrow_client.v1.gds_arrow_client import GdsArrowClient
from graphdatascience.graph_construction.arrow_v1_graph_constructor import ArrowV1GraphConstructor
from graphdatascience.graph_construction.graph_constructor import GraphConstructor
from graphdatascience.procedure_surface.cypher.catalog.catalog_cypher_endpoints import CatalogCypherEndpoints
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner

from .graph_constructor_test_base import GraphConstructorTestBase


@pytest.fixture
def constructor_factory(gds_arrow_client: GdsArrowClient) -> Callable[..., GraphConstructor]:
    def factory(graph_name: str, **kwargs: object) -> GraphConstructor:
        return ArrowV1GraphConstructor(
            database="neo4j",
            graph_name=graph_name,
            flight_client=gds_arrow_client,
            **kwargs,  # type: ignore[arg-type]
        )

    return factory


@pytest.fixture
def catalog(query_runner: Neo4jQueryRunner) -> Generator[CatalogCypherEndpoints, None, None]:
    yield CatalogCypherEndpoints(query_runner)


class TestArrowV1GraphConstructor(GraphConstructorTestBase):
    graph_name_prefix = "arrow_v1"
