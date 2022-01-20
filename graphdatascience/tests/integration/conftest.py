import os
from typing import Generator

import pytest
from neo4j import Driver, GraphDatabase

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner

URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")

AUTH = None
if os.environ.get("NEO4J_USER") is not None:
    AUTH = (
        os.environ.get("NEO4J_USER"),
        os.environ.get("NEO4J_PASSWORD", "neo4j"),
    )


@pytest.fixture(scope="package")
def neo4j_driver() -> Generator[Driver, None, None]:
    driver = GraphDatabase.driver(URI, auth=AUTH)

    yield driver

    driver.close()


@pytest.fixture(scope="package")
def runner(neo4j_driver: Driver) -> Neo4jQueryRunner:
    return GraphDataScience.create_neo4j_query_runner(neo4j_driver)


@pytest.fixture(scope="package")
def gds(runner: Neo4jQueryRunner) -> GraphDataScience:
    return GraphDataScience(runner)
