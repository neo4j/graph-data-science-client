import neo4j
import pytest

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


@pytest.mark.parametrize(
    "uri,expected",
    [
        ("neo4j+s://foo.bar:7687", "neo4j+s://foo.bar:7687"),
        ("neo4j://foo.bar:7687", "neo4j://foo.bar:7687"),
        ("neo4j://foo.bar", "neo4j://foo.bar:7687"),
        ("bolt://localhost:7687", "bolt://localhost:7687"),
        ("bolt+s://localhost:7688", "bolt+s://localhost:7688"),
        ("bolt://[::1]:7687", "bolt://[::1]:7687"),
    ],
)
def test_uri_from_driver(uri: str, expected: str) -> None:
    driver = neo4j.GraphDatabase.driver(uri, auth=("neo4j", "pw"))
    try:
        assert Neo4jQueryRunner.uri_from_driver(driver) == expected
    finally:
        driver.close()
