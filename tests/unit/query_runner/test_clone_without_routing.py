from unittest.mock import MagicMock, patch

from neo4j import GraphDatabase

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


def test_clone_without_routing_always_auto_closes() -> None:
    auth = ("neo4j", "password")
    parent_driver = GraphDatabase.driver("bolt://localhost:7687", auth=auth)
    try:
        parent = Neo4jQueryRunner(
            driver=parent_driver,
            protocol="bolt",
            auth=auth,
            auto_close=False,
        )

        mock_clone_driver = MagicMock()
        with patch.object(GraphDatabase, "driver", return_value=mock_clone_driver):
            clone = parent.cloneWithoutRouting("localhost", 7687)

        assert clone._auto_close is True  # type: ignore[attr-defined]
        clone.close()
        mock_clone_driver.close.assert_called_once()
    finally:
        parent_driver.close()
