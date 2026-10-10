from typing import Any

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


def test_configure_aura_respects_caller_overrides() -> None:
    config: dict[str, Any] = {"connection_acquisition_timeout": 30}
    Neo4jQueryRunner._configure_aura(config)
    assert config["connection_acquisition_timeout"] == 30  # setdefault does not overwrite
