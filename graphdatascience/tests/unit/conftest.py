from typing import Any, Dict, List, Optional

import pytest
from pandas import DataFrame

from graphdatascience import QueryRunner
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.cypher_graph_constructor import (
    CypherGraphConstructor,
)
from graphdatascience.query_runner.graph_constructor import GraphConstructor
from graphdatascience.server_version.server_version import ServerVersion

# Should mirror the latest GDS server version under development.
DEFAULT_SERVER_VERSION = ServerVersion(2, 2, 0)


class CollectingQueryRunner(QueryRunner):
    def __init__(self, server_version: ServerVersion) -> None:
        self._mock_result: Optional[DataFrame] = None
        self.queries: List[str] = []
        self.params: List[Dict[str, Any]] = []
        self._server_version = server_version

    def run_query(
        self, query: str, params: Optional[Dict[str, Any]] = None, db: Optional[str] = None, internal: bool = True
    ) -> DataFrame:
        if params is None:
            params = {}

        self.queries.append(query)
        self.params.append(params)

        # This "mock" lets us initialize the GDS object without issues.
        return (
            self._mock_result if self._mock_result is not None else DataFrame([{"version": str(self._server_version)}])
        )

    def last_query(self) -> str:
        return self.queries[-1]

    def last_params(self) -> Dict[str, Any]:
        return self.params[-1]

    def set_database(self, _: str) -> None:
        pass

    def database(self) -> str:
        return "dummy"

    def create_graph_constructor(
        self, graph_name: str, concurrency: int, undirected_relationship_types: Optional[List[str]]
    ) -> GraphConstructor:
        return CypherGraphConstructor(
            self, graph_name, concurrency, undirected_relationship_types, self._server_version
        )

    def set__mock_result(self, result: DataFrame) -> None:
        self._mock_result = result


@pytest.fixture
def runner(server_version: ServerVersion) -> CollectingQueryRunner:
    return CollectingQueryRunner(server_version)


@pytest.fixture
def gds(runner: CollectingQueryRunner) -> GraphDataScience:
    return GraphDataScience(runner, arrow=False)


@pytest.fixture(scope="package")
def server_version() -> ServerVersion:
    return DEFAULT_SERVER_VERSION
