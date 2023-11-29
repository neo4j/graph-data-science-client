from typing import Any, Dict, Generator, List, Optional

import pytest
from pandas import DataFrame

from graphdatascience import QueryRunner
from graphdatascience.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.aura_db_arrow_query_runner import (
    AuraDbConnectionInfo,
)
from graphdatascience.query_runner.cypher_graph_constructor import (
    CypherGraphConstructor,
)
from graphdatascience.query_runner.graph_constructor import GraphConstructor
from graphdatascience.server_version.server_version import ServerVersion

# Should mirror the latest GDS server version under development.
DEFAULT_SERVER_VERSION = ServerVersion(2, 6, 0)


class CollectingQueryRunner(QueryRunner):
    def __init__(self, server_version: ServerVersion) -> None:
        self._mock_result: Optional[DataFrame] = None
        self.queries: List[str] = []
        self.params: List[Dict[str, Any]] = []
        self._server_version = server_version
        self._database = "dummy"

    def call_procedure(
        self,
        endpoint: str,
        params: Optional[CallParameters] = None,
        yields: Optional[List[str]] = None,
        database: Optional[str] = None,
        logging: bool = False,
        custom_error: bool = True,
    ) -> DataFrame:
        if params is None:
            params = CallParameters()

        yields_clause = "" if yields is None else " YIELD " + ", ".join(yields)
        query = f"CALL {endpoint}({params.placeholder_str()}){yields_clause}"

        return self.run_cypher(query, params, database, custom_error)

    def run_cypher(
        self, query: str, params: Optional[Dict[str, Any]] = None, db: Optional[str] = None, custom_error: bool = True
    ) -> DataFrame:
        if params is None:
            params = {}

        self.queries.append(query)
        self.params.append(dict(params.items()))

        # This "mock" lets us initialize the GDS object without issues.
        return (
            self._mock_result if self._mock_result is not None else DataFrame([{"version": str(self._server_version)}])
        )

    def server_version(self) -> ServerVersion:
        return self._server_version

    def driver_config(self) -> Dict[str, Any]:
        return {}

    def encrypted(self) -> bool:
        return False

    def last_query(self) -> str:
        return self.queries[-1]

    def last_params(self) -> Dict[str, Any]:
        return self.params[-1]

    def set_database(self, database: str) -> None:
        self._database = database

    def set_bookmarks(self, _: Optional[Any]) -> None:
        pass

    def database(self) -> str:
        return self._database

    def bookmarks(self) -> Optional[Any]:
        return None

    def last_bookmarks(self) -> Optional[Any]:
        return None

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
def gds(runner: CollectingQueryRunner) -> Generator[GraphDataScience, None, None]:
    gds = GraphDataScience(runner, arrow=False)
    yield gds

    gds.close()


@pytest.fixture
def aura_gds(runner: CollectingQueryRunner) -> Generator[AuraGraphDataScience, None, None]:
    aura_gds = AuraGraphDataScience(
        endpoint=runner, auth=("some", "auth"), aura_db_connection_info=AuraDbConnectionInfo("uri", ("some", "auth"))
    )
    yield aura_gds

    aura_gds.close()


@pytest.fixture(scope="package")
def server_version() -> ServerVersion:
    return DEFAULT_SERVER_VERSION
