from typing import Any, Dict, Generator, List, Optional

import pytest
from pandas import DataFrame

from graphdatascience import QueryRunner
from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.cypher_graph_constructor import (
    CypherGraphConstructor,
)
from graphdatascience.query_runner.graph_constructor import GraphConstructor
from graphdatascience.query_runner.query_runner import EndpointType
from graphdatascience.server_version.server_version import ServerVersion

# Should mirror the latest GDS server version under development.
DEFAULT_SERVER_VERSION = ServerVersion(2, 6, 0)


class CollectingQueryRunner(QueryRunner):
    def __init__(self, server_version: ServerVersion) -> None:
        self._mock_result: Optional[DataFrame] = None
        self.queries: List[str] = []
        self.params: List[Dict[str, Any]] = []
        self._server_version = server_version

    # FIXME: avoid copy of Neo4j Query runner impl (could have different mocks per endpoint now)
    def call_endpoint(
        self,
        type: EndpointType,
        endpoint: str,
        params: Optional[CallParameters] = None,
        yields: Optional[List[str]] = None,
        database: Optional[str] = None,
        logging: bool = False,
        custom_error: bool = True,
    ) -> DataFrame:
        if params is None:
            params = CallParameters()

        call_keyword = "CALL" if type == EndpointType.PROCEDURE else "RETURN"

        if yields is not None and type == EndpointType.FUNCTION:
            raise ValueError("Functions cannot yield results")

        yields_clause = "" if yields is None else " YIELD " + ", ".join(yields)
        query = f"{call_keyword} {endpoint}({params.placeholder_str()}){yields_clause}"

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

    def last_query(self) -> str:
        return self.queries[-1]

    def last_params(self) -> Dict[str, Any]:
        return self.params[-1]

    def set_database(self, _: str) -> None:
        pass

    def set_bookmarks(self, _: Optional[Any]) -> None:
        pass

    def database(self) -> str:
        return "dummy"

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


@pytest.fixture(scope="package")
def server_version() -> ServerVersion:
    return DEFAULT_SERVER_VERSION
