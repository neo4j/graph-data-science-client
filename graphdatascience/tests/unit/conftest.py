from typing import Any, Dict, List, Optional, Union

import pandas
import pytest
from pandas.core.frame import DataFrame

from graphdatascience import QueryRunner
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.graph_constructor import GraphConstructor
from graphdatascience.server_version.server_version import ServerVersion

# Should mirror the latest GDS server version under development.
DEFAULT_SERVER_VERSION = ServerVersion(2, 1, 0)


class CollectingQueryRunner(QueryRunner):
    def __init__(self, server_version: Union[str, ServerVersion]) -> None:
        self.queries: List[str] = []
        self.params: List[Dict[str, Any]] = []
        self.server_version = server_version

    def run_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> DataFrame:
        if params is None:
            params = {}

        self.queries.append(query)
        self.params.append(params)

        # This "mock" lets us initialize the GDS object without issues.
        return pandas.DataFrame([{"version": str(self.server_version)}])

    def last_query(self) -> str:
        return self.queries[-1]

    def last_params(self) -> Dict[str, Any]:
        return self.params[-1]

    def set_database(self, _: str) -> None:
        pass

    def database(self) -> str:
        return "dummy"

    def create_graph_constructor(self, _: str, __: int) -> GraphConstructor:
        raise NotImplementedError


@pytest.fixture
def runner(server_version: ServerVersion) -> CollectingQueryRunner:
    return CollectingQueryRunner(server_version)


@pytest.fixture
def gds(runner: CollectingQueryRunner) -> GraphDataScience:
    return GraphDataScience(runner, arrow=False)


@pytest.fixture(scope="package")
def server_version() -> ServerVersion:
    return DEFAULT_SERVER_VERSION
