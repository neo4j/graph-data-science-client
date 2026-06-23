from typing import Generator

import pytest

from graphdatascience.procedure_surface.api.debug_endpoints import DebugSysInfoResult
from graphdatascience.procedure_surface.cypher.debug_cypher_endpoints import DebugCypherEndpoints
from graphdatascience.query_runner.query_runner import QueryRunner


@pytest.fixture
def debug_endpoints(query_runner: QueryRunner) -> Generator[DebugCypherEndpoints, None, None]:
    yield DebugCypherEndpoints(query_runner)


def test_sys_info(debug_endpoints: DebugCypherEndpoints) -> None:
    result = debug_endpoints.sys_info()

    assert isinstance(result, DebugSysInfoResult)
    assert result.gds_version is not None
    assert result.gds_edition is not None
