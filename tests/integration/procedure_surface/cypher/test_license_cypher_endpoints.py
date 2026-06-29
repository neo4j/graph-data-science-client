from typing import Generator

import pytest

from graphdatascience.procedure_surface.api.license_endpoints import LicenseStateResult
from graphdatascience.procedure_surface.cypher.license_cypher_endpoints import LicenseCypherEndpoints
from graphdatascience.query_runner.query_runner import QueryRunner


@pytest.fixture
def license_endpoints(query_runner: QueryRunner) -> Generator[LicenseCypherEndpoints, None, None]:
    yield LicenseCypherEndpoints(query_runner)


def test_license_state(license_endpoints: LicenseCypherEndpoints) -> None:
    result = license_endpoints.state()

    assert isinstance(result, LicenseStateResult)
    assert isinstance(result.is_licensed, bool)
    assert isinstance(result.details, str)
