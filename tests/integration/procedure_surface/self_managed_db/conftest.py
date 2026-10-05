from pathlib import Path
from typing import Generator

import pytest
from testcontainers.core.network import Network

from graphdatascience.query_runner import QueryRunner
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from tests.integration.services import create_db_query_runner, self_managed_db_alias, start_self_managed_database


@pytest.fixture(scope="package")
def self_managed_db_connection(
    network: Network, logs_dir: Path, request: pytest.FixtureRequest
) -> Generator[DbmsConnectionInfo, None, None]:
    """Stock Neo4j database (no GDS plugin) with the shipped remote-projection stubs enabled.

    The `query_runner` inherited from `procedure_surface/conftest.py` resolves to this
    fixture, so tests in this package project from a plugin-free Neo4j into the GDS
    session (`arrow_client`).
    """
    yield from start_self_managed_database(logs_dir, network, request.node.name, db_alias=self_managed_db_alias())


@pytest.fixture(scope="package")
def query_runner(self_managed_db_connection: DbmsConnectionInfo) -> Generator[QueryRunner, None, None]:
    yield from create_db_query_runner(self_managed_db_connection)
