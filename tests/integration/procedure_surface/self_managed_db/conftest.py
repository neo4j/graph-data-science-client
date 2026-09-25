from pathlib import Path
from typing import Generator

import pytest
from testcontainers.core.network import Network

from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from tests.integration.services import start_self_managed_database, self_managed_db_alias


@pytest.fixture(scope="package")
def neo4j_connection(
    network: Network, logs_dir: Path, request: pytest.FixtureRequest
) -> Generator[DbmsConnectionInfo, None, None]:
    """Stock Neo4j database (no GDS plugin) with the shipped remote-projection stubs enabled.

    The `query_runner` inherited from `procedure_surface/conftest.py` resolves to this
    fixture, so tests in this package project from a plugin-free Neo4j into the GDS
    session (`arrow_client`).
    """
    yield from start_self_managed_database(logs_dir, network, request.node.name, db_alias=self_managed_db_alias())
