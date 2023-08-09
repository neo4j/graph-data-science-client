import os
from pathlib import Path
from typing import Any, Generator, Optional

import pytest
from neo4j import Driver, GraphDatabase

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.aura_db_arrow_query_runner import (
    AuraDbConnectionInfo,
)
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.server_version.server_version import ServerVersion

URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
URI_TLS = os.environ.get("NEO4J_URI", "bolt+ssc://localhost:7687")

AUTH = ("neo4j", "password")
if os.environ.get("NEO4J_USER"):
    AUTH = (
        os.environ.get("NEO4J_USER", "DUMMY"),
        os.environ.get("NEO4J_PASSWORD", "neo4j"),
    )

DB = os.environ.get("NEO4J_DB", "neo4j")

AURA_DB_URI = os.environ.get("NEO4J_AURA_DB_URI", "bolt://localhost:7689")
AURA_DB_AUTH = ("neo4j", "password")


@pytest.fixture(scope="package")
def neo4j_driver() -> Generator[Driver, None, None]:
    driver = GraphDatabase.driver(URI, auth=AUTH)

    yield driver

    driver.close()


@pytest.fixture(scope="package")
def runner(neo4j_driver: Driver) -> Generator[Neo4jQueryRunner, None, None]:
    _runner = Neo4jQueryRunner(neo4j_driver)
    _runner.set_database(DB)

    yield _runner

    _runner.close()


@pytest.fixture(scope="package", autouse=False)
def auradb_runner() -> Generator[Neo4jQueryRunner, None, None]:
    driver = GraphDatabase.driver(AURA_DB_URI, auth=AURA_DB_AUTH)

    _runner = Neo4jQueryRunner(driver)
    _runner.set_database(DB)

    yield _runner

    driver.close()


@pytest.fixture(scope="package")
def gds() -> Generator[GraphDataScience, None, None]:
    _gds = GraphDataScience(URI, auth=AUTH)
    _gds.set_database(DB)

    yield _gds

    _gds.close()


@pytest.fixture(scope="package")
def gds_with_tls() -> Generator[GraphDataScience, None, None]:
    integration_test_dir = Path(__file__).resolve().parent
    cert = os.path.join(integration_test_dir, "resources", "arrow-flight-gds-test.crt")

    with open(cert, "rb") as f:
        root_ca = f.read()

    _gds = GraphDataScience(
        URI_TLS,
        auth=AUTH,
        arrow=True,
        arrow_disable_server_verification=True,
        arrow_tls_root_certs=root_ca,
    )
    _gds.set_database(DB)

    yield _gds

    _gds.close()


@pytest.fixture(scope="package")
def gds_without_arrow() -> Generator[GraphDataScience, None, None]:
    _gds = GraphDataScience(URI, auth=AUTH, arrow=False)
    _gds.set_database(DB)

    yield _gds

    _gds.close()


@pytest.fixture(scope="package", autouse=False)
def gds_with_cloud_setup(request: pytest.FixtureRequest) -> Optional[Generator[GraphDataScience, None, None]]:
    if "cloud_architecture" not in request.keywords:
        _gds = GraphDataScience(
            URI, auth=AUTH, arrow=True, aura_db_connection_info=AuraDbConnectionInfo(AURA_DB_URI, AURA_DB_AUTH)
        )
        _gds.set_database(DB)

        yield _gds

        _gds.close()
    return None


@pytest.fixture(autouse=True)
def clean_up(gds: GraphDataScience) -> Generator[None, None, None]:
    yield

    res = gds.graph.list()
    for graph_name in res["graphName"]:
        gds.graph.get(graph_name).drop(failIfMissing=True)

    res = gds.beta.pipeline.list()
    for pipeline_name in res["pipelineName"]:
        gds.pipeline.get(pipeline_name).drop(failIfMissing=True)

    res = gds.model.list() if gds.server_version() >= ServerVersion(2, 5, 0) else gds.beta.model.list()

    for model_info in res["modelInfo"]:
        model = gds.model.get(model_info["modelName"])
        if model.stored():
            if gds.server_version() >= ServerVersion(2, 5, 0):
                gds.model.delete(model)
            else:
                gds.alpha.model.delete(model)
        if model.exists():
            model.drop(failIfMissing=True)

    gds.run_cypher("MATCH (n) DETACH DELETE (n)")


def pytest_collection_modifyitems(config: Any, items: Any) -> None:
    if config.getoption("--target-aura"):
        skip_on_aura = pytest.mark.skip(reason="skipping since targeting AuraDS")
        for item in items:
            if "skip_on_aura" in item.keywords:
                item.add_marker(skip_on_aura)
    else:
        skip_not_aura = pytest.mark.skip(reason="skipping since not targeting AuraDS")
        for item in items:
            if "only_on_aura" in item.keywords:
                item.add_marker(skip_not_aura)

    if not config.getoption("--include-ogb"):
        skip_ogb_only = pytest.mark.skip(reason="need --include-ogb option to run")
        for item in items:
            if "ogb" in item.keywords:
                item.add_marker(skip_ogb_only)

    if not config.getoption("--include-enterprise"):
        skip_enterprise = pytest.mark.skip(reason="need --include-enterprise option to run")
        for item in items:
            if "enterprise" in item.keywords:
                item.add_marker(skip_enterprise)

    # `encrypted` includes marked tests and excludes everything else
    if config.getoption("--encrypted-only"):
        skip_encrypted_only = pytest.mark.skip(reason="not marked as `encrypted_only`")
        for item in items:
            if "encrypted_only" not in item.keywords:
                item.add_marker(skip_encrypted_only)
    else:
        skip_encrypted_only = pytest.mark.skip(reason="need --encrypted-only option to run")
        for item in items:
            if "encrypted_only" in item.keywords:
                item.add_marker(skip_encrypted_only)

    if not config.getoption("--include-model-store-location"):
        skip_stored_models = pytest.mark.skip(reason="need --include-model-store-location option to run")
        for item in items:
            if "model_store_location" in item.keywords:
                item.add_marker(skip_stored_models)

    # `cloud-architecture` includes marked tests and excludes everything else
    if config.getoption("--include-cloud-architecture"):
        skip_on_prem = pytest.mark.skip(reason="not marked as `cloud-architecture`")
        for item in items:
            if "cloud_architecture" not in item.keywords:
                item.add_marker(skip_on_prem)
    else:
        skip_cloud_architecture = pytest.mark.skip(reason="need --include-cloud-architecture option to run")
        for item in items:
            if "cloud_architecture" in item.keywords:
                item.add_marker(skip_cloud_architecture)

    try:
        server_version = GraphDataScience(URI, auth=AUTH)._server_version
    except Exception as e:
        print("Could not derive GDS library server version")
        raise e

    skip_incompatible_versions = pytest.mark.skip(reason=f"incompatible with GDS server version {server_version}")

    for item in items:
        for mark in item.iter_markers(name="compatible_with"):
            kwargs = mark.kwargs

            if "min_inclusive" in kwargs and kwargs["min_inclusive"] > server_version:
                item.add_marker(skip_incompatible_versions)
                continue

            if "max_exclusive" in kwargs and kwargs["max_exclusive"] <= server_version:
                item.add_marker(skip_incompatible_versions)
