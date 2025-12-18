import os
from pathlib import Path
from typing import Any, Generator

import neo4j.exceptions
import pytest
from neo4j import Driver, GraphDatabase
from pytest_mock import MockerFixture

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.arrow_authentication import UsernamePasswordAuthentication
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.server_version.server_version import ServerVersion
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.session.session_lifecycle_manager import SessionLifecycleManager

URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
URI_TLS = os.environ.get("NEO4J_URI", "bolt+ssc://localhost:7687")

AUTH = ("neo4j", "password")
if neo4j_user := os.environ.get("NEO4J_USER", os.environ.get("NEO4J_USERNAME", "neo4j")):
    AUTH = (
        neo4j_user,
        os.environ.get("NEO4J_PASSWORD", "password"),
    )

DB = os.environ.get("NEO4J_DB", "neo4j")

AURA_DB_URI = os.environ.get("AURA_DB_URI", "bolt://localhost:7687")
AURA_DB_AUTH = ("neo4j", "password")


@pytest.fixture(scope="package", autouse=False)
def neo4j_driver() -> Generator[Driver, None, None]:
    driver = GraphDatabase.driver(URI, auth=AUTH)

    yield driver

    driver.close()


@pytest.fixture(scope="package", autouse=False)
def runner(neo4j_driver: Driver) -> Generator[Neo4jQueryRunner, None, None]:
    _runner = Neo4jQueryRunner.create_for_db(neo4j_driver)
    _runner.set_database(DB)

    yield _runner

    _runner.close()


@pytest.fixture(scope="package", autouse=False)
def gds() -> Generator[GraphDataScience, None, None]:
    _gds = GraphDataScience(URI, auth=AUTH)
    _gds.set_database(DB)

    yield _gds

    _gds.close()


@pytest.fixture(scope="package", autouse=False)
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


@pytest.fixture(scope="package", autouse=False)
def gds_without_arrow() -> Generator[GraphDataScience, None, None]:
    _gds = GraphDataScience(URI, auth=AUTH, arrow=False)
    _gds.set_database(DB)

    yield _gds

    _gds.close()


@pytest.fixture(scope="package", autouse=False)
def gds_with_cloud_setup(
    request: pytest.FixtureRequest, mocker: MockerFixture
) -> Generator[AuraGraphDataScience, None, None]:
    _gds = AuraGraphDataScience.create(
        session_bolt_connection_info=DbmsConnectionInfo(URI, AUTH[0], AUTH[1]),
        arrow_authentication=UsernamePasswordAuthentication(AUTH[0], AUTH[1]),
        db_endpoint=DbmsConnectionInfo(AURA_DB_URI, AURA_DB_AUTH[0], AURA_DB_AUTH[1]),
        session_lifecycle_manager=mocker.Mock(spec=SessionLifecycleManager),
    )
    _gds.set_database(DB)

    yield _gds

    _gds.close()


@pytest.fixture(scope="package", autouse=False)
def standalone_aura_gds(mocker: MockerFixture) -> Generator[AuraGraphDataScience, None, None]:
    _gds = AuraGraphDataScience.create(
        session_bolt_connection_info=DbmsConnectionInfo(URI, AUTH[0], AUTH[1]),
        arrow_authentication=UsernamePasswordAuthentication(AUTH[0], AUTH[1]),
        db_endpoint=None,
        session_lifecycle_manager=mocker.Mock(spec=SessionLifecycleManager),
    )

    yield _gds

    _gds.close()


def is_neo4j_44(gds: GraphDataScience) -> bool:
    try:
        result = gds.run_cypher(
            "CALL dbms.components() YIELD name, versions, edition unwind versions as version RETURN name, version, edition"
        )
        version = result["version"][0]
        return version.startswith("4.4")  # type: ignore
    except Exception as e:
        print(e)
        return False


def id_warning_pattern() -> str:
    if neo4j.__version__.startswith("6."):
        return r"(.*feature deprecated without replacement\. id is deprecated and will be removed without a replacement.*)|(.*feature deprecated with replacement\. id is deprecated.*)"
    else:
        return r".*The query used a deprecated function.*[`']id[`'].*"


@pytest.fixture(autouse=True)
def clean_up(gds: GraphDataScience) -> Generator[None, None, None]:
    yield

    res = gds.graph.list()
    for graph_name in res["graphName"]:
        gds.graph.drop(graph_name, failIfMissing=True)

    res = gds.pipeline.list() if gds.server_version() >= ServerVersion(2, 5, 0) else gds.beta.pipeline.list()
    for pipeline_name in res["pipelineName"]:
        gds.pipeline.get(pipeline_name).drop(failIfMissing=True)

    if gds.server_version() >= ServerVersion(2, 5, 0):
        model_names = gds.model.list()["modelName"]
    else:
        model_names = gds.beta.model.list()["modelInfo"].apply(func=lambda row: row["modelName"])

    for model_name in model_names:
        model = gds.model.get(model_name)
        if model.stored():
            if gds.server_version() >= ServerVersion(2, 5, 0):
                gds.model.delete(model)
            else:
                gds.alpha.model.delete(model)
        if model.exists():
            model.drop(failIfMissing=True)

    try:
        # for the session setup there should be no data
        if gds.run_cypher("MATCH (n) RETURN count(n)").squeeze() > 0:
            gds.run_cypher("MATCH (n) DETACH DELETE (n)")
    except neo4j.exceptions.ClientError as e:
        print(e)


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

    with GraphDataScience(URI, auth=AUTH) as gds:
        try:
            server_version = gds._server_version
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

    db_driver_version = Neo4jQueryRunner._NEO4J_DRIVER_VERSION
    skip_incompatible_driver_version = pytest.mark.skip(reason=f"incompatible with driver version {db_driver_version}")

    for item in items:
        for mark in item.iter_markers(name="compatible_with_db_driver"):
            kwargs = mark.kwargs

            if "min_inclusive" in kwargs and kwargs["min_inclusive"] > db_driver_version:
                item.add_marker(skip_incompatible_driver_version)
                continue

            if "max_exclusive" in kwargs and kwargs["max_exclusive"] <= db_driver_version:
                item.add_marker(skip_incompatible_driver_version)
