from pathlib import Path
from typing import Generator

import numpy as np
import pandas as pd
import pytest
from testcontainers.core.network import Network

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.gds_arrow_client import GdsArrowClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.catalog import CatalogArrowEndpoints
from graphdatascience.procedure_surface.arrow.catalog.graph_backend_arrow import get_graph
from graphdatascience.query_runner.termination_flag import TerminationFlag
from tests.integrationV2.conftest import GdsSessionConnectionInfo, create_arrow_client, start_session
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture(scope="package")
def session_connection(
    network: Network, password_dir: Path, logs_dir: Path
) -> Generator[GdsSessionConnectionInfo, None, None]:
    yield from start_session(logs_dir, network, password_dir)


@pytest.fixture(scope="package")
def arrow_client(session_connection: GdsSessionConnectionInfo) -> AuthenticatedArrowClient:
    return create_arrow_client(session_connection)


@pytest.fixture(scope="package")
def gds_arrow_client(arrow_client: AuthenticatedArrowClient) -> GdsArrowClient:
    return GdsArrowClient(arrow_client)


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    gdl = """
        CREATE
        (a: Node:Foo {prop1: 1, prop2: 42.0}),
        (b: Node {prop1: 2, prop2: 43.0}),
        (c: Node:Foo {prop1: 3, prop2: 44.0}),

        (a)-[:REL {relX: 1, relY: 42}]->(b),
        (b)-[:REL {relX: 2, relY: 43}]->(c),
        (c)-[:REL2 {relX: 1, relY: 2}]->(a),
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


def test_stream_node_label(gds_arrow_client: GdsArrowClient, sample_graph: GraphV2) -> None:
    job_id = gds_arrow_client.get_nodes(sample_graph.name(), node_filter="n.prop1 > 1")
    result = gds_arrow_client.stream_job(sample_graph.name(), job_id)

    assert ["nodeId"] == list(result.columns)
    assert len(result) == 2


def test_stream_node_properties(gds_arrow_client: GdsArrowClient, sample_graph: GraphV2) -> None:
    job_id = gds_arrow_client.get_node_properties(sample_graph.name(), node_properties=["prop1", "prop2"])
    result = gds_arrow_client.stream_job(sample_graph.name(), job_id)

    assert len(result) == 3
    assert "nodeId" in result.columns
    assert "prop1" in result.columns
    assert "prop2" in result.columns
    assert {"nodeId", "prop1", "prop2"} == set(result.columns)
    assert set(result["prop1"].tolist()) == {1, 2, 3}
    assert set(result["prop2"].tolist()) == {42.0, 43.0, 44.0}


def test_stream_relationship_properties(gds_arrow_client: GdsArrowClient, sample_graph: GraphV2) -> None:
    job_id = gds_arrow_client.get_relationships(sample_graph.name(), ["REL"], relationship_properties=["relX", "relY"])
    result = gds_arrow_client.stream_job(sample_graph.name(), job_id)

    assert len(result) == 2
    assert "sourceNodeId" in result.columns
    assert "targetNodeId" in result.columns
    assert "relationshipType" in result.columns
    assert "REL" in result["relationshipType"].tolist()
    assert "relX" in result.columns
    assert "relY" in result.columns
    assert list(result["relX"].tolist()) == [1.0, 2.0]
    assert list(result["relY"].tolist()) == [42.0, 43.0]


def test_project_from_triplets(arrow_client: AuthenticatedArrowClient, gds_arrow_client: GdsArrowClient) -> None:
    df = pd.DataFrame(
        {"sourceNode": np.array([1, 2, 3], dtype=np.int64), "targetNode": np.array([4, 5, 6], dtype=np.int64)}
    )

    graph_name = "triplets"

    job_id = gds_arrow_client.create_graph_from_triplets(graph_name)
    gds_arrow_client.upload_triplets(job_id, df)
    gds_arrow_client.triplet_load_done(job_id)

    while gds_arrow_client.job_status(job_id).status != "Done":
        pass

    with get_graph(graph_name, arrow_client) as G:
        assert G.node_count() == 6
        assert G.relationship_count() == 3
        assert G.name() == graph_name


def test_project_from_triplets_interrupted(gds_arrow_client: GdsArrowClient) -> None:
    df = pd.DataFrame(
        {"sourceNode": np.array([1, 2, 3], dtype=np.int64), "targetNode": np.array([4, 5, 6], dtype=np.int64)}
    )

    termination_flag = TerminationFlag.create()
    termination_flag.set()

    job_id = gds_arrow_client.create_graph_from_triplets("triplets")
    with pytest.raises(Exception, match=".*was aborted.*"):
        gds_arrow_client.upload_triplets(job_id, df, termination_flag=termination_flag)


def test_project_from_tables(arrow_client: AuthenticatedArrowClient, gds_arrow_client: GdsArrowClient) -> None:
    nodes = pd.DataFrame(
        {
            "nodeId": np.array([1, 2, 3, 4, 5, 6], dtype=np.int64),
        }
    )

    rels = pd.DataFrame(
        {
            "sourceNodeId": np.array([1, 2, 3], dtype=np.int64),
            "targetNodeId": np.array([4, 5, 6], dtype=np.int64),
        }
    )

    job_id = gds_arrow_client.create_graph("table")
    gds_arrow_client.upload_nodes(job_id, nodes)
    gds_arrow_client.node_load_done(job_id)

    while gds_arrow_client.job_status(job_id).status != "RELATIONSHIP_LOADING":
        pass

    gds_arrow_client.upload_relationships(job_id, rels)
    gds_arrow_client.relationship_load_done(job_id)

    while gds_arrow_client.job_status(job_id).status != "Done":
        pass

    listing = CatalogArrowEndpoints(arrow_client).list("table")[0]
    assert listing.node_count == 6
    assert listing.relationship_count == 3
    assert listing.graph_name == "table"
