from typing import Generator

import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.server_version.server_version import ServerVersion
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience


@pytest.fixture(autouse=True)
def G(gds_with_cloud_setup: AuraGraphDataScience) -> Generator[Graph, None, None]:
    gds_with_cloud_setup.run_cypher(
        """
        CREATE
        (a:Location {name: 'A', population: 1337}),
        (b:Location {name: 'B'}),
        (c:Location {name: 'C'}),
        (d:Location {name: 'D'}),
        (e:Location {name: 'G'}),
        (f:Location {name: 2}),
        (a)-[:ROAD {cost: 50}]->(b),
        (a)-[:ROAD {cost: 50}]->(c),
        (a)-[:ROAD {cost: 100}]->(d),
        (b)-[:ROAD {cost: 40}]->(d),
        (c)-[:ROAD {cost: 40}]->(d),
        (c)-[:ROAD {cost: 80}]->(e),
        (d)-[:ROAD {cost: 30}]->(e),
        (d)-[:ROAD {cost: 80}]->(f),
        (e)-[:ROAD {cost: 40}]->(f)
        """
    )
    G, _ = gds_with_cloud_setup.graph.project(
        "g",
        """
         MATCH (n)-[r]->(m)
         RETURN gds.graph.project.remote(n, m, {
            sourceNodeLabels: labels(n),
            targetNodeLabels: labels(m),
            sourceNodeProperties: n {.population},
            targetNodeProperties: m {.population},
            relationshipType: type(r),
            relationshipProperties: properties(r)
        })
        """,
    )

    yield G

    G.drop()
    gds_with_cloud_setup.run_cypher("MATCH (n) DETACH DELETE n")


# @pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_remote_util_as_node(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    id = gds_with_cloud_setup.find_node_id(["Location"], {"name": "A"})
    result = gds_with_cloud_setup.util.asNode(id)
    assert result["name"] == "A"


# @pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_remote_util_as_nodes(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    ids = [
        gds_with_cloud_setup.find_node_id(["Location"], {"name": "A"}),
        gds_with_cloud_setup.find_node_id(["Location"], {"name": 2}),
    ]
    result = gds_with_cloud_setup.util.asNodes(ids)
    assert len(result) == 2


# @pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_util_nodeProperty(gds_with_cloud_setup: AuraGraphDataScience, G: Graph) -> None:
    id = gds_with_cloud_setup.find_node_id(["Location"], {"name": "A"})
    result = gds_with_cloud_setup.util.nodeProperty(G, id, "population")
    assert result == 1337
