import pytest
from pandas import DataFrame

from graphdatascience import ServerVersion
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_graph_construct_and_calling_procedure(standalone_aura_gds: AuraGraphDataScience) -> None:
    nodes = DataFrame({"nodeId": [0, 1, 2, 3]})
    relationships = DataFrame({"sourceNodeId": [0, 1, 2, 3], "targetNodeId": [1, 2, 3, 0]})

    G = standalone_aura_gds.graph.construct("graph", nodes, relationships)

    assert G.name() == "graph"
    assert G.node_count() == 4
    assert G.relationship_count() == 4

    result = standalone_aura_gds.pageRank.stream(G, mutateProperty="score")

    assert len(result) == 4


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_fails_on_calling_cypher(standalone_aura_gds: AuraGraphDataScience) -> None:
    with pytest.raises(NotImplementedError):
        standalone_aura_gds.run_cypher("MATCH (n) RETURN n")
