from gdsclient import GDS


def test_create_graph_native():
    gds = GDS()
    graph = gds.graph.create("g", "A", "R")
    assert graph
