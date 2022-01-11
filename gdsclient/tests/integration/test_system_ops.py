from gdsclient.graph_data_science import GraphDataScience


def test_listProgress(gds: GraphDataScience) -> None:
    result = gds.beta.listProgress()

    assert len(result) == 0
