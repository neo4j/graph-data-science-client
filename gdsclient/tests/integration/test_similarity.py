import pytest

from gdsclient.graph_data_science import GraphDataScience


def test_similarity_jaccard(gds: GraphDataScience) -> None:
    result = gds.alpha.similarity.jaccard([1, 2, 3], [1, 2, 4, 5])
    assert result == pytest.approx(0.4, 0.01)


def test_similarity_cosine(gds: GraphDataScience) -> None:
    result = gds.alpha.similarity.cosine([3, 8, 7, 5, 2, 9], [10, 8, 6, 6, 4, 5])
    assert result == pytest.approx(0.86, 0.01)


def test_similarity_cosine_stats(gds: GraphDataScience) -> None:
    node1 = {"item": 1, "weights": [42.0, 13.37]}
    node2 = {"item": 2, "weights": [13.37, 42]}

    result = gds.alpha.similarity.cosine.stats(data=[node1, node2])
    assert result[0]["nodes"] == 2
