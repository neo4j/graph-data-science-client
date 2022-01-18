import pytest

from gdsclient.graph_data_science import GraphDataScience


def test_similarity_jaccard(gds: GraphDataScience) -> None:
    result = gds.alpha.similarity.jaccard([1, 2, 3], [1, 2, 4, 5])
    assert result == pytest.approx(0.4, 0.01)
