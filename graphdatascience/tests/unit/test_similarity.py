from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.tests.unit.conftest import CollectingQueryRunner


def test_similarity_stats(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.alpha.similarity.cosine.stats(hello=0.2, you=42)

    assert runner.last_query() == "CALL gds.alpha.similarity.cosine.stats($config)"
    assert runner.last_params() == {
        "config": {"hello": 0.2, "you": 42},
    }


def test_similarity_stream(
    runner: CollectingQueryRunner, gds: GraphDataScience
) -> None:
    gds.alpha.similarity.pearson.stream(hello=0.2, you=42)

    assert runner.last_query() == "CALL gds.alpha.similarity.pearson.stream($config)"
    assert runner.last_params() == {
        "config": {"hello": 0.2, "you": 42},
    }


def test_similarity_write(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.alpha.similarity.euclidean.write(hello=0.2, you=42)

    assert runner.last_query() == "CALL gds.alpha.similarity.euclidean.write($config)"
    assert runner.last_params() == {
        "config": {"hello": 0.2, "you": 42},
    }
