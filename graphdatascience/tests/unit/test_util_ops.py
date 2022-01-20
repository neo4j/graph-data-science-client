from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.tests.unit.conftest import CollectingQueryRunner


def test_version(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.version()

    assert runner.last_query() == "RETURN gds.version() as version"
    assert runner.last_params() == {}


def test_list(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.list()

    assert runner.last_query() == "CALL gds.list()"
    assert runner.last_params() == {}
