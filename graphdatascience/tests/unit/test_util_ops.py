from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.tests.unit.conftest import CollectingQueryRunner


def test_version(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    assert gds.version() == f"{gds._server_version}"


def test_list(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.list()

    assert runner.last_query() == "CALL gds.list()"
    assert runner.last_params() == {}
