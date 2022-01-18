from gdsclient.graph_data_science import GraphDataScience
from gdsclient.tests.unit.conftest import CollectingQueryRunner


def test_version(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.version()

    assert runner.last_query() == "RETURN gds.version() as version"
    assert runner.last_params() == {}
