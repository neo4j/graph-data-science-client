from .conftest import CollectingQueryRunner
from graphdatascience.graph_data_science import GraphDataScience


def test_listProgress(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.beta.listProgress()

    assert runner.last_query() == "CALL gds.beta.listProgress()"
    assert runner.last_params() == {}

    gds.beta.listProgress("myJobId")

    assert runner.last_query() == "CALL gds.beta.listProgress($job_id)"
    assert runner.last_params() == {"job_id": "myJobId"}


def test_systemMonitor(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.alpha.systemMonitor()

    assert runner.last_query() == "CALL gds.alpha.systemMonitor()"
    assert runner.last_params() == {}


def test_sysInfo(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.debug.sysInfo()

    assert runner.last_query() == "CALL gds.debug.sysInfo()"
    assert runner.last_params() == {}


def test_userLog(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.alpha.userLog()

    assert runner.last_query() == "CALL gds.alpha.userLog()"
    assert runner.last_params() == {}
