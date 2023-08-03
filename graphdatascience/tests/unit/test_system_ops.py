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


def test_set_defaults(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.alpha.config.defaults.set("concurrency", 2, "bob")

    assert runner.last_query() == "CALL gds.alpha.config.defaults.set($key, $value, $username)"
    assert runner.last_params() == {"key": "concurrency", "value": 2, "username": "bob"}


def test_list_defaults(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.alpha.config.defaults.list(username="bob", key="concurrency")

    assert runner.last_query() == "CALL gds.alpha.config.defaults.list($config)"
    assert runner.last_params() == {"config": {"username": "bob", "key": "concurrency"}}


def test_alpha_backup(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.alpha.backup(concurrency=4)

    assert runner.last_query() == "CALL gds.alpha.backup($config)"
    assert runner.last_params() == {"config": {"concurrency": 4}}

def test_backup(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.backup(concurrency=4)

    assert runner.last_query() == "CALL gds.backup($config)"
    assert runner.last_params() == {"config": {"concurrency": 4}}


def test_alpha_restore(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.alpha.restore(concurrency=4)

    assert runner.last_query() == "CALL gds.alpha.restore($config)"
    assert runner.last_params() == {"config": {"concurrency": 4}}

def test_restore(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.restore(concurrency=4)

    assert runner.last_query() == "CALL gds.restore($config)"
    assert runner.last_params() == {"config": {"concurrency": 4}}


def test_license_state(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.license.state()

    assert runner.last_query() == "CALL gds.license.state()"
    assert runner.last_params() == {}
