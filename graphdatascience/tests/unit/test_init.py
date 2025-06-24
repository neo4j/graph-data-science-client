from dataclasses import asdict

import pytest
from pandas import DataFrame

from graphdatascience.arrow_client.arrow_info import ArrowInfo
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.server_version.server_version import ServerVersion
from graphdatascience.tests.unit.conftest import CollectingQueryRunner

GDS_INIT_VERSION_TESTDATA = [(2, 10, 0, "2.10.0"), (42, 1337, 99, "42.1337.99"), (4, 5, 6, "4.5.6-alpha2")]


@pytest.mark.parametrize("major, minor, patch, version_string", GDS_INIT_VERSION_TESTDATA)
def test_gds_init_version(major: int, minor: int, patch: int, version_string: str) -> None:
    arrow_info = ArrowInfo(listenAddress="foo.bar", enabled=True, running=True, versions=[])
    query_runner = CollectingQueryRunner(
        ServerVersion.from_string(version_string),
        result_mock=DataFrame([asdict(arrow_info)]),
    )

    gds = GraphDataScience(endpoint=query_runner, arrow=False)

    server_version = gds._server_version
    assert server_version.major == major
    assert server_version.minor == minor
    assert server_version.patch == patch


def test_warn_old_gds_version() -> None:
    arrow_info = ArrowInfo(listenAddress="foo.bar", enabled=True, running=True, versions=[])
    query_runner = CollectingQueryRunner(
        ServerVersion.from_string("2.1.0"),
        result_mock=DataFrame([asdict(arrow_info)]),
    )

    with pytest.warns(DeprecationWarning, match=r"Client does not support the given server version `2.1.0`"):
        GraphDataScience(endpoint=query_runner, arrow=False)


def test_endpoint_as_required_kwparam(runner: CollectingQueryRunner) -> None:
    runner.add__mock_result(
        "gds.debug.arrow",
        DataFrame([asdict(ArrowInfo(listenAddress="foo.bar", enabled=True, running=True, versions=[]))]),
    )
    GraphDataScience(endpoint=runner, arrow=False)

    # Should still be mandatory
    with pytest.raises(TypeError, match=r"__init__\(\) missing 1 required positional argument: 'endpoint'"):
        GraphDataScience()  # type: ignore
