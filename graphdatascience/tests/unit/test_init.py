from dataclasses import asdict

import pytest
from pandas import DataFrame

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.arrow_info import ArrowInfo
from graphdatascience.server_version.server_version import ServerVersion
from graphdatascience.tests.unit.conftest import CollectingQueryRunner

GDS_INIT_VERSION_TESTDATA = [(2, 1, 0, "2.1.0"), (42, 1337, 99, "42.1337.99"), (1, 2, 3, "1.2.3-alpha2")]


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


def test_endpoint_as_required_kwparam(runner: CollectingQueryRunner) -> None:
    runner.add__mock_result(
        "gds.debug.arrow",
        DataFrame([asdict(ArrowInfo(listenAddress="foo.bar", enabled=True, running=True, versions=[]))]),
    )
    GraphDataScience(endpoint=runner, arrow=False)

    # Should still be mandatory
    with pytest.raises(TypeError, match=r"__init__\(\) missing 1 required positional argument: 'endpoint'"):
        GraphDataScience()  # type: ignore
