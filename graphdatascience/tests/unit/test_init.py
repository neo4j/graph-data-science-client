import pytest

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.tests.unit.conftest import CollectingQueryRunner

GDS_INIT_VERSION_TESTDATA = [(2, 1, 0, "2.1.0"), (42, 1337, 99, "42.1337.99"), (1, 2, 3, "1.2.3-alpha2")]


@pytest.mark.parametrize("major, minor, patch, version_string", GDS_INIT_VERSION_TESTDATA)
def test_gds_init_version(major: int, minor: int, patch: int, version_string: str) -> None:
    gds = GraphDataScience(CollectingQueryRunner(version_string), arrow=False)

    server_version = gds._server_version
    assert server_version.major == major
    assert server_version.minor == minor
    assert server_version.patch == patch
