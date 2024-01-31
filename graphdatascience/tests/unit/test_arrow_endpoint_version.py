import pytest

from graphdatascience.graph_data_science import ArrowQueryRunner
from graphdatascience.query_runner.arrow_endpoint_version import (
    ArrowEndpointVersion,
    UnsupportedArrowEndpointVersion,
)


@pytest.mark.parametrize("arrow_version", [ArrowEndpointVersion.V1, ArrowEndpointVersion.ALPHA])
def test_arrow_version_parsing(arrow_version):
    arrow_info = dict()
    arrow_info["version"] = arrow_version.name()
    actual = ArrowQueryRunner._read_arrow_version(arrow_info)
    assert actual == arrow_version


@pytest.mark.parametrize("arrow_version", ["v2", "unsupported"])
def test_arrow_version_parsing_fails(arrow_version):
    arrow_info = dict()
    arrow_info["version"] = arrow_version
    with pytest.raises(UnsupportedArrowEndpointVersion) as e:
        ArrowQueryRunner._read_arrow_version(arrow_info)
    assert arrow_version in str(e.value)


def test_arrow_version_prefix():
    assert ArrowEndpointVersion.ALPHA.prefix() == ""
    assert ArrowEndpointVersion.V1.prefix() == "v1/"
