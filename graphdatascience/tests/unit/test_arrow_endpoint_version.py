import pytest

from graphdatascience.graph_data_science import ArrowQueryRunner
from graphdatascience.query_runner.arrow_endpoint_version import (
    ArrowEndpointVersion,
    UnsupportedArrowEndpointVersion,
)


@pytest.mark.parametrize(
    "arrow_versions",
    [
        (ArrowEndpointVersion.ALPHA, []),
        (ArrowEndpointVersion.ALPHA, ["alpha"]),
        (ArrowEndpointVersion.V1, ["v1"]),
        (ArrowEndpointVersion.V1, ["alpha", "v1"]),
        (ArrowEndpointVersion.V1, ["v1", "v2"]),
        (ArrowEndpointVersion.ALPHA, ["alpha"]),
        (ArrowEndpointVersion.ALPHA, ["alpha", "v2"]),
    ],
)
def test_from_arrow_info_multiple_versions(arrow_versions):
    arrow_info = dict()
    arrow_info["versions"] = arrow_versions[1]
    actual = ArrowEndpointVersion.from_arrow_info(arrow_info)
    assert actual == arrow_versions[0]


@pytest.mark.parametrize("arrow_version", ["v2", "unsupported"])
def test_from_arrow_info_fails(arrow_version):
    arrow_info = dict()
    arrow_info["versions"] = [arrow_version]
    with pytest.raises(UnsupportedArrowEndpointVersion) as e:
        ArrowEndpointVersion.from_arrow_info(arrow_info)
    assert "Unsupported" in str(e.value)
    assert arrow_version in str(e.value)


def test_prefix():
    assert ArrowEndpointVersion.ALPHA.prefix() == ""
    assert ArrowEndpointVersion.V1.prefix() == "v1/"
