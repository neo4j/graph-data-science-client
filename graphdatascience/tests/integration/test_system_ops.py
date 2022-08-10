import pytest

from graphdatascience.graph_data_science import GraphDataScience


def test_listProgress(gds: GraphDataScience) -> None:
    result = gds.beta.listProgress()

    assert len(result) >= 0


@pytest.mark.enterprise
def test_systemMonitor(gds: GraphDataScience) -> None:
    result = gds.alpha.systemMonitor()

    assert result["freeHeap"] >= 0
    assert len(result["ongoingGdsProcedures"]) >= 0


def test_debug_sysInfo(gds: GraphDataScience) -> None:
    result = gds.debug.sysInfo()

    assert "gdsVersion" in (list(result["key"]))


def test_debug_arrow(gds: GraphDataScience) -> None:
    result = gds.debug.arrow()

    assert "listenAddress" in result


@pytest.mark.enterprise
def test_is_licensed(gds: GraphDataScience) -> None:
    assert gds.is_licensed()
