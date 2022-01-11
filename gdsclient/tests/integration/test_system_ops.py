import pytest

from gdsclient.graph_data_science import GraphDataScience


def test_listProgress(gds: GraphDataScience) -> None:
    result = gds.beta.listProgress()

    assert len(result) == 0


@pytest.mark.enterprise
def test_systemMonitor(gds: GraphDataScience) -> None:
    result = gds.alpha.systemMonitor()

    assert result[0]["freeHeap"] >= 0
    assert len(result[0]["ongoingGdsProcedures"]) == 0


def test_sysInfo(gds: GraphDataScience) -> None:
    result = gds.debug.sysInfo()

    assert result[0]["key"] == "gdsVersion"
