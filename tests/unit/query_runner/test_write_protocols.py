from io import StringIO

import pytest
from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.query_runner.protocol.status import Status
from graphdatascience.query_runner.protocol.write_protocols import RemoteWriteBackV3
from graphdatascience.query_runner.termination_flag import TerminationFlagNoop
from graphdatascience.server_version.server_version import ServerVersion
from tests.unit.conftest import CollectingQueryRunner


def test_write_back_v3_progress_logging() -> None:
    with StringIO() as pbarOutputStream:
        qr = CollectingQueryRunner(ServerVersion(0, 0, 0))
        qr.add__mock_result("gds.arrow.write.v3", DataFrame([{"status": Status.COMPLETED.name, "progress": 1.0}]))

        wp = RemoteWriteBackV3(progress_bar_options={"file": pbarOutputStream, "mininterval": 0})

        wp.run_write_back(
            query_runner=qr,
            parameters=CallParameters(graphName="myGraph", jobId="myJob"),
            log_progress=True,
            terminationFlag=TerminationFlagNoop(),
            yields=None,
        )

        bar_output = pbarOutputStream.getvalue().split("\r")

        assert any(
            ["Write-Back (graph: myGraph):   0%|          | 0.0/100 [00:00<?, ?%/s]" in line for line in bar_output]
        ), bar_output
        assert any(["Write-Back (graph: myGraph): 100%|##########| 100.0/100" in line for line in bar_output])


def test_write_back_v3_progress_logging_without_bar() -> None:
    # for self-managed dbs the endpoint doesnt return the progress yet
    with StringIO() as pbarOutputStream:
        qr = CollectingQueryRunner(ServerVersion(0, 0, 0))
        qr.add__mock_result("gds.arrow.write.v3", DataFrame([{"status": Status.COMPLETED.name}]))

        wp = RemoteWriteBackV3(progress_bar_options={"file": pbarOutputStream, "mininterval": 0})

        wp.run_write_back(
            query_runner=qr,
            parameters=CallParameters(graphName="myGraph", jobId="myJob"),
            log_progress=True,
            terminationFlag=TerminationFlagNoop(),
            yields=None,
        )

        bar_output = pbarOutputStream.getvalue().split("\r")

        assert any(
            ["Write-Back (graph: myGraph):   0%|          | 0.0/100 [00:00<?, ?%/s]" in line for line in bar_output]
        ), bar_output
        assert any(["Write-Back (graph: myGraph): 100%|##########| 100.0/100" in line for line in bar_output])


def test_write_back_v3_progress_logging_aborted() -> None:
    with StringIO() as pbarOutputStream:
        qr = CollectingQueryRunner(ServerVersion(0, 0, 0))
        qr.add__mock_result("gds.arrow.write.v3", ValueError("Job aborted"))

        wp = RemoteWriteBackV3(progress_bar_options={"file": pbarOutputStream, "mininterval": 0})

        with pytest.raises(ValueError, match="Job aborted"):
            wp.run_write_back(
                query_runner=qr,
                parameters=CallParameters(graphName="myGraph", jobId="myJob"),
                log_progress=True,
                terminationFlag=TerminationFlagNoop(),
                yields=None,
            )

        bar_output = pbarOutputStream.getvalue().split("\r")

        assert any(
            ["Write-Back (graph: myGraph):   0%|          | 0.0/100 [00:00<?, ?%/s]" in line for line in bar_output]
        ), bar_output
        assert any(["status: FAILED" in line for line in bar_output]), bar_output
