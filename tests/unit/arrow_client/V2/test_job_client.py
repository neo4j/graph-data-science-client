from io import StringIO
from typing import Any
from unittest.mock import Mock

import pytest
from pyarrow import ArrowKeyError
from pyarrow.flight import FlightTimedOutError
from pytest_mock import MockerFixture

from graphdatascience.arrow_client.v2.api_types import UNKNOWN_PROGRESS, JobIdConfig, JobStatus
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.query_runner.termination_flag import TerminationFlag
from tests.unit.arrow_client.arrow_test_utils import ArrowTestResult


def stub_action_responses(mocker: MockerFixture, mock_client: Mock, responses: list[Any]) -> None:
    """Queue one response per ``do_action_with_retry`` call.

    Each entry is either a ``JobStatus``/dict poll result or an exception instance to raise.
    """
    mock_client.do_action_with_retry = mocker.Mock(
        side_effect=[r if isinstance(r, BaseException) else iter([ArrowTestResult(_dump(r))]) for r in responses]
    )


def _dump(response: JobStatus | dict[str, Any]) -> dict[str, Any]:
    return response.dump_camel() if isinstance(response, JobStatus) else response


def test_run_job(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-123"
    endpoint = "v2/test.endpoint"
    config = {"param1": "value1", "param2": 42}

    stub_action_responses(mocker, mock_client, [{"jobId": job_id}])

    result = JobClient.run_job(mock_client, endpoint, config)

    mock_client.do_action_with_retry.assert_called_once_with(endpoint, config)
    assert result == job_id


def test_run_job_and_wait(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-456"
    endpoint = "v2/test.endpoint"
    config = {"param": "value"}

    job_id_config = JobIdConfig(jobId=job_id)
    status = JobStatus(jobId=job_id, progress=1.0, status="Done", description="")

    stub_action_responses(mocker, mock_client, [job_id_config.dump_camel(), status])

    result = JobClient().run_job_and_wait(mock_client, endpoint, config, show_progress=False)

    mock_client.do_action_with_retry.assert_called_with("v2/jobs.status", job_id_config.dump_camel())
    assert result == job_id


def test_wait_for_job_completes_immediately(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-789"

    status = JobStatus(jobId=job_id, progress=1.0, status="Done", description="")

    stub_action_responses(mocker, mock_client, [status])

    JobClient().wait_for_job(mock_client, job_id, show_progress=False)

    mock_client.do_action_with_retry.assert_called_once_with("v2/jobs.status", JobIdConfig(jobId=job_id).dump_camel())


def test_wait_for_job_waits_for_completion(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-waiting"
    status_running = JobStatus(jobId=job_id, progress=0.5, status="RUNNING", description="")
    status_done = JobStatus(jobId=job_id, progress=1.0, status="Done", description="")

    stub_action_responses(mocker, mock_client, [status_running, status_done])

    JobClient().wait_for_job(mock_client, job_id, show_progress=False)

    assert mock_client.do_action_with_retry.call_count == 2


def test_wait_for_job_waits_for_expected_status(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-waiting"
    status_running = JobStatus(jobId=job_id, progress=0.5, status="RUNNING", description="")
    status_done = JobStatus(jobId=job_id, progress=1.0, status="RELATIONSHIP_LOADING", description="")

    stub_action_responses(mocker, mock_client, [status_running, status_done])

    JobClient().wait_for_job(mock_client, job_id, show_progress=False, expected_status="RELATIONSHIP_LOADING")

    assert mock_client.do_action_with_retry.call_count == 2


def test_wait_for_job_waits_for_aborted(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-waiting"
    status_running = JobStatus(jobId=job_id, progress=0.5, status="RUNNING", description="")
    status_done = JobStatus(jobId=job_id, progress=1.0, status="Aborted", description="")

    stub_action_responses(mocker, mock_client, [status_running, status_done])

    JobClient().wait_for_job(mock_client, job_id, show_progress=False)

    assert mock_client.do_action_with_retry.call_count == 2


def test_wait_for_job_stops_on_interrupt(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-waiting"
    stub_action_responses(mocker, mock_client, [])

    termination_flag = TerminationFlag.create()
    termination_flag.set()

    with pytest.raises(
        RuntimeError, match="Closing client connection. Note, the query will be continued on the server-side"
    ):
        JobClient().wait_for_job(mock_client, job_id, show_progress=False, termination_flag=termination_flag)

    assert mock_client.do_action_with_retry.call_count == 0


def test_wait_for_job_progress_bar_quantive(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-progress"
    status_running = JobStatus(jobId=job_id, progress=0.5, status="RUNNING", description="Algo :: Halfway there")
    status_done = JobStatus(jobId=job_id, progress=1.0, status="Done", description="Algo")

    stub_action_responses(mocker, mock_client, [status_running, status_done])

    with StringIO() as pbarOutputStream:
        client = JobClient(progress_bar_options={"file": pbarOutputStream, "mininterval": 0, "ascii": True})
        client.wait_for_job(mock_client, job_id, show_progress=True)

        progress_output = pbarOutputStream.getvalue().split("\r")
        assert "Algo:  50%|#####     | 50.0/100 [00:00<?, ?%/s]" in progress_output
        assert any("Algo: 100%|##########| 100.0/100" in line for line in progress_output)


def test_wait_for_job_progress_bar_qualitative(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-progress"
    status_initial = JobStatus(jobId=job_id, progress=UNKNOWN_PROGRESS, status="RUNNING", description="Algo")
    status_running = JobStatus(
        jobId=job_id, progress=UNKNOWN_PROGRESS, status="RUNNING", description="Algo :: Halfway there"
    )
    status_done = JobStatus(jobId=job_id, progress=UNKNOWN_PROGRESS, status="Done", description="Algo")

    stub_action_responses(mocker, mock_client, [status_initial, status_running, status_done])

    with StringIO() as pbarOutputStream:
        client = JobClient(progress_bar_options={"file": pbarOutputStream, "mininterval": 0})
        client.wait_for_job(mock_client, job_id, show_progress=True)

        progress_output = pbarOutputStream.getvalue().split("\r")
        assert "Algo [elapsed: 00:00 ]" in progress_output
        assert "Algo [elapsed: 00:00 , status: RUNNING, task: Halfway there]" in progress_output
        assert any("Algo [elapsed: 00:00 , status: FINISHED]" in line for line in progress_output)


def test_wait_for_job_progress_bar_with_steps(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-steps"
    statuses = [
        JobStatus(
            jobId=job_id,
            progress=0.33,
            status="RUNNING",
            description="GS :: Training epoch 1",
            stepProgress={"current": 2, "total": 5, "progress": 0.0},
        ),
        JobStatus(
            jobId=job_id,
            progress=0.4,
            status="RUNNING",
            description="GS :: Training epoch 1",
            stepProgress={"current": 2, "total": 5, "progress": 0.5},
        ),
        JobStatus(
            jobId=job_id,
            progress=0.5,
            status="RUNNING",
            description="GS :: Training epoch 2",
            stepProgress={"current": 3, "total": 5, "progress": 0.0},
        ),
        JobStatus(
            jobId=job_id,
            progress=1.0,
            status="Done",
            description="GS :: Processing results",
            stepProgress={"current": 5, "total": 5, "progress": 1.0},
        ),
    ]

    stub_action_responses(mocker, mock_client, statuses)

    with StringIO() as pbarOutputStream:
        client = JobClient(progress_bar_options={"file": pbarOutputStream, "mininterval": 0, "ascii": True})
        client.wait_for_job(mock_client, job_id, show_progress=True)

        output = pbarOutputStream.getvalue()
        assert "step: 2/5, task: Training epoch 1" in output
        assert "step: 3/5, task: Training epoch 2" in output
        # The terminal poll's step is rendered (as its own bar) and kept on the final line.
        assert "status: Done, step: 5/5, task: Processing results" in output
        assert "status: FINISHED, step: 5/5, task: Processing results" in output


def test_wait_for_job_replaces_bar_when_step_changes(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-steps"
    statuses = [
        JobStatus(
            jobId=job_id,
            progress=0.2,
            status="RUNNING",
            description="Algo :: walks",
            stepProgress={"current": 1, "total": 3},
        ),
        JobStatus(
            jobId=job_id,
            progress=0.2,
            status="RUNNING",
            description="Algo :: walks",
            stepProgress={"current": 1, "total": 3},
        ),
        JobStatus(
            jobId=job_id,
            progress=0.6,
            status="RUNNING",
            description="Algo :: epoch 1",
            stepProgress={"current": 2, "total": 3, "progress": 0.0},
        ),
        JobStatus(
            jobId=job_id,
            progress=1.0,
            status="Done",
            description="Algo :: results",
            stepProgress={"current": 3, "total": 3, "progress": 1.0},
        ),
    ]

    stub_action_responses(mocker, mock_client, statuses)

    bar_cls = mocker.patch("graphdatascience.arrow_client.v2.job_client.TqdmProgressBar")
    bars = [mocker.Mock(), mocker.Mock(), mocker.Mock()]
    bar_cls.side_effect = bars

    JobClient().wait_for_job(mock_client, job_id, show_progress=True)

    # step 1 stays qualitative for two polls (one bar, updated in place), step 2 replaces it,
    # and the terminal step 3 replaces step 2 to surface the final step on the closing bar.
    assert bar_cls.call_count == 3
    assert bars[0].update.call_count == 2
    bars[0].close.assert_called_once()
    bars[1].close.assert_called_once()
    bars[2].close.assert_not_called()
    bars[2].finish.assert_called_once_with(success=True, step="3/3", sub_tasks_description="results")


def test_wait_for_job_replaces_bar_when_step_mode_changes(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-mode"
    statuses = [
        JobStatus(
            jobId=job_id,
            progress=0.33,
            status="RUNNING",
            description="FP :: Computing event vectors",
            stepProgress={"current": 2, "total": 3},
        ),
        JobStatus(
            jobId=job_id,
            progress=0.5,
            status="RUNNING",
            description="FP :: Computing embeddings",
            stepProgress={"current": 2, "total": 3, "progress": 0.5},
        ),
        JobStatus(
            jobId=job_id,
            progress=1.0,
            status="Done",
            description="FP :: Processing results",
            stepProgress={"current": 3, "total": 3, "progress": 1.0},
        ),
    ]

    stub_action_responses(mocker, mock_client, statuses)

    bar_cls = mocker.patch("graphdatascience.arrow_client.v2.job_client.TqdmProgressBar")
    bars = [mocker.Mock(), mocker.Mock(), mocker.Mock()]
    bar_cls.side_effect = bars

    JobClient().wait_for_job(mock_client, job_id, show_progress=True)

    # Same step number but qualitative -> quantitative replaces the bar, and the terminal
    # step 3 replaces it again to surface the final step on the closing bar.
    assert bar_cls.call_count == 3
    assert bar_cls.call_args_list[0].kwargs["relative_progress"] is None
    assert bar_cls.call_args_list[1].kwargs["relative_progress"] == 50.0
    assert bar_cls.call_args_list[2].kwargs["relative_progress"] == 100.0
    bars[0].close.assert_called_once()
    bars[1].close.assert_called_once()
    bars[2].finish.assert_called_once_with(success=True, step="3/3", sub_tasks_description="Processing results")


def test_wait_for_job_without_step_progress_uses_single_bar(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-legacy"
    statuses = [
        JobStatus(jobId=job_id, progress=0.5, status="RUNNING", description="Algo :: Halfway there"),
        JobStatus(jobId=job_id, progress=1.0, status="Done", description="Algo"),
    ]

    stub_action_responses(mocker, mock_client, statuses)

    bar_cls = mocker.patch("graphdatascience.arrow_client.v2.job_client.TqdmProgressBar")
    bars = [mocker.Mock()]
    bar_cls.side_effect = bars

    JobClient().wait_for_job(mock_client, job_id, show_progress=True)

    # The terminal poll reuses the existing bar (no step to replace) and closes it with no step.
    assert bar_cls.call_count == 1
    assert bars[0].update.call_count == 2
    bars[0].close.assert_not_called()
    bars[0].finish.assert_called_once_with(success=True, step=None, sub_tasks_description=None)


def test_get_summary(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "summary-job-123"
    expected_summary = {"nodeCount": 100, "relationshipCount": 200, "requiredMemory": "1GB"}

    stub_action_responses(mocker, mock_client, [expected_summary])

    result = JobClient.get_summary(mock_client, job_id)

    mock_client.do_action_with_retry.assert_called_once_with(
        "v2/results.summary", JobIdConfig(jobId=job_id).dump_camel()
    )
    assert result == expected_summary


def test_run_job_recovers_when_job_already_started(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "client-job-id"
    endpoint = "v2/test.endpoint"
    config = {"jobId": job_id, "param": "value"}

    status = JobStatus(jobId=job_id, progress=0.5, status="RUNNING", description="")

    stub_action_responses(mocker, mock_client, [FlightTimedOutError("connection lost"), status])

    result = JobClient.run_job(mock_client, endpoint, config)

    assert result == job_id
    assert mock_client.do_action_with_retry.call_count == 2


def test_run_job_retries_when_job_not_started(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "client-job-id"
    endpoint = "v2/test.endpoint"
    config = {"jobId": job_id, "param": "value"}

    stub_action_responses(
        mocker, mock_client, [FlightTimedOutError("connection lost"), ArrowKeyError("job not found"), {"jobId": job_id}]
    )

    result = JobClient.run_job(mock_client, endpoint, config)

    assert result == job_id
    run_calls = [c for c in mock_client.do_action_with_retry.call_args_list if c.args[0] == endpoint]
    status_calls = [c for c in mock_client.do_action_with_retry.call_args_list if c.args[0] == "v2/jobs.status"]
    assert len(run_calls) == 2
    assert len(status_calls) == 1


def test_run_job_raises_after_max_attempts(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "client-job-id"
    endpoint = "v2/test.endpoint"
    config = {"jobId": job_id, "param": "value"}

    stub_action_responses(
        mocker,
        mock_client,
        [
            FlightTimedOutError("timeout 1"),
            ArrowKeyError("job not found 1"),
            FlightTimedOutError("timeout 2"),
            ArrowKeyError("job not found 2"),
            FlightTimedOutError("timeout 3"),
            ArrowKeyError("job not found 3"),
        ],
    )

    with pytest.raises(FlightTimedOutError):
        JobClient.run_job(mock_client, endpoint, config)

    run_calls = [c for c in mock_client.do_action_with_retry.call_args_list if c.args[0] == endpoint]
    status_calls = [c for c in mock_client.do_action_with_retry.call_args_list if c.args[0] == "v2/jobs.status"]
    assert len(run_calls) == 3
    assert len(status_calls) == 3


def test_run_job_generates_job_id_and_retries(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    endpoint = "v2/test.endpoint"
    config = {"param": "value"}

    stub_action_responses(
        mocker, mock_client, [FlightTimedOutError("timeout 1"), ArrowKeyError("job not found"), {"jobId": "server-job"}]
    )

    result = JobClient.run_job(mock_client, endpoint, config)

    assert result == "server-job"
    assert "jobId" in config
    run_calls = [c for c in mock_client.do_action_with_retry.call_args_list if c.args[0] == endpoint]
    status_calls = [c for c in mock_client.do_action_with_retry.call_args_list if c.args[0] == "v2/jobs.status"]
    assert len(run_calls) == 2
    assert len(status_calls) == 1
