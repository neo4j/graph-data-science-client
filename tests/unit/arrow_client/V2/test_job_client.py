from io import StringIO

import pytest
from pyarrow import ArrowKeyError
from pyarrow.flight import FlightTimedOutError
from pytest_mock import MockerFixture

from graphdatascience.arrow_client.v2.api_types import UNKNOWN_PROGRESS, JobIdConfig, JobStatus
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.query_runner.termination_flag import TerminationFlag
from tests.unit.arrow_client.arrow_test_utils import ArrowTestResult


def test_run_job(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-123"
    endpoint = "v2/test.endpoint"
    config = {"param1": "value1", "param2": 42}

    mock_client.do_action_with_retry.return_value = iter([ArrowTestResult({"jobId": job_id})])

    result = JobClient.run_job(mock_client, endpoint, config)

    mock_client.do_action_with_retry.assert_called_once_with(endpoint, config)
    assert result == job_id


def test_run_job_and_wait(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-456"
    endpoint = "v2/test.endpoint"
    config = {"param": "value"}

    job_id_config = JobIdConfig(jobId=job_id)

    status = JobStatus(
        jobId=job_id,
        progress=1.0,
        status="Done",
        description="",
    )

    do_action_with_retry = mocker.Mock()
    do_action_with_retry.side_effect = [
        iter([ArrowTestResult(job_id_config.dump_camel())]),
        iter([ArrowTestResult(status.dump_camel())]),
    ]

    mock_client.do_action_with_retry = do_action_with_retry

    result = JobClient().run_job_and_wait(mock_client, endpoint, config, show_progress=False)

    do_action_with_retry.assert_called_with("v2/jobs.status", job_id_config.dump_camel())
    assert result == job_id


def test_wait_for_job_completes_immediately(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-789"

    status = JobStatus(
        jobId=job_id,
        progress=1.0,
        status="Done",
        description="",
    )

    mock_client.do_action_with_retry.return_value = iter([ArrowTestResult(status.dump_camel())])

    JobClient().wait_for_job(mock_client, job_id, show_progress=False)

    mock_client.do_action_with_retry.assert_called_once_with("v2/jobs.status", JobIdConfig(jobId=job_id).dump_camel())


def test_wait_for_job_waits_for_completion(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-waiting"
    status_running = JobStatus(
        jobId=job_id,
        progress=0.5,
        status="RUNNING",
        description="",
    )
    status_done = JobStatus(
        jobId=job_id,
        progress=1.0,
        status="Done",
        description="",
    )

    do_action_with_retry = mocker.Mock()
    do_action_with_retry.side_effect = [
        iter([ArrowTestResult(status_running.dump_camel())]),
        iter([ArrowTestResult(status_done.dump_camel())]),
    ]

    mock_client.do_action_with_retry = do_action_with_retry

    JobClient().wait_for_job(mock_client, job_id, show_progress=False)

    assert mock_client.do_action_with_retry.call_count == 2


def test_wait_for_job_waits_for_expected_status(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-waiting"
    status_running = JobStatus(
        jobId=job_id,
        progress=0.5,
        status="RUNNING",
        description="",
    )
    status_done = JobStatus(
        jobId=job_id,
        progress=1.0,
        status="RELATIONSHIP_LOADING",
        description="",
    )

    do_action_with_retry = mocker.Mock()
    do_action_with_retry.side_effect = [
        iter([ArrowTestResult(status_running.dump_camel())]),
        iter([ArrowTestResult(status_done.dump_camel())]),
    ]

    mock_client.do_action_with_retry = do_action_with_retry

    JobClient().wait_for_job(mock_client, job_id, show_progress=False, expected_status="RELATIONSHIP_LOADING")

    assert mock_client.do_action_with_retry.call_count == 2


def test_wait_for_job_waits_for_aborted(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-waiting"
    status_running = JobStatus(
        jobId=job_id,
        progress=0.5,
        status="RUNNING",
        description="",
    )
    status_done = JobStatus(
        jobId=job_id,
        progress=1.0,
        status="Aborted",
        description="",
    )

    do_action_with_retry = mocker.Mock()
    do_action_with_retry.side_effect = [
        iter([ArrowTestResult(status_running.dump_camel())]),
        iter([ArrowTestResult(status_done.dump_camel())]),
    ]

    mock_client.do_action_with_retry = do_action_with_retry

    JobClient().wait_for_job(mock_client, job_id, show_progress=False)

    assert mock_client.do_action_with_retry.call_count == 2


def test_wait_for_job_stops_on_interrupt(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-waiting"
    mock_client.do_action_with_retry = mocker.Mock()

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

    do_action_with_retry = mocker.Mock()
    do_action_with_retry.side_effect = [
        iter([ArrowTestResult(status_running.dump_camel())]),
        iter([ArrowTestResult(status_done.dump_camel())]),
    ]

    mock_client.do_action_with_retry = do_action_with_retry

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

    do_action_with_retry = mocker.Mock()
    do_action_with_retry.side_effect = [
        iter([ArrowTestResult(status_initial.dump_camel())]),
        iter([ArrowTestResult(status_running.dump_camel())]),
        iter([ArrowTestResult(status_done.dump_camel())]),
    ]

    mock_client.do_action_with_retry = do_action_with_retry

    with StringIO() as pbarOutputStream:
        client = JobClient(progress_bar_options={"file": pbarOutputStream, "mininterval": 0})
        client.wait_for_job(mock_client, job_id, show_progress=True)

        progress_output = pbarOutputStream.getvalue().split("\r")
        assert "Algo [elapsed: 00:00 ]" in progress_output
        assert "Algo [elapsed: 00:00 , status: RUNNING, task: Halfway there]" in progress_output
        assert any("Algo [elapsed: 00:00 , status: FINISHED]" in line for line in progress_output)


def test_get_summary(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    # Setup
    job_id = "summary-job-123"
    expected_summary = {"nodeCount": 100, "relationshipCount": 200, "requiredMemory": "1GB"}

    mock_client.do_action_with_retry.return_value = iter([ArrowTestResult(expected_summary)])

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

    do_action_with_retry = mocker.Mock()
    do_action_with_retry.side_effect = [
        FlightTimedOutError("connection lost"),
        iter([ArrowTestResult(status.dump_camel())]),
    ]
    mock_client.do_action_with_retry = do_action_with_retry

    result = JobClient.run_job(mock_client, endpoint, config)

    assert result == job_id
    assert do_action_with_retry.call_count == 2


def test_run_job_retries_when_job_not_started(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "client-job-id"
    endpoint = "v2/test.endpoint"
    config = {"jobId": job_id, "param": "value"}

    do_action_with_retry = mocker.Mock()
    do_action_with_retry.side_effect = [
        FlightTimedOutError("connection lost"),
        ArrowKeyError("job not found"),
        iter([ArrowTestResult({"jobId": job_id})]),
    ]
    mock_client.do_action_with_retry = do_action_with_retry

    result = JobClient.run_job(mock_client, endpoint, config)

    assert result == job_id
    run_calls = [c for c in do_action_with_retry.call_args_list if c.args[0] == endpoint]
    status_calls = [c for c in do_action_with_retry.call_args_list if c.args[0] == "v2/jobs.status"]
    assert len(run_calls) == 2
    assert len(status_calls) == 1


def test_run_job_raises_after_max_attempts(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "client-job-id"
    endpoint = "v2/test.endpoint"
    config = {"jobId": job_id, "param": "value"}

    do_action_with_retry = mocker.Mock()
    do_action_with_retry.side_effect = [
        FlightTimedOutError("timeout 1"),
        ArrowKeyError("job not found 1"),
        FlightTimedOutError("timeout 2"),
        ArrowKeyError("job not found 2"),
        FlightTimedOutError("timeout 3"),
        ArrowKeyError("job not found 3"),
    ]
    mock_client.do_action_with_retry = do_action_with_retry

    with pytest.raises(FlightTimedOutError):
        JobClient.run_job(mock_client, endpoint, config)

    run_calls = [c for c in do_action_with_retry.call_args_list if c.args[0] == endpoint]
    status_calls = [c for c in do_action_with_retry.call_args_list if c.args[0] == "v2/jobs.status"]
    assert len(run_calls) == 3
    assert len(status_calls) == 3


def test_run_job_generates_job_id_and_retries(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    endpoint = "v2/test.endpoint"
    config = {"param": "value"}

    do_action_with_retry = mocker.Mock()
    do_action_with_retry.side_effect = [
        FlightTimedOutError("timeout 1"),
        ArrowKeyError("job not found"),
        iter([ArrowTestResult({"jobId": "server-job"})]),
    ]
    mock_client.do_action_with_retry = do_action_with_retry

    result = JobClient.run_job(mock_client, endpoint, config)

    assert result == "server-job"
    assert "jobId" in config
    run_calls = [c for c in do_action_with_retry.call_args_list if c.args[0] == endpoint]
    status_calls = [c for c in do_action_with_retry.call_args_list if c.args[0] == "v2/jobs.status"]
    assert len(run_calls) == 2
    assert len(status_calls) == 1
