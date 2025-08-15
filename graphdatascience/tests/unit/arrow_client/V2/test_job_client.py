import json

from pytest_mock import MockerFixture

from graphdatascience.arrow_client.v2.api_types import JobIdConfig, JobStatus
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.tests.unit.arrow_client.arrow_test_utils import ArrowTestResult


def test_run_job(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-123"
    endpoint = "v2/test.endpoint"
    config = {"param1": "value1", "param2": 42}

    mock_client.do_action_with_retry.return_value = iter([ArrowTestResult({"jobId": job_id})])

    result = JobClient.run_job(mock_client, endpoint, config)

    expected_config = json.dumps(config).encode("utf-8")
    mock_client.do_action_with_retry.assert_called_once_with(endpoint, expected_config)
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
    )

    do_action_with_retry = mocker.Mock()
    do_action_with_retry.side_effect = [
        iter([ArrowTestResult(job_id_config.dump_camel())]),
        iter([ArrowTestResult(status.dump_camel())]),
    ]

    mock_client.do_action_with_retry = do_action_with_retry

    result = JobClient.run_job_and_wait(mock_client, endpoint, config)

    do_action_with_retry.assert_called_with("v2/jobs.status", job_id_config.dump_json().encode("utf-8"))
    assert result == job_id


def test_wait_for_job_completes_immediately(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-789"

    status = JobStatus(
        jobId=job_id,
        progress=1.0,
        status="Done",
    )

    mock_client.do_action_with_retry.return_value = iter([ArrowTestResult(status.dump_camel())])

    JobClient.wait_for_job(mock_client, job_id)

    mock_client.do_action_with_retry.assert_called_once_with(
        "v2/jobs.status", JobIdConfig(jobId=job_id).dump_json().encode("utf-8")
    )


def test_wait_for_job_waits_for_completion(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    job_id = "test-job-waiting"
    status_running = JobStatus(
        jobId=job_id,
        progress=0.5,
        status="RUNNING",
    )
    status_done = JobStatus(
        jobId=job_id,
        progress=1.0,
        status="Done",
    )

    do_action_with_retry = mocker.Mock()
    do_action_with_retry.side_effect = [
        iter([ArrowTestResult(status_running.dump_camel())]),
        iter([ArrowTestResult(status_done.dump_camel())]),
    ]

    mock_client.do_action_with_retry = do_action_with_retry

    JobClient.wait_for_job(mock_client, job_id)

    assert mock_client.do_action_with_retry.call_count == 2


def test_get_summary(mocker: MockerFixture) -> None:
    mock_client = mocker.Mock()
    # Setup
    job_id = "summary-job-123"
    expected_summary = {"nodeCount": 100, "relationshipCount": 200, "requiredMemory": "1GB"}

    mock_client.do_action_with_retry.return_value = iter([ArrowTestResult(expected_summary)])

    result = JobClient.get_summary(mock_client, job_id)

    mock_client.do_action_with_retry.assert_called_once_with(
        "v2/results.summary", JobIdConfig(jobId=job_id).dump_json().encode("utf-8")
    )
    assert result == expected_summary
