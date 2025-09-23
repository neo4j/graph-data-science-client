from graphdatascience.arrow_client.v2.api_types import UNKNOWN_PROGRESS, JobStatus


def test_job_status() -> None:
    status_with_progress = JobStatus(
        jobId="job-123",
        progress=0.75,
        status="Running",
        description="Main task :: Subtask details",
    )
    assert status_with_progress.progress_known() is True
    assert status_with_progress.progress_percent() == 75.0
    assert status_with_progress.base_task() == "Main task"
    assert status_with_progress.sub_tasks() == "Subtask details"

    status_unknown_progress = JobStatus(
        jobId="job-456",
        progress=UNKNOWN_PROGRESS,
        status="Pending",
        description="Only main task",
    )
    assert status_unknown_progress.progress_known() is False
    assert status_unknown_progress.progress_percent() is None
    assert status_unknown_progress.base_task() == "Only main task"
    assert status_unknown_progress.sub_tasks() is None

    status_without_description = JobStatus(
        jobId="job-789",
        progress=0.5,
        status="Running",
        description="",
    )
    assert status_without_description.base_task() == ""
    assert status_without_description.sub_tasks() is None
