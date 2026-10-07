from graphdatascience.arrow_client.v2.api_types import UNKNOWN_PROGRESS, JobStatus, StepProgress


def test_step_progress_quantitative() -> None:
    step = StepProgress(current=4, total=9, progress=0.75)

    assert step.current == 4
    assert step.total == 9
    assert step.progress_percent() == 75.0
    assert step.label() == "4/9"


def test_step_progress_qualitative() -> None:
    step = StepProgress(current=2, total=9)

    assert step.progress is None
    assert step.progress_percent() is None
    assert step.label() == "2/9"


def test_job_status_step_progress() -> None:
    status = JobStatus(
        jobId="job-123",
        progress=0.44,
        status="Running",
        description="Algo :: Training epoch 2",
        stepProgress={"current": 5, "total": 9, "progress": 0.5},
    )
    assert status.step_progress is not None
    assert status.step_progress.current == 5
    assert status.step_progress.total == 9
    assert status.step_progress.progress_percent() == 50.0


def test_job_status_without_step_progress() -> None:
    absent = JobStatus(jobId="job-1", progress=0.5, status="Running", description="Algo")
    assert absent.step_progress is None

    explicit_null = JobStatus(jobId="job-2", progress=0.5, status="Running", description="Algo", stepProgress=None)
    assert explicit_null.step_progress is None


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
