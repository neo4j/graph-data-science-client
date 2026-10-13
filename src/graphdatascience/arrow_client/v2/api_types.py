from graphdatascience.arrow_client.arrow_base_model import ArrowBaseModel


class JobIdConfig(ArrowBaseModel):
    job_id: str


UNKNOWN_PROGRESS = -1


class StepProgress(ArrowBaseModel):
    """Progress of a single step within a multi-step job.

    ``progress`` is ``None`` for qualitative steps (e.g. "Fetching graph") and a fraction
    in ``[0, 1]`` for quantitative steps (e.g. "Training epoch 3").
    """

    current: int
    total: int
    progress: float | None = None

    def progress_percent(self) -> float | None:
        if self.progress is None:
            return None
        return self.progress * 100

    def label(self) -> str:
        return f"{self.current}/{self.total}"


class JobStatus(ArrowBaseModel):
    """Status and progress information for a GDS Arrow job."""

    job_id: str
    status: str
    progress: float
    description: str
    step_progress: StepProgress | None = None

    def progress_known(self) -> bool:
        if self.progress == UNKNOWN_PROGRESS:
            return False
        return True

    def progress_percent(self) -> float | None:
        if self.progress_known():
            return self.progress * 100
        return None

    def base_task(self) -> str:
        return self.description.split("::")[0].strip()

    def sub_tasks(self) -> str | None:
        task_split = self.description.split("::", maxsplit=1)
        if len(task_split) > 1:
            return task_split[1].strip()
        return None

    def aborted(self) -> bool:
        return self.status.lower() == "aborted"

    def succeeded(self) -> bool:
        return self.status.lower() == "done"

    def running(self) -> bool:
        return self.status.lower() == "running"


class MutateResult(ArrowBaseModel):
    mutate_millis: int
    node_properties_written: int
    relationships_written: int
