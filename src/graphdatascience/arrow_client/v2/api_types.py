from graphdatascience.arrow_client.arrow_base_model import ArrowBaseModel


class JobIdConfig(ArrowBaseModel):
    job_id: str


UNKNOWN_PROGRESS = -1


class JobStatus(ArrowBaseModel):
    job_id: str
    status: str
    progress: float
    description: str

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
        return self.status == "Aborted"

    def succeeded(self) -> bool:
        return self.status == "Done"

    def running(self) -> bool:
        return self.status == "Running"


class MutateResult(ArrowBaseModel):
    mutate_millis: int
    node_properties_written: int
    relationships_written: int
