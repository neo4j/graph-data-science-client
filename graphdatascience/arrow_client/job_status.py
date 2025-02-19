from dataclasses import dataclass

from graphdatascience import JobId


@dataclass(repr=True, frozen=True)
class JobStatus:
    jobId: JobId
    status: str
    progress: float

    @staticmethod
    def from_json(json: dict[str, str]):
        return JobStatus(
            jobId=JobId(json["jobId"]),
            status=json["status"],
            progress=float(json["progress"]),
        )