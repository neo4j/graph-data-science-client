from dataclasses import dataclass


@dataclass(frozen=True, repr=True)
class JobIdConfig:
    jobId: str


@dataclass(frozen=True, repr=True)
class JobStatus:
    jobId: str
    status: str
    progress: float


@dataclass(frozen=True, repr=True)
class MutateResult:
    nodePropertiesWritten: int
    relationshipsWritten: int
