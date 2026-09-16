from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class JobStatus:
    """Protocol-agnostic snapshot of a write-back job's state."""

    done: bool
    status: str
    progress: float
    written_node_properties: int
    written_node_labels: int
    written_relationships: int


class WriteProtocol(ABC):
    @abstractmethod
    def start_job(
        self,
        graph_name: str,
        job_id: str,
        concurrency: int | None = None,
        property_overwrites: dict[str, str] | None = None,
        relationship_type_overwrite: str | None = None,
        log_progress: bool = True,
    ) -> None:
        """Initial call to start the write-back job. No-op for protocols that combine start+poll."""

    @abstractmethod
    def get_status(self, job_id: str) -> JobStatus:
        """Fetch the current state of the write-back job and normalize it."""
