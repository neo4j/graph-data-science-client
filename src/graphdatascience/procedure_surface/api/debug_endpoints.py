from __future__ import annotations

from abc import ABC, abstractmethod

from pydantic import Field

from graphdatascience.procedure_surface.api.base_result import BaseResult


class DebugEndpoints(ABC):
    @abstractmethod
    def sys_info(self) -> DebugSysInfoResult:
        """
        Return system and debugging information about the installed GDS library.

        Not available in AuraDS.

        Returns
        -------
        DebugSysInfoResult
            System and build information about the installed GDS library.
        """


class DebugSysInfoResult(BaseResult):
    """A curated, stable subset of the key/value pairs returned by ``gds.debug.sysInfo``.

    The procedure returns many more (version- and JVM-dependent) entries, such as
    memory-pool details; those are ignored here.
    """

    gds_version: str | None = None
    gds_edition: str | None = None
    neo4j_version: str | None = None
    minimum_required_java_version: int | None = None
    build_date: str | None = None
    build_jdk: str | None = None
    build_java_version: str | None = None
    build_hash: str | None = None
    available_cpus: int | None = Field(default=None, alias="availableCPUs")
    physical_cpus: int | None = Field(default=None, alias="physicalCPUs")
    available_heap_in_bytes: int | None = None
    heap_free_in_bytes: int | None = None
    heap_total_in_bytes: int | None = None
    heap_max_in_bytes: int | None = None
    vm_name: str | None = None
    vm_version: str | None = None
    vm_compiler: str | None = None
    containerized: bool | None = None
