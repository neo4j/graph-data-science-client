from __future__ import annotations

from abc import ABC, abstractmethod

from graphdatascience.procedure_surface.api.base_result import BaseResult


class LicenseEndpoints(ABC):
    @abstractmethod
    def state(self) -> LicenseStateResult:
        """
        Return the licensing state of the installed GDS library.

        Returns
        -------
        LicenseStateResult
            Whether the library is licensed and additional license details.
        """


class LicenseStateResult(BaseResult):
    details: str
    is_licensed: bool
