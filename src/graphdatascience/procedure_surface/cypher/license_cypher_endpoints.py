from __future__ import annotations

import re

from graphdatascience.procedure_surface.api.license_endpoints import LicenseEndpoints, LicenseStateResult
from graphdatascience.query_runner.query_runner import QueryRunner


class LicenseCypherEndpoints(LicenseEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def state(self) -> LicenseStateResult:
        try:
            result = self._query_runner.call_procedure(endpoint="gds.license.state").squeeze()
        except Exception as e:
            # AuraDS does not have `gds.license.state`, but is always GDS EE.
            if re.match(r"There is no procedure with the name `gds.*` registered for this database instance", str(e)):
                return LicenseStateResult(details="AuraDS", isLicensed=True)
            raise

        return LicenseStateResult(**result.to_dict())
