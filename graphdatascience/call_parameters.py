from collections import OrderedDict
from typing import Any, Optional
from uuid import uuid4


class CallParameters(OrderedDict[str, Any]):
    # since Python 3.6 also initializing through CallParameters(**kwargs) is order preserving

    def placeholder_str(self) -> str:
        return ", ".join([f"${k}" for k in self.keys()])

    def get_job_id(self) -> Optional[str]:
        config = self["config"] if "config" in self else {}

        job_id = None
        if "jobId" in config:
            job_id = config["jobId"]

        if "job_id" in config:
            job_id = config["job_id"]

        if job_id is None:
            return None

        return str(job_id)

    def ensure_job_id_in_config(self) -> str:
        """
        Ensure that a job id is present in the `config` parameter. If not, generate a new one.
        This enables the client to check on the progress later on.
        """
        config = self.get("config")
        if not config:
            raise ValueError("config is not set in the parameters")

        job_id = self.get_job_id()
        if job_id is None:
            job_id = str(uuid4())
            config["jobId"] = job_id
        return job_id
