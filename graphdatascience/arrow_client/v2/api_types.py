from graphdatascience.arrow_client.arrow_base_model import ArrowBaseModel


class JobIdConfig(ArrowBaseModel):
    job_id: str


class JobStatus(ArrowBaseModel):
    job_id: str
    status: str
    progress: float


class MutateResult(ArrowBaseModel):
    node_properties_written: int
    relationships_written: int
