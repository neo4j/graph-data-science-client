import json

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.api_types import MutateResult
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize_single


class MutationClient:
    MUTATE_ENDPOINT = "v2/results.mutate"

    @staticmethod
    def mutate_node_property(client: AuthenticatedArrowClient, job_id: str, mutate_property: str) -> MutateResult:
        mutate_config = {"jobId": job_id, "mutateProperty": mutate_property}
        encoded_config = json.dumps(mutate_config).encode("utf-8")
        mutate_arrow_res = client.do_action_with_retry(MutationClient.MUTATE_ENDPOINT, encoded_config)
        return MutateResult(**deserialize_single(mutate_arrow_res))
