import datetime
import json
import re
from typing import Any, Optional

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize
from graphdatascience.model.v2.model_api import ModelApi
from graphdatascience.model.v2.model_info import ModelDetails


class ModelApiArrow(ModelApi):
    def __init__(self, arrow_client: AuthenticatedArrowClient):
        self._arrow_client: AuthenticatedArrowClient = arrow_client
        super().__init__()

    def exists(self, model: str) -> bool:
        raw_result = self._arrow_client.do_action_with_retry(
            "v2/model.exists", payload=json.dumps({"modelName": model}).encode("utf-8")
        )
        result = deserialize(raw_result)

        if not result:
            return False

        return True

    def get(self, model: str) -> ModelDetails:
        raw_result = self._arrow_client.do_action_with_retry(
            "v2/model.get", payload=json.dumps({"modelName": model}).encode("utf-8")
        )
        result = deserialize(raw_result)

        if not result:
            raise ValueError(f"There is no '{model}' in the model catalog")

        return self._parse_model_details(result[0])

    def drop(self, model: str, fail_if_missing: bool) -> Optional[ModelDetails]:
        raw_result = self._arrow_client.do_action_with_retry(
            "v2/model.drop", payload=json.dumps({"modelName": model, "failIfMissing": fail_if_missing}).encode("utf-8")
        )
        result = deserialize(raw_result)

        if not result:
            return None

        return self._parse_model_details(result[0])

    def _parse_model_details(self, input: dict[str, Any]) -> ModelDetails:
        creation_time = input.pop("creationTime")
        if creation_time and isinstance(creation_time, str):
            # Trim microseconds from 9 digits to 6 digits
            trimmed = re.sub(r"\.(\d{6})\d+", r".\1", creation_time)
            input["creationTime"] = datetime.datetime.strptime(trimmed, "%Y-%m-%dT%H:%M:%S.%fZ[%Z]")

        return ModelDetails(**input)
