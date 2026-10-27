from __future__ import annotations

import json

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize
from graphdatascience.model.model_details import ModelDetails
from graphdatascience.procedure_surface.api.model.model_catalog_endpoints import (
    ModelCatalogEndpoints,
    ModelDeleteResult,
    ModelExistsResult,
    ModelLoadResult,
    ModelStoreResult,
)


class ModelCatalogArrowEndpoints(ModelCatalogEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient):
        self._arrow_client = arrow_client

    def list(self) -> list[ModelDetails]:
        raw = self._arrow_client.do_action_with_retry("v2/model.list", payload=json.dumps({}).encode("utf-8"))
        items = deserialize(raw)
        return [ModelDetails(**it) for it in items]

    def exists(self, model_name: str) -> ModelExistsResult | None:
        raw = self._arrow_client.do_action_with_retry(
            "v2/model.exists", payload=json.dumps({"modelName": model_name}).encode("utf-8")
        )
        items = deserialize(raw)
        if not items:
            return None
        item = items[0]
        return ModelExistsResult(**item)

    def get(self, model_name: str) -> ModelDetails:
        raw = self._arrow_client.do_action_with_retry(
            "v2/model.get", payload=json.dumps({"modelName": model_name}).encode("utf-8")
        )
        items = deserialize(raw)
        if not items:
            raise ValueError(f"Model with name `{model_name}` does not exist")
        return ModelDetails(**items[0])

    def drop(self, model_name: str, *, fail_if_missing: bool = True) -> ModelDetails | None:
        raw = self._arrow_client.do_action_with_retry(
            "v2/model.drop",
            payload=json.dumps({"modelName": model_name, "failIfMissing": fail_if_missing}).encode("utf-8"),
        )
        items = deserialize(raw)
        if not items and fail_if_missing:
            raise ValueError(f"Model with name `{model_name}` does not exist")
        if not items:
            return None
        return ModelDetails(**items[0])

    def delete(self, model_name: str, fail_if_missing: bool = False) -> ModelDeleteResult | None:
        raw = self._arrow_client.do_action_with_retry(
            "v2/model.delete",
            payload=json.dumps({"modelName": model_name, "failIfMissing": fail_if_missing}).encode("utf-8"),
        )
        items = deserialize(raw)
        if not items and fail_if_missing:
            raise ValueError(f"Model with name `{model_name}` does not exist")
        if not items:
            return None
        return ModelDeleteResult(**items[0])

    def load(self, model_name: str) -> ModelLoadResult:
        raw = self._arrow_client.do_action_with_retry(
            "v2/model.load", payload=json.dumps({"modelName": model_name}).encode("utf-8")
        )
        items = deserialize(raw)
        if not items:
            raise ValueError(f"Model with name `{model_name}` does not exist")
        return ModelLoadResult(**items[0])

    def store(self, model_name: str, *, fail_if_unsupported: bool = False) -> ModelStoreResult:
        raw = self._arrow_client.do_action_with_retry(
            "v2/model.store",
            payload=json.dumps({"modelName": model_name, "failIfUnsupported": fail_if_unsupported}).encode("utf-8"),
        )
        items = deserialize(raw)
        if not items:
            raise ValueError(f"Model with name `{model_name}` does not exist")
        return ModelStoreResult(**items[0])

    def publish(self, model_name: str) -> ModelDetails:
        raw = self._arrow_client.do_action_with_retry(
            "v2/model.publish", payload=json.dumps({"modelName": model_name}).encode("utf-8")
        )
        items = deserialize(raw)
        if not items:
            raise ValueError(f"Model with name `{model_name}` does not exist")
        return ModelDetails(**items[0])
