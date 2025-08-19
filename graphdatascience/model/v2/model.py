from __future__ import annotations

from abc import ABC
from typing import Optional

from graphdatascience.model.v2.model_api import ModelApi
from graphdatascience.model.v2.model_info import ModelDetails


# Compared to v1 Model offering typed parameters for predict endpoints
class Model(ABC):
    def __init__(self, name: str, model_api: ModelApi):
        self._name = name
        self._model_api = model_api

    # TODO estimate mode, predict modes on here?
    # implement Cypher and Arrow info_provider and stuff

    def name(self) -> str:
        """
        Get the name of the model.

        Returns:
            The name of the model.

        """
        return self._name

    def details(self) -> ModelDetails:
        return self._model_api.get(self._name)

    def exists(self) -> bool:
        """
        Check whether the model exists.

        Returns:
            True if the model exists, False otherwise.

        """
        return self._model_api.exists(self._name)

    def drop(self, failIfMissing: bool = False) -> Optional[ModelDetails]:
        """
        Drop the model.

        Args:
            failIfMissing: If True, an error is thrown if the model does not exist. If False, no error is thrown.

        Returns:
            The result of the drop operation.

        """
        return self._model_api.drop(self._name, failIfMissing)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name()}, type={self.details().type})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.details().model_dump()})"
