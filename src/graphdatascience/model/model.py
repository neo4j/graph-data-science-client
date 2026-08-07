from __future__ import annotations

from abc import ABC

from graphdatascience.model.model_catalog_protocol import ModelCatalogProtocol
from graphdatascience.model.model_details import ModelDetails
from graphdatascience.procedure_surface.api.model.model_catalog_endpoints import (
    ModelDeleteResult,
    ModelLoadResult,
    ModelStoreResult,
)


class Model(ABC):
    def __init__(self, name: str, catalog: ModelCatalogProtocol):
        self._name = name
        self._catalog = catalog

    def name(self) -> str:
        """
        Get the name of the model.

        Returns
        -------
        str
            The name of the model.

        """
        return self._name

    def details(self) -> ModelDetails:
        """
        Get metadata about the model from the model catalog.

        Returns
        -------
        ModelDetails
            The details of the model.

        """
        return self._catalog.get(self._name)

    def exists(self) -> bool:
        """
        Check whether the model exists.

        Returns
        -------
        bool
            True if the model exists, False otherwise.

        """
        return self._catalog.exists(self._name) is not None

    def drop(self, fail_if_missing: bool = False) -> ModelDetails | None:
        """
        Drop the model.

        Args:
            fail_if_missing: If True, an error is thrown if the model does not exist. If False, no error is thrown.

        Returns
        -------
        ModelDetails | None
            The result of the drop operation.

        """
        return self._catalog.drop(self._name, fail_if_missing=fail_if_missing)

    def delete(self, fail_if_missing: bool = False) -> ModelDeleteResult | None:
        """
        Delete the persisted model from storage.

        Args:
            fail_if_missing: If True, an error is thrown if the model does not exist. If False, no error is thrown.

        Returns
        -------
        ModelDeleteResult | None
            The result of the delete operation.

        """
        return self._catalog.delete(self._name, fail_if_missing)

    def load(self) -> ModelLoadResult:
        """
        Load the persisted model into the in-memory catalog.

        Returns
        -------
        ModelLoadResult
            The result of the load operation.

        """
        return self._catalog.load(self._name)

    def store(self, fail_if_unsupported: bool = False) -> ModelStoreResult:
        """
        Persist the model to storage.

        Args:
            fail_if_unsupported: If True, an error is thrown if the model is not supported for storing.

        Returns
        -------
        ModelStoreResult
            The result of the store operation.

        """
        return self._catalog.store(self._name, fail_if_unsupported=fail_if_unsupported)

    def publish(self) -> ModelDetails:
        """
        Publish the model so it becomes accessible to other users.

        Returns
        -------
        ModelDetails
            The details of the published model.

        """
        return self._catalog.publish(self._name)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name()}, type={self.details().model_type})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.details().model_dump()})"
