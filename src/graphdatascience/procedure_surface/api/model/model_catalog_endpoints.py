from __future__ import annotations

from abc import ABC, abstractmethod

from graphdatascience.model.v2.model_details import ModelDetails
from graphdatascience.procedure_surface.api.base_result import BaseResult


class ModelExistsResult(BaseResult):
    model_name: str
    model_type: str
    exists: bool


class ModelDeleteResult(BaseResult):
    model_name: str
    delete_millis: int


class ModelLoadResult(BaseResult):
    model_name: str
    load_millis: int


class ModelStoreResult(BaseResult):
    model_name: str
    store_millis: int


class ModelCatalogEndpoints(ABC):
    @abstractmethod
    def list(self) -> list[ModelDetails]:
        """List all models in the model catalog.

        Returns
        -------
        list[ModelDetails]
            List of model catalog entries.
        """

    @abstractmethod
    def exists(self, model_name: str) -> ModelExistsResult | None:
        """Check whether a model exists.

        Parameters
        ----------
        model_name: str
            The name of the model to check.

        Returns
        -------
        ModelExistsResult | None
            A result object when the model exists; otherwise None.
        """

    @abstractmethod
    def get(self, model_name: str) -> ModelDetails | None:
        """Get a model catalog entry by name.

        Parameters
        ----------
        model_name: str
            The model name to resolve.

        Returns
        -------
        ModelDetails | None
            The model details when found; otherwise None.
        """

    @abstractmethod
    def drop(self, model_name: str, *, fail_if_missing: bool = False) -> ModelDetails | None:
        """Drop a model from the in-memory catalog.

        Parameters
        ----------
        model_name: str
            The model to drop.
        fail_if_missing: bool
            If True, a missing model will cause an error. If False, returns None when missing.

        Returns
        -------
        ModelDetails | None
            The model details after the drop operation when applicable; otherwise None.
        """

    @abstractmethod
    def delete(self, model_name: str) -> ModelDeleteResult | None:
        """Delete a persisted model from storage.

        Parameters
        ----------
        model_name: str
            The model to delete.

        Returns
        -------
        ModelDeleteResult | None
            The delete result when applicable.
        """

    @abstractmethod
    def load(self, model_name: str) -> ModelLoadResult | None:
        """Load a persisted model into the session/catalog.

        Parameters
        ----------
        model_name: str
            The model to load.

        Returns
        -------
        ModelLoadResult | None
            The load result when applicable.
        """

    @abstractmethod
    def store(self, model_name: str, *, fail_if_unsupported: bool = False) -> ModelStoreResult | None:
        """Persist/store a model.

        Parameters
        ----------
        model_name: str
            The model to store.
        fail_if_unsupported: bool
            If True, unsupported models cause an error.

        Returns
        -------
        ModelStoreResult | None
            The store result when applicable.
        """
