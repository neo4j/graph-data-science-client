from abc import ABC, abstractmethod

from graphdatascience.model.v2.model_details import ModelDetails


class ModelApi(ABC):
    @abstractmethod
    def exists(self, model: str) -> bool:
        """
        Check if a specific model exists.

        Args:
            model: The name of the model.

        Returns:
            True if the model exists, False otherwise.
        """
        pass

    @abstractmethod
    def get(self, model: str) -> ModelDetails:
        """
        Get the details of a specific model.

        Args:
            model: The name of the model.

        Returns:
            The details of the model.
        """
        pass

    @abstractmethod
    def drop(self, model: str, fail_if_missing: bool) -> ModelDetails | None:
        """
        Drop a specific model.

        Args:
            model: The name of the model.
            fail_if_missing: If True, an error is thrown if the model does not exist. If False, no error is thrown.

        Returns:
            The result of the drop operation.
        """
        pass
