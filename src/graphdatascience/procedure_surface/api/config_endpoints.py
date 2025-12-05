from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ConfigEndpoints(ABC):
    @property
    @abstractmethod
    def defaults(self) -> DefaultsEndpoints:
        pass

    @property
    @abstractmethod
    def limits(self) -> LimitsEndpoints:
        pass


class DefaultsEndpoints(ABC):
    @abstractmethod
    def set(
        self,
        key: str,
        value: Any,
        username: str | None = None,
    ) -> None:
        """
        Configure a new default configuration value.

        Parameters:
            key : str
                The configuration key for which the default value is being set.
            value : Any
                The value to set as the default for the given key.
            username : str | None, default=None
                If set, the configuration will be set for the given user.

        Returns: None
        """
        pass

    @abstractmethod
    def list(
        self,
        username: str | None = None,
        key: str | None = None,
    ) -> dict[str, Any]:
        """
        List configured default configuration values.

        Parameters:
            key : str | None (default=None)
                List only the default value for the given key.
            username : str | None, default=None
                List only default values for the given user.

        Returns: dict[str, Any]
            A dictionary containing the default configuration values.
        """
        pass


class LimitsEndpoints(ABC):
    @abstractmethod
    def set(
        self,
        key: str,
        value: Any,
        username: str | None = None,
    ) -> None:
        """
        Configure a new limit for a configuration value.

        Parameters:
            key : str
                The configuration key for which the limit is being set.
            value : Any
                The value to set as the limit for the given key.
            username : str | None, default=None
                If set, the limit will be set for the given user.

        Returns: None
        """
        pass

    @abstractmethod
    def list(
        self,
        username: str | None = None,
        key: str | None = None,
    ) -> dict[str, Any]:
        """
        List configured configuration limits.

        Parameters:
            key : str | None (default=None)
                List only the limits for the given key.
            username : str | None, default=None
                List only liomits for the given user.

        Returns: dict[str, Any]
            A dictionary containing the configuration limits.
        """
        pass
