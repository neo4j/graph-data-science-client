from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


@dataclass(frozen=True)
class SessionMemoryValue:
    value: str

    def __str__(self) -> str:
        return self.value

    @staticmethod
    def fromApiResponse(value: str) -> SessionMemoryValue:
        """
        Converts the string value from an API response to a SessionMemory enumeration value.

        Args:
            value: The string value from the API response.

        Returns:
            The SessionMemory enumeration value.

        """
        if value == "":
            raise ValueError("memory configuration cannot be empty")

        return SessionMemoryValue(value.replace("Gi", "GB"))

    @staticmethod
    def fromInstanceSize(value: Optional[str]) -> SessionMemoryValue:
        if not value:
            return SESSION_MEMORY_VALUE_UNKNOWN

        return SessionMemoryValue(value.replace("Gi", "GB"))


SESSION_MEMORY_VALUE_UNKNOWN = SessionMemoryValue("")


class SessionMemory(Enum):
    """
    Enumeration representing session main memory configurations.
    """

    m_4GB = SessionMemoryValue("4GB")
    m_8GB = SessionMemoryValue("8GB")
    m_16GB = SessionMemoryValue("16GB")
    m_24GB = SessionMemoryValue("24GB")
    m_32GB = SessionMemoryValue("32GB")
    m_48GB = SessionMemoryValue("48GB")
    m_64GB = SessionMemoryValue("64GB")
    m_96GB = SessionMemoryValue("96GB")
    m_128GB = SessionMemoryValue("128GB")
    m_192GB = SessionMemoryValue("192GB")
    m_256GB = SessionMemoryValue("256GB")
    m_384GB = SessionMemoryValue("384GB")

    @classmethod
    def all_values(cls) -> list[SessionMemoryValue]:
        """
        All supported memory configurations.

        Returns:
            A list of strings representing all supported memory configurations for sessions.

        """
        return [e.value for e in cls]
