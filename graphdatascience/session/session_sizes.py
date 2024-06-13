from enum import Enum
from typing import List


class SessionMemory(Enum):
    """
    Enumeration representing session main memory configurations.
    """

    m_4GB = "4GB"
    m_8GB = "8GB"
    m_16GB = "16GB"
    m_24GB = "24GB"
    m_32GB = "32GB"
    m_48GB = "48GB"
    m_64GB = "64GB"
    m_96GB = "96GB"
    m_128GB = "128GB"
    m_192GB = "192GB"
    m_256GB = "256GB"
    m_384GB = "384GB"

    @classmethod
    def all_values(cls) -> List[str]:
        """
        All supported memory configurations.

        Returns:
            A list of strings representing all supported memory configurations for sessions.

        """
        return [e.value for e in cls]

    @staticmethod
    def fromApiResponse(value: str) -> "SessionMemory":
        """
        Converts the string value from an API response to a SessionMemory enumeration value.

        Args:
            value: The string value from the API response.

        Returns:
            The SessionMemory enumeration value.

        """
        try:
            return SessionMemory(value.replace("Gi", "GB"))
        except ValueError:
            raise ValueError(f"Unsupported memory configuration: {value}")
