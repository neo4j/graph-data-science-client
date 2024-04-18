from enum import Enum
from typing import List


class SessionMemory(Enum):
    """
    Enumeration representing session main memory configurations.
    """

    m_1GB = "1GB"
    m_2GB = "2GB"
    m_4GB = "4GB"
    m_8GB = "8GB"
    m_16GB = "16GB"
    m_32GB = "32GB"
    m_64GB = "64GB"
    m_128GB = "128GB"
    m_256GB = "256GB"
    m_320GB = "320GB"
    m_512GB = "512GB"

    @classmethod
    def all_values(cls) -> List[str]:
        """
        All supported memory configurations.

        Returns:
            A list of strings representing all supported memory configurations for sessions.

        """
        return [e.value for e in cls]
