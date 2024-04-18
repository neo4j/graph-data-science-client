from enum import Enum
from typing import List


class SessionMemory(Enum):
    """
    Enumeration representing session memory configurations.
    """

    _1GB = "1GB"
    _2GB = "2GB"
    _4GB = "4GB"
    _8GB = "8GB"
    _16GB = "16GB"
    _32GB = "32GB"
    _64GB = "64GB"
    _128GB = "128GB"
    _256GB = "256GB"
    _320GB = "320GB"
    _512GB = "512GB"
    DEFAULT = "8GB"

    @classmethod
    def all_values(cls) -> List[str]:
        return [e.value for e in cls if not e.name == SessionMemory.DEFAULT.name]
