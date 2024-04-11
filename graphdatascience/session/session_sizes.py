from enum import Enum


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
