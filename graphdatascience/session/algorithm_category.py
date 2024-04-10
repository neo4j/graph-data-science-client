from enum import Enum


class AlgorithmCategory(str, Enum):
    """
    Enumeration of supported algorithm categories used for size estimation.
    """

    CENTRALITY = "centrality"
    COMMUNITY_DETECTION = "community-detection"
    MACHINE_LEARNING = "machine-learning"
    NODE_EMBEDDING = "node-embedding"
    PATH_FINDING = "path-finding"
    SIMILARITY = "similarity"
