from dataclasses import dataclass


@dataclass
class CloudLocation:
    """
    Represent the location in Aura
    """

    provider: str
    region: str
