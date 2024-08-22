from dataclasses import dataclass


@dataclass
class CloudLocation:
    """
    Represent the location in Aura.

    Attributes:
        provider (str): Cloud provider: "gcp", "aws" or "azure".
        region (str): Cloud region, e.g. "europe-west1".
    """

    provider: str
    region: str
