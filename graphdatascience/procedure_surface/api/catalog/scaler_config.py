from pydantic import BaseModel


class ScalerConfig(BaseModel):
    """
    Configuration for a scaler used in the Scale Properties algorithm.

    Attributes
    ----------
    type : str
        The type of scaler to use. Can be 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center'.
    offset : int | float
        The offset to add to the property values before applying the log transformation. Only used when type is 'Log'.
    """

    type: str
    offset: int | float | None = None
