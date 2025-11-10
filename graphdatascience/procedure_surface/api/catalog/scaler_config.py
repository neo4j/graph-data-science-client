from pydantic import BaseModel, Field


class ScalerConfig(BaseModel):
    type: str = Field(
        description="The type of scaler to use. Can be 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center'."
    )
    offset: int | float | None = Field(
        description="The offset to add to the property values before applying the log transformation. Only used when type is 'Log'."
    )
