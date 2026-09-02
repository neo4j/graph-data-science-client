from typing import Annotated, Literal

from annotated_types import Len
from pydantic import BaseModel, Field, PositiveInt, model_validator

NodeType = str

Dropout = Annotated[float, Field(ge=0.0, lt=1.0)]


class GraphSAGEConfig(BaseModel):
    graph_encoder_type: Literal["graphsage"] = "graphsage"
    target_type: NodeType
    hidden_dims: Annotated[list[PositiveInt], Len(max_length=3)] | None = None
    num_neighbors: Annotated[list[PositiveInt], Len(min_length=1, max_length=4)] | None = None
    out_dim: PositiveInt | None = None
    dropout: Dropout | None = None

    @model_validator(mode="after")
    def depth_validator(self) -> "GraphSAGEConfig":
        if self.hidden_dims is not None:
            if self.num_neighbors is not None:
                if len(self.hidden_dims) + 1 != len(self.num_neighbors):
                    raise ValueError(
                        "The depth of the GNN is inferred from the hidden dimensions per layer (+1) and the number of neighbors per layer. These do not align."
                    )
        return self


class FastRPConfig(BaseModel):
    graph_encoder_type: Literal["fast_rp"] = "fast_rp"
    out_dim: PositiveInt | None = None
    coefficients: list[float] | None = None
    normalization_strength: float | None = None


class IdentityConfig(BaseModel):
    graph_encoder_type: Literal["identity"] = "identity"
    target_type: NodeType
    out_dim: PositiveInt


NoTrainGraphEncoderConfig = FastRPConfig | IdentityConfig  # can be run without training
GraphEncoderConfig = FastRPConfig | GraphSAGEConfig | IdentityConfig


class GBClassifierConfig(BaseModel):
    decoder_type: Literal["gb_classifier"] = "gb_classifier"


class MLPClassifierConfig(BaseModel):
    decoder_type: Literal["mlp_classifier"] = "mlp_classifier"
    hidden_dims: list[PositiveInt] | None = None
    dropout: Dropout | None = None


DecoderConfig = GBClassifierConfig | MLPClassifierConfig
