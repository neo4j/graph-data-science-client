from typing import Annotated, Literal

from annotated_types import Len
from pydantic import BaseModel, Field, PositiveInt, model_validator

NodeType = str

Dropout = Annotated[float, Field(ge=0.0, lt=1.0)]


class GraphSAGEConfig(BaseModel):
    """Configuration for the GraphSAGE graph encoder."""

    graph_encoder_type: Literal["graphsage"] = "graphsage"
    target_type: NodeType = Field(description="Node type (label) of the nodes the encoder is trained on.")
    hidden_dims: Annotated[list[PositiveInt], Len(max_length=3)] | None = Field(
        default=None,
        description="Sizes of the hidden layers. The depth of the GNN is inferred as `len(hidden_dims) + 1`.",
    )
    num_neighbors: Annotated[list[PositiveInt], Len(min_length=1, max_length=4)] | None = Field(
        default=None,
        description="Number of neighbors sampled per layer. Must align with the depth implied by `hidden_dims`.",
    )
    out_dim: PositiveInt | None = Field(default=None, description="Output dimensionality of the embeddings.")
    dropout: Dropout | None = Field(default=None, description="Dropout probability applied during training.")

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
    """Configuration for the FastRP graph encoder."""

    graph_encoder_type: Literal["fast_rp"] = "fast_rp"
    out_dim: PositiveInt | None = Field(default=None, description="Output dimensionality of the embeddings.")
    coefficients: list[float] | None = Field(
        default=None,
        description="Weights for each iteration. Controls the influence of each iteration on the final embedding.",
    )
    normalization_strength: float | None = Field(
        default=None,
        description="The normalization strength parameter controls how much the embedding is normalized.",
    )


class IdentityConfig(BaseModel):
    """Configuration for the Identity encoder, which passes node properties through unchanged."""

    graph_encoder_type: Literal["identity"] = "identity"
    target_type: NodeType = Field(description="Node type (label) of the nodes to encode.")
    out_dim: PositiveInt = Field(description="Output dimensionality of the embeddings.")


NoTrainGraphEncoderConfig = FastRPConfig | IdentityConfig  # can be run without training
GraphEncoderConfig = FastRPConfig | GraphSAGEConfig | IdentityConfig


class GBClassifierConfig(BaseModel):
    """Configuration for the gradient boosted tree classifier decoder."""

    decoder_type: Literal["gb_classifier"] = "gb_classifier"


class MLPClassifierConfig(BaseModel):
    """Configuration for the multi-layer perceptron classifier decoder."""

    decoder_type: Literal["mlp_classifier"] = "mlp_classifier"
    hidden_dims: list[PositiveInt] | None = Field(
        default=None, description="Sizes of the hidden layers of the network."
    )
    dropout: Dropout | None = Field(default=None, description="Dropout probability applied during training.")


DecoderConfig = GBClassifierConfig | MLPClassifierConfig
