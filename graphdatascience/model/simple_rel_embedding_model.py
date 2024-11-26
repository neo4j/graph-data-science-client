from typing import Any, Union

from pandas import DataFrame, Series

from ..call_parameters import CallParameters
from ..query_runner.query_runner import QueryRunner
from ..server_version.server_version import ServerVersion

NodeFilter = Union[int, list[int], str]


class SimpleRelEmbeddingModel:
    """
    A class whose instances represent a model for computing and ranking pairwise distance between nodes
    according to knowledge graph style metrics. It may also produce new relationships based on these
    rankings.
    """

    def __init__(
        self,
        scoring_function: str,
        query_runner: QueryRunner,
        server_version: ServerVersion,
        graph_name: str,
        node_embedding_property: str,
        relationship_type_embeddings: dict[str, list[float]],
    ):
        self._scoring_function = scoring_function
        self._query_runner = query_runner
        self._server_version = server_version
        self._graph_name = graph_name
        self._node_embedding_property = node_embedding_property
        self._relationship_type_embeddings = relationship_type_embeddings

    def predict_stream(
        self,
        source_node_filter: NodeFilter,
        target_node_filter: NodeFilter,
        relationship_type: str,
        top_k: int,
        **general_config: Any,
    ) -> DataFrame:
        """
        Compute and stream relationship embeddings

        Args:
            source_node_filter: The specification of source nodes to consider
            target_node_filter: The specification of target nodes to consider
            relationship_type: The name of the relationship type whose embedding will be used in the computation
            top_k: How many target nodes to return for each source node
            general_config: General algorithm keyword parameters such as 'concurrency'

        Returns:
            The `top_k` highest scoring target nodes for each source node, along with the score for the node pair
        """

        config = {
            "sourceNodeFilter": source_node_filter,
            "targetNodeFilter": target_node_filter,
            "nodeEmbeddingProperty": self._node_embedding_property,
            "relationshipTypeEmbedding": self._relationship_type_embeddings[relationship_type],
            "scoringFunction": self._scoring_function,
            "topK": top_k,
            **general_config,
        }

        return self._query_runner.call_procedure(
            endpoint="gds.ml.kge.predict.stream",
            params=CallParameters(graph_name=self._graph_name, config=config),
        )

    def predict_mutate(
        self,
        source_node_filter: NodeFilter,
        target_node_filter: NodeFilter,
        relationship_type: str,
        top_k: int,
        mutate_relationship_type: str,
        mutate_property: str,
        **general_config: Any,
    ) -> "Series[Any]":
        """
        Compute relationship embeddings and add them to graph projection under a new relationship type

        Args:
            source_node_filter: The specification of source nodes to consider
            target_node_filter: The specification of target nodes to consider
            relationship_type: The name of the relationship type whose embedding will be used in the computation
            top_k: How many relationships to add for each source node
            mutate_relationship_type: The name of the new relationship type for the predicted relationships
            mutate_property: The name of the property on the new relationships which will store the model prediction
                score
            general_config: General algorithm keyword parameters such as 'concurrency'

        Returns:
            A `pandas.Series` object with metadata about the performed computation and mutation
        """

        config = {
            "sourceNodeFilter": source_node_filter,
            "targetNodeFilter": target_node_filter,
            "nodeEmbeddingProperty": self._node_embedding_property,
            "relationshipTypeEmbedding": self._relationship_type_embeddings[relationship_type],
            "scoringFunction": self._scoring_function,
            "topK": top_k,
            "mutateRelationshipType": mutate_relationship_type,
            "mutateProperty": mutate_property,
            **general_config,
        }

        return self._query_runner.call_procedure(  # type: ignore
            endpoint="gds.ml.kge.predict.mutate",
            params=CallParameters(graph_name=self._graph_name, config=config),
        ).squeeze()

    def predict_write(
        self,
        source_node_filter: NodeFilter,
        target_node_filter: NodeFilter,
        relationship_type: str,
        top_k: int,
        write_relationship_type: str,
        write_property: str,
        **general_config: Any,
    ) -> "Series[Any]":
        """
        Compute relationship embeddings and write them back to the database under a new relationship type

        Args:
            source_node_filter: The specification of source nodes to consider
            target_node_filter: The specification of target nodes to consider
            relationship_type: The name of the relationship type whose embedding will be used in the computation
            top_k: How many relationships to add for each source node
            write_relationship_type: The name of the new relationship type for the predicted relationships
            write_property: The name of the property on the new relationships which will store the model prediction
                score
            general_config: General algorithm keyword parameters such as 'concurrency'

        Returns:
            A `pandas.Series` object with metadata about the performed computation and write-back
        """

        config = {
            "sourceNodeFilter": source_node_filter,
            "targetNodeFilter": target_node_filter,
            "nodeEmbeddingProperty": self._node_embedding_property,
            "relationshipTypeEmbedding": self._relationship_type_embeddings[relationship_type],
            "scoringFunction": self._scoring_function,
            "topK": top_k,
            "writeRelationshipType": write_relationship_type,
            "writeProperty": write_property,
            **general_config,
        }

        return self._query_runner.call_procedure(  # type: ignore
            endpoint="gds.ml.kge.predict.write",
            params=CallParameters(graph_name=self._graph_name, config=config),
        ).squeeze()

    def scoring_function(self) -> str:
        """
        Get the name of the scoring function the model is using

        Returns:
            The name of the scoring function the model is using
        """
        return self._scoring_function

    def graph_name(self) -> str:
        """
        Get the name of the graph the model is based on

        Returns:
            The name of the graph the model is based on
        """
        return self._graph_name

    def node_embedding_property(self) -> str:
        """
        Get the name of the node property storing embeddings in the graph

        Returns:
            The name of the node property storing embeddings in the graph
        """
        return self._node_embedding_property

    def relationship_type_embeddings(self) -> dict[str, list[float]]:
        """
        Get the relationship type embeddings of the model

        Returns:
            The relationship type embeddings of the model
        """
        return self._relationship_type_embeddings
