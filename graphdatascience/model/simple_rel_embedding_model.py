from typing import Any, Dict, List, Union

from pandas import DataFrame, Series

from ..query_runner.query_runner import QueryRunner
from ..server_version.server_version import ServerVersion

NodeFilter = Union[int, List[int], str]


class SimpleRelEmbeddingModel:
    """
    A class whose instances represent a model for computing and producing knowledge graph style relationship embeddings.
    """

    def __init__(
        self,
        scoring_function: str,
        query_runner: QueryRunner,
        server_version: ServerVersion,
        graph_name: str,
        node_embedding_property: str,
        relationship_type_embeddings: Dict[str, List[float]],
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
    ) -> DataFrame:
        """
        Compute and stream relationship embeddings

        Args:
            source_node_filter: The specification of source nodes to consider
            target_node_filter: The specification of target nodes to consider
            relationship_type: The name of the relationship type whose embedding will be used in the computation
            top_k: How many relationship embeddings to return for each source node

        Returns:
            The `top_k` highest scoring relationship embeddings to any target node, for each source node
        """
        return self._query_runner.run_query(
            """
            CALL gds.ml.kge.predict.stream(
                $graph_name,
                {
                    sourceNodeFilter: $source_node_filter,
                    targetNodeFilter: $target_node_filter,
                    nodeEmbeddingProperty: $node_embedding_property,
                    relationshipTypeEmbedding: $relationship_type_embedding,
                    scoringFunction: $scoring_function,
                    topK: $top_k
                }
            )
            """,
            {
                "graph_name": self._graph_name,
                "source_node_filter": source_node_filter,
                "target_node_filter": target_node_filter,
                "node_embedding_property": self._node_embedding_property,
                "relationship_type_embedding": self._relationship_type_embeddings[relationship_type],
                "scoring_function": self._scoring_function,
                "top_k": top_k,
            },
        )

    def predict_mutate(
        self,
        source_node_filter: NodeFilter,
        target_node_filter: NodeFilter,
        relationship_type: str,
        top_k: int,
        mutate_relationship_type: str,
        mutate_property: str,
    ) -> "Series[Any]":
        """
        Compute relationship embeddings and add them to graph projection under a new relationship type

        Args:
            source_node_filter: The specification of source nodes to consider
            target_node_filter: The specification of target nodes to consider
            relationship_type: The name of the relationship type whose embedding will be used in the computation
            top_k: How many relationship embeddings to add for each source node
            mutate_relationship_type: The name of the new relationship type hosting the predicted relationship embeddings # noqa: E501
            mutate_property: The name of the property on the new relationships which will store the model prediction score # noqa: E501

        Returns:
            A `pandas.Series` object with metadata about the performed computation and mutation
        """
        return self._query_runner.run_query(  # type: ignore
            """
            CALL gds.ml.kge.predict.mutate(
                $graph_name,
                {
                    sourceNodeFilter: $source_node_filter,
                    targetNodeFilter: $target_node_filter,
                    nodeEmbeddingProperty: $node_embedding_property,
                    relationshipTypeEmbedding: $relationship_type_embedding,
                    scoringFunction: $scoring_function,
                    topK: $top_k,
                    mutateRelationshipType: $mutate_relationship_type,
                    mutateProperty: $mutate_property
                }
            )
            """,
            {
                "graph_name": self._graph_name,
                "source_node_filter": source_node_filter,
                "target_node_filter": target_node_filter,
                "node_embedding_property": self._node_embedding_property,
                "relationship_type_embedding": self._relationship_type_embeddings[relationship_type],
                "scoring_function": self._scoring_function,
                "top_k": top_k,
                "mutate_relationship_type": mutate_relationship_type,
                "mutate_property": mutate_property,
            },
        ).squeeze()

    def predict_write(
        self,
        source_node_filter: NodeFilter,
        target_node_filter: NodeFilter,
        relationship_type: str,
        top_k: int,
        write_relationship_type: str,
        write_property: str,
    ) -> "Series[Any]":
        """
        Compute relationship embeddings and write them back to the database under a new relationship type

        Args:
            source_node_filter: The specification of source nodes to consider
            target_node_filter: The specification of target nodes to consider
            relationship_type: The name of the relationship type whose embedding will be used in the computation
            top_k: How many relationship embeddings to add for each source node
            write_relationship_type: The name of the new relationship type hosting the predicted relationship embeddings
            write_property: The name of the property on the new relationships which will store the model prediction score # noqa: E501

        Returns:
            A `pandas.Series` object with metadata about the performed computation and write-back
        """
        return self._query_runner.run_query(  # type: ignore
            """
            CALL gds.ml.kge.predict.write(
                $graph_name,
                {
                    sourceNodeFilter: $source_node_filter,
                    targetNodeFilter: $target_node_filter,
                    nodeEmbeddingProperty: $node_embedding_property,
                    relationshipTypeEmbedding: $relationship_type_embedding,
                    scoringFunction: $scoring_function,
                    topK: $top_k,
                    writeRelationshipType: $write_relationship_type,
                    writeProperty: $write_property
                }
            )
            """,
            {
                "graph_name": self._graph_name,
                "source_node_filter": source_node_filter,
                "target_node_filter": target_node_filter,
                "node_embedding_property": self._node_embedding_property,
                "relationship_type_embedding": self._relationship_type_embeddings[relationship_type],
                "scoring_function": self._scoring_function,
                "top_k": top_k,
                "write_relationship_type": write_relationship_type,
                "write_property": write_property,
            },
        ).squeeze()

    def scoring_fuction(self) -> str:
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

    def relationship_type_embeddings(self) -> Dict[str, List[float]]:
        """
        Get the relationship type embeddings of the model

        Returns:
            The relationship type embeddings of the model
        """
        return self._relationship_type_embeddings
