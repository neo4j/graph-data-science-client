from typing import Any, Dict, List, Union

from pandas import DataFrame, Series

from ..query_runner.query_runner import QueryRunner
from ..server_version.server_version import ServerVersion


class SimpleEdgeEmbeddingModel:
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
        source_node_filter: Union[str, List[int]],
        target_node_filter: Union[str, List[int]],
        relationship_type: str,
        top_k: int,
    ) -> DataFrame:
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
        source_node_filter: Union[str, List[int]],
        target_node_filter: Union[str, List[int]],
        relationship_type: str,
        top_k: int,
        mutate_relationship_type: str,
    ) -> "Series[Any]":
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
                    mutateRelationshipType: $mutate_relationship_type
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
            },
        ).squeeze()

    def predict_write(
        self,
        source_node_filter: Union[str, List[int]],
        target_node_filter: Union[str, List[int]],
        relationship_type: str,
        top_k: int,
        write_relationship_type: str,
    ) -> "Series[Any]":
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
                    writeRelationshipType: $write_relationship_type
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
            },
        ).squeeze()

    def scoring_fuction(self) -> str:
        return self._scoring_function

    def graph_name(self) -> str:
        return self._graph_name

    def node_embedding_property(self) -> str:
        return self._node_embedding_property

    def relationship_type_embeddings(self) -> Dict[str, List[float]]:
        return self._relationship_type_embeddings
