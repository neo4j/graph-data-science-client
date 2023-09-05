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
        source_node_spec: Union[str, List[int]],
        target_node_spec: Union[str, List[int]],
        relationship_type: str,
        top_k: int,
        threshold: float,
    ) -> DataFrame:
        return self._query_runner.run_query(
            """
            CALL gds.ml.kge.predict.stream(
                $graph_name,
                {
                    sourceNodeSpec: $source_node_spec
                    targetNodeSpec: $target_node_spec
                    nodeEmbeddingProperty: $node_embedding_property
                    relationshipTypeEmbedding: $relationship_type_embedding
                    scoringFunction: $scoring_function,
                    topK: $top_k,
                    threshold: $threshold
                }
            )
            """,
            {
                "graph_name": self._graph_name,
                "source_node_spec": source_node_spec,
                "target_node_spec": target_node_spec,
                "node_embedding_property": self._node_embedding_property,
                "relationship_type_embedding": self._relationship_type_embeddings[relationship_type],
                "scoring_function": self._scoring_function,
                "top_k": top_k,
                "threshold": threshold,
            },
        )

    def predict_mutate(
        self,
        source_node_spec: Union[str, List[int]],
        target_node_spec: Union[str, List[int]],
        relationship_type: str,
        top_k: int,
        threshold: float,
        mutate_relationship_type: str,
    ) -> "Series[Any]":
        return self._query_runner.run_query(  # type: ignore
            """
            CALL gds.ml.kge.predict.mutate(
                $graph_name,
                {
                    sourceNodeSpec: $source_node_spec
                    targetNodeSpec: $target_node_spec
                    nodeEmbeddingProperty: $node_embedding_property
                    relationshipTypeEmbedding: $relationship_type_embedding
                    scoringFunction: $scoring_function,
                    topK: $top_k,
                    threshold: $threshold,
                    mutateRelationshipType: $mutate_relationship_type
                }
            )
            """,
            {
                "graph_name": self._graph_name,
                "source_node_spec": source_node_spec,
                "target_node_spec": target_node_spec,
                "node_embedding_property": self._node_embedding_property,
                "relationship_type_embedding": self._relationship_type_embeddings[relationship_type],
                "scoring_function": self._scoring_function,
                "top_k": top_k,
                "threshold": threshold,
                "mutate_relationship_type": mutate_relationship_type,
            },
        ).squeeze()

    def predict_write(
        self,
        source_node_spec: Union[str, List[int]],
        target_node_spec: Union[str, List[int]],
        relationship_type: str,
        top_k: int,
        threshold: float,
        write_relationship_type: str,
    ) -> "Series[Any]":
        return self._query_runner.run_query(  # type: ignore
            """
            CALL gds.ml.kge.predict.mutate(
                $graph_name,
                {
                    sourceNodeSpec: $source_node_spec
                    targetNodeSpec: $target_node_spec
                    nodeEmbeddingProperty: $node_embedding_property
                    relationshipTypeEmbedding: $relationship_type_embedding
                    scoringFunction: $scoring_function,
                    topK: $top_k,
                    threshold: $threshold,
                    writeRelationshipType: $write_relationship_type
                }
            )
            """,
            {
                "graph_name": self._graph_name,
                "source_node_spec": source_node_spec,
                "target_node_spec": target_node_spec,
                "node_embedding_property": self._node_embedding_property,
                "relationship_type_embedding": self._relationship_type_embeddings[relationship_type],
                "scoring_function": self._scoring_function,
                "top_k": top_k,
                "threshold": threshold,
                "write_relationship_type": write_relationship_type,
            },
        ).squeeze()
