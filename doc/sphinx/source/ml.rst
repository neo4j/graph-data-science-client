Machine learning procedures
----------------------------
All the machine learning procedures under the `gds` namespace.
This includes running embedding algorithms and creating various pipelines.

.. py:function:: gds.pipeline.get(self, pipeline_name: str) -> TrainingPipeline[PipelineModel]

    Get a pipeline object representing a pipeline in the Pipeline Catalog.

.. py:function:: gds.alpha.ml.splitRelationships.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    Splits a graph into holdout and remaining relationship types and adds them to the graph.

.. py:function:: gds.alpha.scaleProperties.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    Scale node properties

.. py:function:: gds.alpha.scaleProperties.stream(self, G: Graph, **config: Any) -> DataFrame

    Scale node properties

.. py:function:: gds.beta.graphSage.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.

.. py:function:: gds.beta.graphSage.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.

.. py:function:: gds.beta.graphSage.stream(self, G: Graph, **config: Any) -> DataFrame

    The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.

.. py:function:: gds.beta.graphSage.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.graphSage.train(self, G: Graph, **config: Any) -> Tuple[MODEL_TYPE, "Series[Any]"]

    The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.

.. py:function:: gds.beta.graphSage.train.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.graphSage.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.

.. py:function:: gds.beta.graphSage.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.hashgnn.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    HashGNN creates node embeddings by hashing and message passing.

.. py:function:: gds.beta.hashgnn.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    HashGNN creates node embeddings by hashing and message passing.

.. py:function:: gds.beta.hashgnn.stream(self, G: Graph, **config: Any) -> DataFrame

    HashGNN creates node embeddings by hashing and message passing.

.. py:function:: gds.beta.hashgnn.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    HashGNN creates node embeddings by hashing and message passing.

.. py:function:: gds.beta.node2vec.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    The Node2Vec algorithm computes embeddings for nodes based on random walks.

.. py:function:: gds.beta.node2vec.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.node2vec.stream(self, G: Graph, **config: Any) -> DataFrame

    The Node2Vec algorithm computes embeddings for nodes based on random walks.

.. py:function:: gds.beta.node2vec.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.node2vec.write(self, G: Graph, **config: Any) -> "Series[Any]"

    The Node2Vec algorithm computes embeddings for nodes based on random walks.

.. py:function:: gds.beta.node2vec.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.pipeline.drop(self, pipeline: TrainingPipeline[PipelineModel]) -> "Series[Any]"

    Drops a pipeline and frees up the resources it occupies.

.. py:function:: gds.beta.pipeline.exists(self, pipeline_name: str) -> "Series[Any]"

    Checks if a given pipeline exists in the pipeline catalog.

.. py:function:: gds.beta.pipeline.linkPrediction.create(self, name: str) -> Tuple[LPTrainingPipeline, "Series[Any]"]

    Creates a link prediction pipeline in the pipeline catalog.

.. py:function:: gds.beta.pipeline.list(self, pipeline: Optional[TrainingPipeline[PipelineModel]] = None) -> DataFrame

    Lists all pipelines contained in the pipeline catalog.

.. py:function:: gds.beta.pipeline.nodeClassification.create(self, name: str) -> Tuple[NCTrainingPipeline, "Series[Any]"]

    Creates a node classification training pipeline in the pipeline catalog.

.. py:function:: gds.fastRP.mutate(self, G: Graph, **config: Any) -> "Series[Any]"

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.mutate.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.stats(self, G: Graph, **config: Any) -> "Series[Any]"

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.stats.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.stream(self, G: Graph, **config: Any) -> DataFrame

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.stream.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.write(self, G: Graph, **config: Any) -> "Series[Any]"

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.write.estimate(self, G: Graph, **config: Any) -> "Series[Any]"

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.alpha.ml.oneHotEncoding(self, available_values: List[Any], selected_values: List[Any]) -> List[int]

    Return a list of selected values in a one hot encoding format.
