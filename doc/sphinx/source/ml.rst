Machine learning procedures
----------------------------
Listing of all machine learning procedures in the Neo4j Graph Data Science Python Client API.
This includes running embedding algorithms and creating various pipelines.
These all assume that an object of :class:`.GraphDataScience` is available as `gds`.


.. py:function:: gds.pipeline.get(pipeline_name: str) -> TrainingPipeline[PipelineModel]

    Get a pipeline object representing a pipeline in the Pipeline Catalog.

.. py:function:: gds.alpha.ml.splitRelationships.mutate(G: Graph, **config: Any) -> "Series[Any]"

    Splits a graph into holdout and remaining relationship types and adds them to the graph.

.. py:function:: gds.alpha.pipeline.nodeRegression.create(name: str) -> Tuple[NRTrainingPipeline, "Series[Any]"]

    Creates a node regression training pipeline in the pipeline catalog.

.. py:function:: gds.alpha.scaleProperties.mutate(G: Graph, **config: Any) -> "Series[Any]"

    Scale node properties

.. py:function:: gds.alpha.scaleProperties.stream(G: Graph, **config: Any) -> DataFrame

    Scale node properties

.. py:function:: gds.beta.graphSage.mutate(G: Graph, **config: Any) -> "Series[Any]"

    The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.

.. py:function:: gds.beta.graphSage.mutate.estimate(G: Graph, **config: Any) -> "Series[Any]"

    The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.

.. py:function:: gds.beta.graphSage.stream(G: Graph, **config: Any) -> DataFrame

    The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.

.. py:function:: gds.beta.graphSage.stream.estimate(G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.graphSage.train(G: Graph, **config: Any) -> Tuple[MODEL_TYPE, "Series[Any]"]

    The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.

.. py:function:: gds.beta.graphSage.train.estimate(G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.graphSage.write(G: Graph, **config: Any) -> "Series[Any]"

    The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.

.. py:function:: gds.beta.graphSage.write.estimate(G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.hashgnn.mutate(G: Graph, **config: Any) -> "Series[Any]"

    HashGNN creates node embeddings by hashing and message passing.

.. py:function:: gds.beta.hashgnn.mutate.estimate(G: Graph, **config: Any) -> "Series[Any]"

    HashGNN creates node embeddings by hashing and message passing.

.. py:function:: gds.beta.hashgnn.stream(G: Graph, **config: Any) -> DataFrame

    HashGNN creates node embeddings by hashing and message passing.

.. py:function:: gds.beta.hashgnn.stream.estimate(G: Graph, **config: Any) -> "Series[Any]"

    HashGNN creates node embeddings by hashing and message passing.

.. py:function:: gds.beta.node2vec.mutate(G: Graph, **config: Any) -> "Series[Any]"

    The Node2Vec algorithm computes embeddings for nodes based on random walks.

.. py:function:: gds.beta.node2vec.mutate.estimate(G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.node2vec.stream(G: Graph, **config: Any) -> DataFrame

    The Node2Vec algorithm computes embeddings for nodes based on random walks.

.. py:function:: gds.beta.node2vec.stream.estimate(G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.node2vec.write(G: Graph, **config: Any) -> "Series[Any]"

    The Node2Vec algorithm computes embeddings for nodes based on random walks.

.. py:function:: gds.beta.node2vec.write.estimate(G: Graph, **config: Any) -> "Series[Any]"

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.pipeline.drop(pipeline: TrainingPipeline[PipelineModel]) -> "Series[Any]"

    Drops a pipeline and frees up the resources it occupies.

.. py:function:: gds.beta.pipeline.exists(pipeline_name: str) -> "Series[Any]"

    Checks if a given pipeline exists in the pipeline catalog.

.. py:function:: gds.beta.pipeline.linkPrediction.create(name: str) -> Tuple[LPTrainingPipeline, "Series[Any]"]

    Creates a link prediction pipeline in the pipeline catalog.

.. py:function:: gds.beta.pipeline.list(pipeline: Optional[TrainingPipeline[PipelineModel]] = None) -> DataFrame

    Lists all pipelines contained in the pipeline catalog.

.. py:function:: gds.beta.pipeline.nodeClassification.create(name: str) -> Tuple[NCTrainingPipeline, "Series[Any]"]

    Creates a node classification training pipeline in the pipeline catalog.

.. py:function:: gds.fastRP.mutate(G: Graph, **config: Any) -> "Series[Any]"

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.mutate.estimate(G: Graph, **config: Any) -> "Series[Any]"

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.stats(G: Graph, **config: Any) -> "Series[Any]"

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.stats.estimate(G: Graph, **config: Any) -> "Series[Any]"

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.stream(G: Graph, **config: Any) -> DataFrame

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.stream.estimate(G: Graph, **config: Any) -> "Series[Any]"

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.write(G: Graph, **config: Any) -> "Series[Any]"

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.write.estimate(G: Graph, **config: Any) -> "Series[Any]"

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.alpha.ml.oneHotEncoding(available_values: List[Any], selected_values: List[Any]) -> List[int]

    Return a list of selected values in a one hot encoding format.
