Machine learning procedures
----------------------------
Listing of all machine learning procedures in the Neo4j Graph Data Science Python Client API.
This includes running embedding algorithms and creating various pipelines.
These all assume that an object of :class:`.GraphDataScience` is available as `gds`.

.. toctree::
    :maxdepth: 1
    :hidden:
    :titlesonly:

    ../pipeline/index



.. py:function:: gds.pipeline.get(pipeline_name: str) -> TrainingPipeline[PipelineModel]

    Get a pipeline object representing a pipeline in the Pipeline Catalog.

.. py:function:: gds.alpha.ml.splitRelationships.mutate(G: Graph, **config: Any) -> pandas.Series[Any]

    Splits a graph into holdout and remaining relationship types and adds them to the graph.

.. deprecated:: 2.24.0
    Since GDS server version 2.24.0 you should use the endpoint :func:`gds.splitRelationships.mutate` instead.

.. py:function:: gds.alpha.ml.splitRelationships.mutate.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    Splits a graph into holdout and remaining relationship types and adds them to the graph.

.. deprecated:: 2.24.0
    Since GDS server version 2.24.0 you should use the endpoint :func:`gds.splitRelationships.mutate.estimate` instead.

.. py:function:: gds.alpha.pipeline.nodeRegression.create(name: str) -> Tuple[NRTrainingPipeline, pandas.Series[Any]]

    Creates a node regression training pipeline in the pipeline catalog.

.. py:function:: gds.beta.graphSage.mutate(G: Graph, **config: Any) -> pandas.Series[Any]

    The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.

.. py:function:: gds.beta.graphSage.mutate.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.

.. py:function:: gds.beta.graphSage.stream(G: Graph, **config: Any) -> pandas.DataFrame

    The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.

.. py:function:: gds.beta.graphSage.stream.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.graphSage.train(G: Graph, **config: Any) -> Tuple[MODEL_TYPE, pandas.Series[Any]]

    The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.

.. py:function:: gds.beta.graphSage.train.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.graphSage.write(G: Graph, **config: Any) -> pandas.Series[Any]

    The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.

.. py:function:: gds.beta.graphSage.write.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.hashgnn.mutate` instead.

.. py:function:: gds.beta.hashgnn.mutate(G: Graph, **config: Any) -> pandas.Series[Any]

    HashGNN creates node embeddings by hashing and message passing.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.hashgnn.mutate.estimate` instead.

.. py:function:: gds.beta.hashgnn.mutate.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    HashGNN creates node embeddings by hashing and message passing.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.hashgnn.stream` instead.

.. py:function:: gds.beta.hashgnn.stream(G: Graph, **config: Any) -> pandas.DataFrame

    HashGNN creates node embeddings by hashing and message passing.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.hashgnn.stream.estimate` instead.

.. py:function:: gds.beta.hashgnn.stream.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    HashGNN creates node embeddings by hashing and message passing.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.node2vec.mutate` instead.

.. py:function:: gds.beta.node2vec.mutate(G: Graph, **config: Any) -> pandas.Series[Any]

    The Node2Vec algorithm computes embeddings for nodes based on random walks.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.node2vec.mutate.estimate` instead.

.. py:function:: gds.beta.node2vec.mutate.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.node2vec.stream` instead.

.. py:function:: gds.beta.node2vec.stream(G: Graph, **config: Any) -> pandas.DataFrame

    The Node2Vec algorithm computes embeddings for nodes based on random walks.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.node2vec.stream.estimate` instead.

.. py:function:: gds.beta.node2vec.stream.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.node2vec.write` instead.

.. py:function:: gds.beta.node2vec.write(G: Graph, **config: Any) -> pandas.Series[Any]

    The Node2Vec algorithm computes embeddings for nodes based on random walks.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.node2vec.write.estimate` instead.

.. py:function:: gds.beta.node2vec.write.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.beta.pipeline.drop(pipeline: TrainingPipeline[PipelineModel]) -> pandas.Series[Any]

    Drops a pipeline and frees up the resources it occupies.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.pipeline.drop` instead.

.. py:function:: gds.beta.pipeline.exists(pipeline_name: str) -> pandas.Series[Any]

    Checks if a given pipeline exists in the pipeline catalog.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.pipeline.exists` instead.

.. py:function:: gds.beta.pipeline.list(pipeline: Optional[TrainingPipeline[PipelineModel]] = None) -> pandas.DataFrame

    Lists all pipelines contained in the pipeline catalog.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.pipeline.list` instead.

.. py:function:: gds.pipeline.drop(pipeline: TrainingPipeline[PipelineModel]) -> pandas.Series[Any]

    Drops a pipeline and frees up the resources it occupies.

.. py:function:: gds.pipeline.exists(pipeline_name: str) -> pandas.Series[Any]

    Checks if a given pipeline exists in the pipeline catalog.

.. py:function:: gds.pipeline.list(pipeline: Optional[TrainingPipeline[PipelineModel]] = None) -> pandas.DataFrame

    Lists all pipelines contained in the pipeline catalog.

.. py:function:: gds.beta.pipeline.linkPrediction.create(name: str) -> Tuple[LPTrainingPipeline, pandas.Series[Any]]

    Creates a link prediction pipeline in the pipeline catalog.

.. py:function:: gds.beta.pipeline.nodeClassification.create(name: str) -> Tuple[NCTrainingPipeline, pandas.Series[Any]]

    Creates a node classification training pipeline in the pipeline catalog.

.. py:function:: gds.fastRP.mutate(G: Graph, **config: Any) -> pandas.Series[Any]

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.mutate.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.stats(G: Graph, **config: Any) -> pandas.Series[Any]

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.stats.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.stream(G: Graph, **config: Any) -> pandas.DataFrame

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.stream.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.write(G: Graph, **config: Any) -> pandas.Series[Any]

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.fastRP.write.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    Random Projection produces node embeddings via the fastrp algorithm

.. py:function:: gds.alpha.ml.oneHotEncoding(available_values: List[Any], selected_values: List[Any]) -> List[int]

    Return a list of selected values in a one hot encoding format.

.. deprecated:: 2.24.0
    Since GDS server version 2.24.0 you should use the endpoint :func:`gds.util.oneHotEncoding` instead.

.. py:function:: gds.hashgnn.mutate(G: Graph, **config: Any) -> pandas.Series[Any]

    HashGNN creates node embeddings by hashing and message passing.

.. py:function:: gds.hashgnn.mutate.estimate(G: Graph, **config: Any) -> pandas.DataFrame

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.hashgnn.stream(G: Graph, **config: Any) -> pandas.DataFrame

    HashGNN creates node embeddings by hashing and message passing.

.. py:function:: gds.hashgnn.stream.estimate(G: Graph, **config: Any) -> pandas.DataFrame

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.hashgnn.write(G: Graph, **config: Any) -> pandas.DataFrame

    HashGNN creates node embeddings by hashing and message passing.

.. py:function:: gds.hashgnn.write.estimate(G: Graph, **config: Any) -> pandas.DataFrame

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.node2vec.mutate(G: Graph, **config: Any) -> pandas.Series[Any]

    The Node2Vec algorithm computes embeddings for nodes based on random walks.

.. py:function:: gds.node2vec.mutate.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.node2vec.stream(G: Graph, **config: Any) -> pandas.DataFrame

    The Node2Vec algorithm computes embeddings for nodes based on random walks.

.. py:function:: gds.node2vec.stream.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.node2vec.write(G: Graph, **config: Any) -> pandas.Series[Any]

    The Node2Vec algorithm computes embeddings for nodes based on random walks.

.. py:function:: gds.node2vec.write.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    Returns an estimation of the memory consumption for that procedure.

.. py:function:: gds.splitRelationships.mutate(G: Graph, **config: Any) -> pandas.Series[Any]

    Splits a graph into holdout and remaining relationship types and adds them to the graph.

.. py:function:: gds.splitRelationships.mutate.estimate(G: Graph, **config: Any) -> pandas.Series[Any]

    Splits a graph into holdout and remaining relationship types and adds them to the graph.
