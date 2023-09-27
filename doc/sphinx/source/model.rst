Model procedures
----------------
Listing of all model procedures in the Neo4j Graph Data Science Python Client API.
These all assume that an object of :class:`.GraphDataScience` is available as `gds`.


.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.backup` instead.

.. py:function:: gds.alpha.backup(**config) -> DataFrame

    The back-up procedure persists graphs and models to disk

.. py:function:: gds.alpha.model.delete(model: Model) -> Series[Any]

    Deletes a stored model from disk.

.. py:function:: gds.alpha.model.load(model_name: str) -> Tuple[Model, Series[Any]]

    Load a stored model into main memory.

.. py:function:: gds.alpha.model.publish(model: Model) -> Model

    Make a trained model accessible by all users.

.. py:function:: gds.alpha.model.store(model: Model, failIfUnsupportedType: bool = True) -> Series[Any]

    Store the selected model to disk.

.. deprecated:: 2.5.0
   Since GDS server version 2.5.0 you should use the endpoint :func:`gds.restore` instead.

.. py:function:: gds.alpha.restore(**config: Any) -> DataFrame

    The restore procedure reads graphs and models from disk.

.. py:function:: gds.backup(**config) -> DataFrame

    The back-up procedure persists graphs and models to disk

.. py:function:: gds.beta.model.drop(model: Model) -> Series[Any]

    Drops a loaded model and frees up the resources it occupies.

.. py:function:: gds.beta.model.exists(model_name: str) -> Series[Any]

    Checks if a given model exists in the model catalog.

.. py:function:: gds.beta.model.list(model: Optional[Model] = None) -> DataFrame

    Lists all models contained in the model catalog.

.. py:function:: gds.model.get(model_name: str) -> Model

    Returns a model from the model catalog.

.. py:function:: gds.restore(**config: Any) -> DataFrame

    The restore procedure reads graphs and models from disk.

.. py:function:: gds.model.delete(model: Model) -> Series[Any]

    Deletes a stored model from disk.

.. py:function:: gds.model.load(model_name: str) -> Tuple[Model, Series[Any]]

    Load a stored model into main memory.

.. py:function:: gds.model.publish(model: Model) -> Model

    Make a trained model accessible by all users.

.. py:function:: gds.model.store(model: Model, failIfUnsupportedType: bool = True) -> Series[Any]

    Store the selected model to disk.

.. py:function:: gds.model.drop(model: Model) -> Series[Any]

    Drops a loaded model and frees up the resources it occupies.

.. py:function:: gds.model.exists(model_name: str) -> Series[Any]

    Checks if a given model exists in the model catalog.

.. py:function:: gds.model.list(model: Optional[Model] = None) -> DataFrame

    Lists all models contained in the model catalog.

.. py:function:: gds.model.transe.create(G: Graph, node_embedding_property: str, relationship_type_embeddings: Dict[str, List[float]) -> SimpleRelEmbeddingModel

    Create a TransE relationship embedding model

    :param G: The `Graph` object representing the graph projection the model is trained on
    :param node_embedding_property: The name of the node property under which the TransE embeddings are stored
    :param relationship_type_embeddings: A dictionary mapping relationship type names to the model's relationship type embeddings

    :returns: A relationship embedding model that can be used to predict new node pair based on the TransE metric

.. py:function:: gds.model.distmult.create(G: Graph, node_embedding_property: str, relationship_type_embeddings: Dict[str, List[float]) -> SimpleRelEmbeddingModel

    Create a DistMult relationship embedding model

    :param G: The `Graph` object representing the graph projection the model is trained on
    :param node_embedding_property: The name of the node property under which the DistMult embeddings are stored
    :param relationship_type_embeddings: A dictionary mapping relationship type names to the model's relationship type embeddings

    :returns: A relationship embedding model that can be used to predict new node pair based on the DistMult metric
