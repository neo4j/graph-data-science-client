Model operations
----------------


.. py:function:: gds.alpha.backup(self, **config) -> DataFrame

    The back-up procedure persists graphs and models to disk


.. py:function:: gds.alpha.model.delete(self, model: Model) -> "Series[Any]"

    Deletes a stored model from disk.

.. py:function:: gds.alpha.model.load(self, model_name: str) -> Tuple[Model, "Series[Any]"]

    Load a stored model into main memory.

.. py:function:: gds.alpha.model.publish(self, model: Model) -> Model

    Make a trained model accessible by all users.

.. py:function:: gds.alpha.model.store(self, model: Model, failIfUnsupportedType: bool = True) -> "Series[Any]"

    Store the selected model to disk.

.. py:function:: gds.alpha.restore(self, **config: Any) -> DataFrame

    The restore procedure reads graphs and models from disk.

.. py:function:: gds.beta.model.drop(self, model: Model) -> "Series[Any]"

    Drops a loaded model and frees up the resources it occupies.

.. py:function:: gds.beta.model.exists(self, model_name: str) -> "Series[Any]"

    Checks if a given model exists in the model catalog.

.. py:function:: gds.beta.model.list(self, model: Optional[Model] = None) -> DataFrame

    Lists all models contained in the model catalog.
