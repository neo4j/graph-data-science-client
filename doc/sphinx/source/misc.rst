Miscellaneous procedures
-------------------------
All the miscellaneous procedures including configuring the library.

.. py:function:: gds.alpha.config.defaults.list(self, key: Optional[str] = None, username: Optional[str] = None) -> DataFrame

    List defaults; global by default, but also optionally for a specific user and/ or key

.. py:function:: gds.alpha.config.defaults.set(self, key: str, value: Any, username: Optional[str] = None) -> None

    Set a default; global by, default, but also optionally for a specific user

.. py:function:: gds.alpha.config.limits.list(self, key: Optional[str] = None, username: Optional[str] = None) -> DataFrame

    List limits; global by default, but also optionally for a specific user and/ or key

.. py:function:: gds.alpha.config.limits.set(self, key: str, value: Any, username: Optional[str] = None) -> None

    Set a limit; global by, default, but also optionally for a specific user

.. py:function:: gds.alpha.systemMonitor(self) -> "Series[Any]"

    Get an overview of the system's workload and available resources

.. py:function:: gds.alpha.userLog(self) -> DataFrame

    Log warnings and hints for currently running tasks.

.. py:function:: gds.beta.listProgress(self, job_id: Optional[str] = None) -> DataFrame

    List progress events for currently running tasks.

.. py:function:: gds.debug.sysInfo(self) -> "Series[Any]"

    Returns details about the status of the system

.. py:function:: gds.util.asNode(self, node_id: int) -> Any

    RETURN gds.util.asNode(nodeId) - Return the node objects for the given node id or null if none exists.

.. py:function:: gds.util.asNodes(self, node_ids: List[int]) -> List[Any]

    RETURN gds.util.asNodes(nodeIds) - Return the node objects for the given node ids or an empty list if none exists.

.. py:function:: gds.util.nodeProperty(self, G: Graph, node_id: int, property_key: str, node_label: str = "*") -> Any

    Returns a node property value from a named in-memory graph.

.. py:function:: gds.version(self) -> str

    Return the installed graph data science library version.

.. py:function:: gds.is_licensed(self) -> bool

    Return True if the graph data science library is licensed.
