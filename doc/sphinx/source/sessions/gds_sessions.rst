GDS Sessions
----------------

.. toctree::
   :hidden:

   session_memory
   algorithm_category
   cloud_location
   dbms_connection_info
   aura_graphdatascience
   job_handles
   gds_arrow_client
   query_runner_types


.. autoclass:: graphdatascience.session.gds_sessions.GdsSessions
    :members:
    :inherited-members:


.. autoclass:: graphdatascience.session.gds_sessions.AuraAPICredentials
    :members:
    :inherited-members:
    :exclude-members: client_id, client_secret, project_id

.. autoclass:: graphdatascience.session.gds_sessions.SessionInfo
    :members:
    :inherited-members:
    :exclude-members: cloud_location, created_at, errors, expiry_date, id, instance_id, memory, name, status, termination_reason, ttl, user_id

.. autoclass:: graphdatascience.session.graph_mapping_context.GraphMappingContext
    :members:
    :inherited-members: