GraphDataScience
----------------

.. autoclass:: graphdatascience.GraphDataScience
    :members:
    :inherited-members:

    .. autofunction:: graphdatascience.utils.util_endpoints.DirectUtilEndpoints.find_node_id
    .. autofunction:: graphdatascience.utils.util_endpoints.DirectUtilEndpoints.version
    .. autofunction:: graphdatascience.utils.util_endpoints.DirectUtilEndpoints.server_version
    .. autofunction:: graphdatascience.utils.util_endpoints.DirectUtilEndpoints.list

    .. attribute:: graph.

        .. autofunction:: graphdatascience.graph.graph_proc_runner.GraphProcRunner.load_cora
        .. autofunction:: graphdatascience.graph.graph_proc_runner.GraphProcRunner.load_karate_club
        .. autofunction:: graphdatascience.graph.graph_proc_runner.GraphProcRunner.load_imdb

        .. autofunction:: graphdatascience.graph.graph_proc_runner.GraphProcRunner.project

        .. attribute:: project.

            .. autofunction:: graphdatascience.graph.graph_project_runner.GraphProjectRunner.estimate
            .. autofunction:: graphdatascience.graph.graph_project_runner.GraphProjectRunner.cypher

            .. attribute:: cypher.

                .. autofunction:: graphdatascience.graph.graph_project_runner.GraphProjectRunner.estimate

    .. attribute:: beta.graph.

        .. autofunction:: graphdatascience.graph.graph_export_runner.GraphExportCsvEndpoints.csv
        .. autofunction:: graphdatascience.graph.graph_beta_proc_runner.GraphBetaProcRunner.generate

        .. attribute:: project.

            .. autofunction:: graphdatascience.graph.graph_project_runner.GraphProjectBetaRunner.subgraph

        .. attribute:: relationships.

            .. autofunction:: graphdatascience.graph.graph_entity_ops_runner.GraphRelationshipsBetaRunner.stream
            .. autofunction:: graphdatascience.graph.graph_entity_ops_runner.GraphRelationshipsBetaRunner.toUndirected

            .. attribute:: toUndirected.

                .. autofunction:: graphdatascience.graph.graph_entity_ops_runner.ToUndirectedRunner.estimate


    .. attribute:: alpha.graph.

        .. autofunction:: graphdatascience.graph.graph_alpha_proc_runner.GraphAlphaProcRunner.construct

    .. attribute:: alpha.graph.sample.

        .. autofunction:: graphdatascience.graph.graph_sample_runner.GraphSampleRunner.rwr

    .. attribute:: alpha.graph.graphProperty.

        .. autofunction:: graphdatascience.graph.graph_entity_ops_runner.GraphPropertyRunner.stream
        .. autofunction:: graphdatascience.graph.graph_entity_ops_runner.GraphPropertyRunner.drop

    .. attribute:: model.

        .. autofunction:: graphdatascience.model.model_proc_runner.ModelProcRunner.get

    .. attribute:: beta.model.

        .. autofunction:: graphdatascience.model.model_beta_proc_runner.ModelBetaProcRunner.list
        .. autofunction:: graphdatascience.model.model_beta_proc_runner.ModelBetaProcRunner.exists
        .. autofunction:: graphdatascience.model.model_beta_proc_runner.ModelBetaProcRunner.drop

    .. attribute:: alpha.model.

        .. autofunction:: graphdatascience.model.model_alpha_proc_runner.ModelAlphaProcRunner.publish
        .. autofunction:: graphdatascience.model.model_alpha_proc_runner.ModelAlphaProcRunner.store
        .. autofunction:: graphdatascience.model.model_alpha_proc_runner.ModelAlphaProcRunner.load
        .. autofunction:: graphdatascience.model.model_alpha_proc_runner.ModelAlphaProcRunner.delete

    .. attribute:: pipeline.

        .. autofunction:: graphdatascience.pipeline.pipeline_proc_runner.PipelineProcRunner.get

    .. attribute:: beta.pipeline.

        .. autofunction:: graphdatascience.pipeline.pipeline_beta_proc_runner.PipelineBetaProcRunner.list
        .. autofunction:: graphdatascience.pipeline.pipeline_beta_proc_runner.PipelineBetaProcRunner.exists
        .. autofunction:: graphdatascience.pipeline.pipeline_beta_proc_runner.PipelineBetaProcRunner.drop

    .. attribute:: util.

        .. autofunction:: graphdatascience.utils.util_proc_runner.UtilProcRunner.asNode
        .. autofunction:: graphdatascience.utils.util_proc_runner.UtilProcRunner.asNodes
        .. autofunction:: graphdatascience.utils.util_proc_runner.UtilProcRunner.nodeProperty

    .. attribute:: alpha.ml.

        .. autofunction:: graphdatascience.utils.util_endpoints.IndirectUtilAlphaEndpoints.oneHotEncoding
