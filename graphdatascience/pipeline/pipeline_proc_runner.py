from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..model.pipeline_model import PipelineModel
from .lp_training_pipeline import LPTrainingPipeline
from .nc_training_pipeline import NCTrainingPipeline
from .nr_training_pipeline import NRTrainingPipeline
from .training_pipeline import TrainingPipeline


class PipelineProcRunner(UncallableNamespace, IllegalAttrChecker):
    @client_only_endpoint("gds.pipeline")
    def get(self, pipeline_name: str) -> TrainingPipeline[PipelineModel]:
        query = "CALL gds.beta.pipeline.list($pipeline_name)"
        params = {"pipeline_name": pipeline_name}
        result = self._query_runner.run_query(query, params, custom_error=False)

        if len(result) == 0:
            raise ValueError(f"No pipeline named '{pipeline_name}' exists")

        pipeline_type = result["pipelineType"][0]
        return self._resolve_pipeline(pipeline_type, pipeline_name)

    def _resolve_pipeline(self, pipeline_type: str, pipeline_name: str) -> TrainingPipeline[PipelineModel]:
        if pipeline_type == "Node classification training pipeline":
            return NCTrainingPipeline(pipeline_name, self._query_runner, self._server_version)
        elif pipeline_type == "Link prediction training pipeline":
            return LPTrainingPipeline(pipeline_name, self._query_runner, self._server_version)
        elif pipeline_type == "Node regression training pipeline":
            return NRTrainingPipeline(pipeline_name, self._query_runner, self._server_version)

        raise ValueError(f"Unknown model type encountered: '{pipeline_type}'")
