import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest import mock


def test_duplicate_node_regression_test_modules_collect_without_import_mismatch() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    command = [
        sys.executable,
        "-m",
        "pytest",
        "--collect-only",
        "-q",
        str(repo_root / "tests/unit/procedure_surface/arrow/pipeline/test_node_regression_pipeline_arrow_endpoints.py"),
        str(
            repo_root
            / "tests/integrationV2/procedure_surface/arrow/pipeline/test_node_regression_pipeline_arrow_endpoints.py"
        ),
        str(
            repo_root / "tests/unit/procedure_surface/cypher/pipeline/test_node_regression_pipeline_cypher_endpoints.py"
        ),
        str(
            repo_root
            / "tests/integrationV2/procedure_surface/cypher/pipeline/test_node_regression_pipeline_cypher_endpoints.py"
        ),
    ]

    result = subprocess.run(command, capture_output=True, text=True, cwd=repo_root, check=False)

    assert result.returncode == 0, result.stdout + result.stderr


def test_node_regression_pipeline_slice_avoids_type_checking_tricks() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    target_files = [
        "src/graphdatascience/procedure_surface/api/pipeline/node_regression_pipeline.py",
        "src/graphdatascience/procedure_surface/api/pipeline/node_regression_pipeline_endpoints.py",
        "src/graphdatascience/procedure_surface/api/pipeline/node_regression_pipeline_protocol.py",
    ]

    for file_name in target_files:
        assert "TYPE_CHECKING" not in (repo_root / file_name).read_text()


def test_node_regression_pipeline_protocol_drops_internal_handle_types() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    protocol_module = (
        repo_root / "src/graphdatascience/procedure_surface/api/pipeline/node_regression_pipeline_protocol.py"
    )

    from graphdatascience.procedure_surface.api.pipeline.node_regression_pipeline_protocol import (
        NodeRegressionPipelineOps,
        NodeRegressionPipelineTrainer,
    )

    assert NodeRegressionPipelineOps.__name__ == "NodeRegressionPipelineOps"
    assert NodeRegressionPipelineTrainer.__name__ == "NodeRegressionPipelineTrainer"
    assert "NodeRegressionPipelineHandle" not in protocol_module.read_text()
    assert "NodeRegressionPipelineRef" not in protocol_module.read_text()
    assert "NodeRegressionPipelineImpl" not in protocol_module.read_text()
    assert "NodeRegressionPipelineLifecycle" not in protocol_module.read_text()


def test_node_regression_pipeline_accepts_separate_ops_and_trainer() -> None:
    from graphdatascience.procedure_surface.api.pipeline.node_regression_pipeline import NodeRegressionPipeline
    from graphdatascience.procedure_surface.api.pipeline.node_regression_pipeline_protocol import (
        NodeRegressionPipelineOps,
        NodeRegressionPipelineTrainer,
    )

    class DummyOps:
        def add_node_property(self, pipeline_name: str, procedure_name: str, **config: Any) -> str:
            return f"{pipeline_name}:{procedure_name}:{config['mutate_property']}"

        def select_features(self, pipeline_name: str, node_properties: str | list[str]) -> str:
            return f"{pipeline_name}:{node_properties}"

        def add_linear_regression(self, pipeline_name: str, **_: Any) -> str:
            return pipeline_name

        def add_random_forest(self, pipeline_name: str, **_: Any) -> str:
            return pipeline_name

        def configure_split(self, pipeline_name: str, **_: Any) -> str:
            return pipeline_name

        def configure_auto_tuning(self, pipeline_name: str, **_: Any) -> str:
            return pipeline_name

    class DummyTrainer:
        def __init__(self) -> None:
            self.pipeline_name: str | None = None

        def train(self, _: Any, pipeline_name: str, **__: Any) -> tuple[None, str]:
            self.pipeline_name = pipeline_name
            return None, "trained"

    ops = DummyOps()
    trainer = DummyTrainer()

    assert isinstance(ops, NodeRegressionPipelineOps)
    assert isinstance(trainer, NodeRegressionPipelineTrainer)

    pipeline = NodeRegressionPipeline("pipe", ops, trainer)

    assert pipeline.add_node_property("pageRank", mutate_property="score") == "pipe:pageRank:score"
    assert pipeline.select_features(node_properties=["score"]) == "pipe:['score']"
    assert pipeline.train(mock.Mock(), metrics=["MAE"], model_name="model", target_property="target") == (
        None,
        "trained",
    )
    assert trainer.pipeline_name == "pipe"
