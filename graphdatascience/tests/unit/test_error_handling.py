import pytest

from graphdatascience.graph_data_science import GraphDataScience


def test_calling_gds(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds' to call"):
        gds()


def test_nonexisting_direct_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.bogus' to call"):
        gds.bogus()


def test_nonexisting_indirect_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.bogus.thing' to call"):
        gds.bogus.thing()


def test_calling_graph(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.graph' to call"):
        gds.graph("hello")


def test_nonexisting_graph_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.graph.bogus' to call"):
        gds.graph.bogus("hello")  # type: ignore


def test_nonexisting_graph_export_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(
        SyntaxError, match="There is no 'gds.graph.export.bogus' to call"
    ):
        gds.graph.export.bogus("hello")  # type: ignore


def test_nonexisting_graph_project_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(
        SyntaxError, match="There is no 'gds.graph.project.bogus' to call"
    ):
        gds.graph.project.bogus("there")  # type: ignore


def test_calling_model(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.beta.model' to call"):
        gds.beta.model(42, 1337)


def test_nonexisting_model_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.beta.model.bogus' to call"):
        gds.beta.model.bogus(42, 1337)  # type: ignore


def test_calling_pipeline(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.beta.pipeline' to call"):
        gds.beta.pipeline(42, 1337)


def test_nonexisting_pipeline_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(
        SyntaxError, match="There is no 'gds.beta.pipeline.bogus' to call"
    ):
        gds.beta.pipeline.bogus(42, 1337)  # type: ignore


def test_calling_linkPrediction(gds: GraphDataScience) -> None:
    with pytest.raises(
        SyntaxError, match="There is no 'gds.beta.pipeline.linkPrediction' to call"
    ):
        gds.beta.pipeline.linkPrediction()


def test_nonexisting_linkPrediction_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(
        SyntaxError,
        match="There is no 'gds.beta.pipeline.linkPrediction.whoops' to call",
    ):
        gds.beta.pipeline.linkPrediction.whoops()  # type: ignore


def test_calling_nodeClassification(gds: GraphDataScience) -> None:
    with pytest.raises(
        SyntaxError,
        match="There is no 'gds.beta.pipeline.nodeClassification' to call",
    ):
        gds.beta.pipeline.nodeClassification(13.37)


def test_nonexisting_nodeClassification_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(
        SyntaxError,
        match="There is no 'gds.beta.pipeline.nodeClassification.whoops' to call",
    ):
        gds.beta.pipeline.nodeClassification.whoops(13.37)  # type: ignore


def test_calling_linkprediction(gds: GraphDataScience) -> None:
    with pytest.raises(
        SyntaxError, match="There is no 'gds.alpha.linkprediction' to call"
    ):
        gds.alpha.linkprediction(1, 2, direction="REVERSE")


def test_nonexisting_linkprediction_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(
        SyntaxError, match="There is no 'gds.alpha.linkprediction.adamicFoobar' to call"
    ):
        gds.alpha.linkprediction.adamicFoobar(1, 2, direction="REVERSE")  # type: ignore


def test_calling_debug(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.debug' to call"):
        gds.debug()


def test_nonexisting_debug_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.debug.sniffDumpo' to call"):
        gds.debug.sniffDumpo()  # type: ignore


def test_calling_util(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.util' to call"):
        gds.util()


def test_nonexisting_util_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.util.askNodez' to call"):
        gds.util.askNodez()  # type: ignore


def test_nonexisting_similarity_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(
        SyntaxError, match="There is no 'gds.alpha.similarity.pearson.bogus' to call"
    ):
        gds.alpha.similarity.pearson.bogus()  # type: ignore


def test_wrong_client_only_prefix(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.beta.model.get' to call"):
        gds.beta.model.get("model")
