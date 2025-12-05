# A list of the server endpoints that should not be reachable via the standard
# string builder construction on the `GraphDataScience` class.
# If a new server endpoint is added to pipelines, the coverage test will fail
# and the new endpoint has to be added to this list, as well as to the pipelines
# classes' implementations.
# Additionally, these endpoints should not be given as suggestions in error
# messages.
IGNORED_SERVER_ENDPOINTS = {
    "gds.alpha.graph.removeGraphProperty",  # Exists but undocumented for GDS 2.1
    "gds.alpha.graph.streamGraphProperty",  # Exists but undocumented for GDS 2.1
    "gds.alpha.pipeline.linkPrediction.addMLP",
    "gds.alpha.pipeline.linkPrediction.addRandomForest",
    "gds.beta.pipeline.linkPrediction.addRandomForest",
    "gds.beta.pipeline.linkPrediction.addFeature",
    "gds.beta.pipeline.linkPrediction.addLogisticRegression",
    "gds.beta.pipeline.linkPrediction.addNodeProperty",
    "gds.alpha.pipeline.linkPrediction.configureAutoTuning",
    "gds.beta.pipeline.linkPrediction.configureSplit",
    "gds.beta.pipeline.linkPrediction.predict.mutate",
    "gds.beta.pipeline.linkPrediction.predict.mutate.estimate",
    "gds.beta.pipeline.linkPrediction.predict.stream",
    "gds.beta.pipeline.linkPrediction.predict.stream.estimate",
    "gds.beta.pipeline.linkPrediction.train",
    "gds.beta.pipeline.linkPrediction.train.estimate",
    "gds.alpha.pipeline.nodeClassification.addMLP",
    "gds.alpha.pipeline.nodeClassification.addRandomForest",
    "gds.beta.pipeline.nodeClassification.addRandomForest",
    "gds.beta.pipeline.nodeClassification.addLogisticRegression",
    "gds.beta.pipeline.nodeClassification.addNodeProperty",
    "gds.alpha.pipeline.nodeClassification.configureAutoTuning",
    "gds.beta.pipeline.nodeClassification.configureSplit",
    "gds.beta.pipeline.nodeClassification.predict.mutate",
    "gds.beta.pipeline.nodeClassification.predict.mutate.estimate",
    "gds.beta.pipeline.nodeClassification.predict.stream",
    "gds.beta.pipeline.nodeClassification.predict.stream.estimate",
    "gds.beta.pipeline.nodeClassification.predict.write",
    "gds.beta.pipeline.nodeClassification.predict.write.estimate",
    "gds.beta.pipeline.nodeClassification.selectFeatures",
    "gds.beta.pipeline.nodeClassification.train",
    "gds.beta.pipeline.nodeClassification.train.estimate",
    "gds.alpha.pipeline.nodeRegression.addRandomForest",
    "gds.alpha.pipeline.nodeRegression.addLinearRegression",
    "gds.alpha.pipeline.nodeRegression.addNodeProperty",
    "gds.alpha.pipeline.nodeRegression.configureAutoTuning",
    "gds.alpha.pipeline.nodeRegression.configureSplit",
    "gds.alpha.pipeline.nodeRegression.predict.mutate",
    "gds.alpha.pipeline.nodeRegression.predict.stream",
    "gds.alpha.pipeline.nodeRegression.selectFeatures",
    "gds.alpha.pipeline.nodeRegression.train",
    "gds.similarity.cosine",
    "gds.similarity.euclidean",
    "gds.similarity.euclideanDistance",
    "gds.similarity.jaccard",
    "gds.similarity.overlap",
    "gds.similarity.pearson",
    "gds.util.NaN",
    "gds.util.infinity",
    "gds.util.isFinite",
    "gds.util.isInfinite",
    "gds.isLicensed",  # mapped through gds.is_licensed
    "gds.alpha.graph.project",  # TODO: Figure out how to support this well
    "gds.ephemeral.database.create",
    "gds.ephemeral.database.drop",
    "gds.alpha.create.cypherdb",
    "gds.alpha.drop.cypherdb",  # previous name of gds.ephemeral.database.create
}
