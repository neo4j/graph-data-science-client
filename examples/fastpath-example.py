from graphdatascience import GraphDataScience
import numpy as np

gds = GraphDataScience(
    "neo4j+s://eddb7e19.databases.neo4j.io",
    auth=("neo4j", "Oz4oBK--Sx4byHjgHgJuMf5VqQncGHG9mbgpy44rQTU"),
    database="neo4j",
)
gds.set_compute_cluster_ip("localhost")

# Preprocessing
# gds.run_cypher("MATCH (p:Patient) SET p.has_diabetes=0")
# gds.run_cypher("MATCH (p:Patient)-[:HAS_ENCOUNTER]->(n:Encounter)-[:HAS_CONDITION]-(c:Condition) WHERE c.description='Diabetes' SET p.has_diabetes=1")
# gds.run_cypher("MATCH (n:Encounter) WITH toInteger(datetime(n.start).epochseconds/(24 * 3600)) as days, n SET n.days=days")
# gds.run_cypher("MATCH (p:Patient)-[:LAST]->(n:Encounter) SET p.output_time=n.days+1")
# gds.run_cypher("MATCH (p:Patient)-[:HAS_ENCOUNTER]->(e1:Encounter)-[:NEXT]->(e2:Encounter)-[:HAS_CONDITION]->(c:Condition) WHERE c.description='Diabetes' SET p.output_time=e1.days + 1")

try:
    G = gds.graph.get("medical")
    G.drop()
except:
    pass


G, _ = gds.graph.project(
    "medical",
    {
        "Patient": {"properties": ["output_time", "has_diabetes"]},
        "Encounter": {"properties": ["days"]},
        "Observation": {"properties": []},
        "Payer": {"properties": []},
        "Provider": {"properties": []},
        "Organization": {"properties": []},
        "Speciality": {"properties": []},
        "Allergy": {"properties": []},
        "Reaction": {"properties": []},
        "Condition": {"properties": []},
        "Drug": {"properties": []},
        "Procedure": {"properties": []},
        "CarePlan": {"properties": []},
        "Device": {"properties": []},
        "ConditionDescription": {"properties": []},
    },
    [
        "HAS_OBSERVATION",
        "HAS_ENCOUNTER",
        "HAS_PROVIDER",
        "AT_ORGANIZATION",
        "HAS_PAYER",
        "HAS_SPECIALITY",
        "BELONGS_TO",
        "INSURANCE_START",
        "INSURANCE_END",
        "HAS_ALLERGY",
        "ALLERGY_DETECTED",
        "HAS_REACTION",
        "CAUSES_REACTION",
        "HAS_CONDITION",
        "HAS_DRUG",
        "HAS_PROCEDURE",
        "HAS_CARE_PLAN",
        "DEVICE_USED",
    ],
)

# Optional
# graph_filter = {
#     "node_labels": {"Paper": ["subject"]},
#     "rel_types": {"CITES": []},
# }

gds.fastRP.mutate(
    G,
    embeddingDimension=256,
    mutateProperty="emb",
    iterationWeights=[1, 1],
    # featureProperties=[],
    # propertyRatio=1.0
)

embeddings = gds.fastpath.stream(
    G,
    # graph_filter=graph_filter,
    base_node_label="Patient",
    # context_node_label="Paper",
    event_node_label="Encounter",
    event_features="emb",
    # next_relationship_type="NEXT",
    # first_relationship_type="FIRST",
    #output_time_property="output_time",
    time_node_property="days",
    dimension=8,
    num_elapsed_times=30,
    num_output_times=10,
    max_elapsed_time=365 * 2,
    # max_elapsed_time=30,
    smoothing_rate=0.0,
    smoothing_window=2,
    decay_factor=0.0,
    #mlflow_experiment_name="test",
)
if "embeddings" in embeddings.columns:
    norms = [np.linalg.norm(x) for x in embeddings.embeddings]
    norm_sum = sum(norms)
    small = [x for x in norms if x < 0.001]
    num_small = len(small)

    breakpoint()
    print(embeddings)
else:
    breakpoint()
