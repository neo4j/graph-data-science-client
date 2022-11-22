import pandas as pd

from graphdatascience import GraphDataScience

# Use Neo4j URI and credentials according to your setup
gds = GraphDataScience("bolt://localhost:7687", auth=None)

CORA_CONTENT = (
    "https://raw.githubusercontent.com/neo4j/graph-data-science/master/test-utils/src/main/resources/cora.content"
)
CORA_CITES = (
    "https://raw.githubusercontent.com/neo4j/graph-data-science/master/test-utils/src/main/resources/cora.cites"
)

content = pd.read_csv(CORA_CONTENT, header=None)
cites = pd.read_csv(CORA_CITES, header=None)


SUBJECT_TO_ID = {
    "Neural_Networks": 0,
    "Rule_Learning": 1,
    "Reinforcement_Learning": 2,
    "Probabilistic_Methods": 3,
    "Theory": 4,
    "Genetic_Algorithms": 5,
    "Case_Based": 6,
}

nodes = pd.DataFrame().assign(
    nodeId=content[0],
    labels="Paper",
    subject=content[1].replace(SUBJECT_TO_ID),
    features=content.iloc[:, 2:].apply(list, axis=1),
)

relationships = pd.DataFrame().assign(sourceNodeId=cites[0], targetNodeId=cites[1], relationshipType="CITES")

# protocol 4 to work with python 3.7
nodes.to_pickle("./cora_nodes_gzip_2.pkl", protocol=4, compression="gzip")
relationships.to_pickle("./cora_rels_gzip_2.pkl", protocol=4, compression="gzip")
