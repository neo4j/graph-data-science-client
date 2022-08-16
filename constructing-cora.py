import pandas as pd
from graphdatascience import GraphDataScience

# Replace with whatever credentials and URI your DB is set up for
gds = GraphDataScience("bolt://localhost:7687")

# This is all copied from Nicola's notebook

CORA_CONTENT = (
    "https://raw.githubusercontent.com/neo4j/graph-data-science/master/test-utils/src/main/resources/cora.content"
)
CORA_CITES = (
    "https://raw.githubusercontent.com/neo4j/graph-data-science/master/test-utils/src/main/resources/cora.cites"
)
SUBJECT_TO_ID = {
    "Neural_Networks": 0,
    "Rule_Learning": 1,
    "Reinforcement_Learning": 2,
    "Probabilistic_Methods": 3,
    "Theory": 4,
    "Genetic_Algorithms": 5,
    "Case_Based": 6,
}

# Read each CSV locally as a Pandas DataFrame
content = pd.read_csv(CORA_CONTENT, header=None)
cites = pd.read_csv(CORA_CITES, header=None)

# Create a new DataFrame with a `nodeId` field, a list of node labels,
# and the additional node properties `subject` and `features`
nodes = pd.DataFrame().assign(
    nodeId=content[0],
    labels="Paper",
    subject=content[1].replace(SUBJECT_TO_ID),
    features=content.iloc[:, 2:].apply(list, axis=1),
)

# Create a new DataFrame containing the relationships between the nodes.
# To create the equivalent of an undirected graph, we need to add direct
# and inverse relationships.
dir_relationships = pd.DataFrame().assign(sourceNodeId=cites[0], targetNodeId=cites[1], relationshipType="CITES")

inv_relationships = pd.DataFrame().assign(sourceNodeId=cites[1], targetNodeId=cites[0], relationshipType="CITES")

relationships = pd.concat([dir_relationships, inv_relationships]).drop_duplicates()

# Finally, create the in-memory graph
G = gds.alpha.graph.construct("cora-graph", nodes, relationships)

result = gds.graph.list()

# The configuration map will be huge, containing all the nodes and their feature vectors
print(result["configuration"][0]["parameters"]["nodes"])
print(result["configuration"][0]["parameters"]["relationships"])

G.drop()
