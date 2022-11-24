from importlib.resources import path

import pandas as pd
from pandas import read_pickle

with path("graphdatascience.resources.imdb", "labels.pkl") as labels_resource:
    class_labels = read_pickle(labels_resource)
movies = pd.DataFrame([item for sublist in class_labels for item in sublist])
movies = movies.rename(columns={0: "nodeId", 1: "class"})
movies["labels"] = "Movie"

with path("graphdatascience.resources.imdb", "node_features.pkl") as features_resource:
    raw_features = read_pickle(features_resource)
nodes_features_df = pd.DataFrame(raw_features)
nodes_features_df["feature"] = nodes_features_df.values.tolist()
nodes_features_df = nodes_features_df["feature"].reset_index().rename(columns={"index": "nodeId"})

movie_nodes_with_features = movies.merge(nodes_features_df, on="nodeId", how="left")
person_nodes_with_features = (
    pd.merge(nodes_features_df, movies, on=["nodeId"], how="left", indicator=True)
    .query('_merge=="left_only"')
    .drop(columns=["class", "_merge"])
)
person_nodes_with_features["labels"] = "Person"
person_nodes_with_features["class"] = -1.0

with path("graphdatascience.resources.imdb", "edges.pkl") as rels_resource:
    spmatrices = read_pickle(rels_resource)

edge_list = []
for spmatrix in spmatrices:
    raw_adj_matrix = pd.DataFrame.sparse.from_spmatrix(spmatrix).stack().reset_index()  # type: ignore
    adj_matrix = raw_adj_matrix[raw_adj_matrix[0] != 0]
    edge_list.append(adj_matrix.iloc[:, :-1].rename(columns={"level_0": "sourceNodeId", "level_1": "targetNodeId"}))

edge_list[0]["relationshipType"] = "MovieDirector"
edge_list[1]["relationshipType"] = "MovieDirector"
edge_list[2]["relationshipType"] = "MovieActor"
edge_list[3]["relationshipType"] = "MovieActor"

labeled_rels = pd.concat(edge_list)

all_nodes = pd.concat([movie_nodes_with_features, person_nodes_with_features])
all_nodes.reset_index()

all_nodes.to_pickle(
    "FILE_LOC",
    protocol=4,
    compression="gzip",
)

labeled_rels.to_pickle(
    "FILE_LOC_2",
    protocol=4,
    compression="gzip",
)
