from importlib.resources import path

import pandas as pd
from pandas import read_pickle

"""
This code included only for reproducibility.

raw source: https://github.com/seongjunyun/Graph_Transformer_Networks/tree/master/prev_GTN
It is used to convert raw pickle data from edges.pkl, labels.pkl and node_features.pkl
into consolidated imdb_nodes_gzip.pkl and imdb_rels_gzip.pkl.

These pickles are then being read by load_imdb()
"""

with path("graphdatascience.resources.imdb", "raw/labels.pkl") as labels_resource:
    class_labels = read_pickle(labels_resource)
movies = pd.DataFrame([item for sublist in class_labels for item in sublist])
movies = movies.rename(columns={0: "nodeId", 1: "genre"})
movies["labels"] = "Movie"

with path("graphdatascience.resources.imdb", "raw/node_features.pkl") as features_resource:
    raw_features = read_pickle(features_resource)
nodes_features_df = pd.DataFrame(raw_features)
nodes_features_df["plot_keywords"] = nodes_features_df.values.tolist()
nodes_features_df = nodes_features_df["plot_keywords"].reset_index().rename(columns={"index": "nodeId"})

movie_nodes_with_features = movies.merge(nodes_features_df, on="nodeId", how="left")
movie_nodes_with_features.reset_index(drop=True, inplace=True)

other_nodes_with_features = (
    pd.merge(nodes_features_df, movies, on=["nodeId"], how="left", indicator=True)
    .query('_merge=="left_only"')
    .drop(columns=["genre", "_merge"])
)

unlabeled_movies = other_nodes_with_features["nodeId"] < 4661
unlabeled_movies_df = other_nodes_with_features[unlabeled_movies]
unlabeled_movies_df["labels"] = "UnclassifiedMovie"
unlabeled_movies_df.reset_index(drop=True, inplace=True)

directors_df = other_nodes_with_features[
    (other_nodes_with_features["nodeId"] >= 4661) & (other_nodes_with_features["nodeId"] <= 6930)
]
directors_df["labels"] = "Director"
directors_df.reset_index(drop=True, inplace=True)

actors_df = other_nodes_with_features[(other_nodes_with_features["nodeId"] >= 6931)]
actors_df["labels"] = "Actor"
actors_df.reset_index(drop=True, inplace=True)

movie_nodes_with_features.to_parquet(
    "/FILE_LOC/imdb_movies_with_genre.parquet.gzip",
    protocol=4,
    compression="gzip",
)

unlabeled_movies_df.to_parquet(
    "/FILE_LOC/imdb_movies_without_genre.parquet.gzip",
    protocol=4,
    compression="gzip",
)

directors_df.to_parquet(
    "/FILE_LOC/imdb_directors.parquet.gzip",
    protocol=4,
    compression="gzip",
)

actors_df.to_parquet(
    "/FILE_LOC/imdb_actors.parquet.gzip",
    compression="gzip",
)


with path("graphdatascience.resources.imdb", "raw/edges.pkl") as rels_resource:
    spmatrices = read_pickle(rels_resource)

edge_list = []
for spmatrix in spmatrices:
    raw_adj_matrix = pd.DataFrame.sparse.from_spmatrix(spmatrix).stack().reset_index()  # type: ignore
    adj_matrix = raw_adj_matrix[raw_adj_matrix[0] != 0]
    edge_list.append(adj_matrix.iloc[:, :-1].rename(columns={"level_0": "sourceNodeId", "level_1": "targetNodeId"}))

edge_list[0]["relationshipType"] = "DIRECTED_IN"
edge_list[1]["relationshipType"] = "DIRECTED_IN"
edge_list[2]["relationshipType"] = "ACTED_IN"
edge_list[3]["relationshipType"] = "ACTED_IN"

edge_list[1].reset_index(drop=True, inplace=True)
edge_list[3].reset_index(drop=True, inplace=True)

edge_list[1].to_parquet(
    "/FILE_LOC/imdb_directed_in_rels.parquet.gzip",
    protocol=4,
    compression="gzip",
)

edge_list[3].to_parquet(
    "/FILE_LOC/imdb_acted_in.parquet.gzip",
    compression="gzip",
)
