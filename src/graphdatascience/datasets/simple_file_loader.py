import pathlib
from typing import NamedTuple

from neo4j import __version__ as neo4j_driver_version
from pandas import DataFrame, read_parquet


class GraphResources(NamedTuple):
    nodes: list[DataFrame]
    rels: list[DataFrame]


class SimpleDatasetLoader:
    def __init__(self) -> None:
        self._is_neo4j_4_driver = neo4j_driver_version.startswith("4.")

    @staticmethod
    def _path(package: str, resource: str) -> pathlib.Path:
        from importlib.resources import files

        # files() returns a Traversable, but usages require a Path object
        return pathlib.Path(str(files(package) / resource))

    def cora(self) -> GraphResources:
        file = self._path("graphdatascience.resources.cora", "cora_nodes.parquet.gzip")
        nodes = read_parquet(file)

        if self._is_neo4j_4_driver:
            # features is read as an ndarray which was not supported in neo4j 4
            nodes["features"] = nodes["features"].apply(lambda x: x.tolist())

        rels = read_parquet(self._path("graphdatascience.resources.cora", "cora_rels.parquet.gzip"))
        return GraphResources([nodes], [rels])

    def karate_club(self) -> GraphResources:
        nodes = DataFrame({"nodeId": range(1, 35)})
        nodes["labels"] = "Person"

        rels = read_parquet(self._path("graphdatascience.resources.karate", "karate_club.parquet.gzip"))

        return GraphResources([nodes], [rels])

    def imdb(self) -> GraphResources:
        package = "graphdatascience.resources.imdb"
        nodes = ["movies_with_genre", "movies_without_genre", "actors", "directors"]
        rels = ["acted_in", "directed_in"]

        node_dfs = []
        for n in nodes:
            resource = self._path(package, f"imdb_{n}.parquet.gzip")
            df = read_parquet(resource)
            if self._is_neo4j_4_driver:
                # features is read as an ndarray which was not supported in neo4j 4
                df["plot_keywords"] = df["plot_keywords"].apply(lambda x: x.tolist())
            node_dfs.append(df)

        rel_dfs = []
        for r in rels:
            resource = self._path(package, f"imdb_{r}.parquet.gzip")
            rel_dfs.append(read_parquet(resource))
        return GraphResources(node_dfs, rel_dfs)

    def lastfm(self) -> GraphResources:
        nodes = ["user_nodes", "artist_nodes"]
        rels = ["user_friend_df_directed", "user_listen_artist_rels", "user_tag_artist_rels"]

        package = "graphdatascience.resources.lastfm"

        node_dfs = []
        for n in nodes:
            resource = self._path(package, f"{n}.parquet.gzip")
            node_dfs.append(read_parquet(resource))

        rel_dfs = []
        for r in rels:
            resource = self._path(package, f"{r}.parquet.gzip")
            rel_dfs.append(read_parquet(resource))

        return GraphResources(node_dfs, rel_dfs)
