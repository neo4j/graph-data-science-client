from graphdatascience.datasets.graph_constructor_func import GraphConstructorFunc
from graphdatascience.datasets.ogb_loader import OGBLLoader, OGBNLoader
from graphdatascience.datasets.simple_file_loader import SimpleDatasetLoader
from graphdatascience.graph.v2 import GraphV2


class DatasetEndpoints:
    def __init__(self, graph_constructor: GraphConstructorFunc) -> None:
        self.construct = graph_constructor
        self._simple_dataset_loader = SimpleDatasetLoader()

    def load_cora(self, graph_name: str = "cora", undirected: bool = False) -> GraphV2:
        """
        A citation network introduced.

        Parameters
        ----------
        graph_name: str
            Name of the graph to be created
        undirected: bool
            Whether the graph should be undirected

        Returns
        --------
        GraphV2
            A handle to the graph.
        """
        nodes, rels = self._simple_dataset_loader.cora()
        undirected_relationship_types: list[str] = ["*"] if undirected else []
        return self.construct(graph_name, nodes, rels, undirected_relationship_types=undirected_relationship_types)

    def load_karate_club(self, graph_name: str = "karate_club", undirected: bool = False) -> GraphV2:
        """
        A social network introduced by http://konect.cc/networks/ucidata-zachary/[Zachary].

        Parameters
        ----------
        graph_name: str
            Name of the graph to be created
        undirected: bool
            Whether the graph should be undirected

        Returns
        --------
        GraphV2
            A handle to the graph.
        """
        nodes, rels = self._simple_dataset_loader.karate_club()
        undirected_relationship_types = ["*"] if undirected else []

        return self.construct(graph_name, nodes, rels, undirected_relationship_types=undirected_relationship_types)

    def load_imdb(self, graph_name: str = "imdb", undirected: bool = True) -> GraphV2:
        """
        A heterogeneous graph that is used to benchmark node classification or link prediction models.

        The graph contains Actors, Directors, Movies (and UnclassifiedMovies) as nodes, and relationships between actors and movies that they acted in,
        and between directors and movies which they directed for.

        Parameters
        ----------
        graph_name: str
            Name of the graph to be created
        undirected: bool
            Whether the graph should be undirected

        Returns
        --------
        GraphV2
            A handle to the graph.
        """
        node_dfs, rel_dfs = self._simple_dataset_loader.imdb()
        # Default undirected which matches raw data
        undirected_relationship_types = ["*"] if undirected else []

        return self.construct(
            graph_name, node_dfs, rel_dfs, undirected_relationship_types=undirected_relationship_types
        )

    def load_lastfm(self, graph_name: str = "lastfm", undirected: bool = True) -> GraphV2:
        """
        A heterogeneous graph that is used to benchmark link prediction models.
        The original raw data is from http://www.lastfm.com/[LastFM].

        The graph contains User and Artists as nodes, each with a `rawId` field that corresponds to the ids from the raw data.
        The `rawId` could be used, for example, to check the artist names by referring back to the HetRec'11 .dat files.

        Parameters
        ----------
        graph_name: str
            Name of the graph to be created
        undirected: bool
            Whether the graph should be undirected

        Returns
        --------
        GraphV2
            A handle to the graph.
        """
        node_dfs, rel_dfs = self._simple_dataset_loader.lastfm()

        # Default undirected for usage in GDS ML pipelines
        if undirected:
            undirected_relationship_types = ["LISTEN_TO", "TAGGED", "IS_FRIEND"]
        else:
            undirected_relationship_types = []

        return self.construct(
            graph_name, node_dfs, rel_dfs, undirected_relationship_types=undirected_relationship_types
        )

    @property
    def ogbn(self) -> OGBNLoader:
        """
        Datasets used for node property prediction.
        """
        return OGBNLoader(self.construct)

    @property
    def ogbl(self) -> OGBLLoader:
        """
        Datasets used for link property prediction.
        """
        return OGBLLoader(self.construct)

    @property
    def networkx(self):  # type:ignore
        """
        Convenience wrapper to load networkx graphs into the graph catalog.

        Returns
        -------
        NXLoader
        """
        try:
            from graphdatascience.datasets.nx_loader import NXLoader
        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                "This feature requires NetworkX support. "
                "You can add NetworkX support by running `pip install graphdatascience[networkx]`"
            )

        return NXLoader(self.construct)
