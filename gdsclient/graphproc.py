class GDS:
    @property
    def graph(self):
        return GraphProc


class GraphProc:
    def create(graph_name, node_projection, relationship_projection):
        return Graph(graph_name, node_projection, relationship_projection)


class Graph:
    def __init__(self, graph_name, node_projection, relationship_projection):
        self.graph_name = graph_name
        self.node_projection = node_projection
        self.relationship_projection = relationship_projection
