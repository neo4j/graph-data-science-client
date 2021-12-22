from .graph_create_runner import GraphCreateRunner


class GraphEndpoints:
    def __init__(self, query_runner):
        self.query_runner = query_runner

    @property
    def create(self):
        return GraphCreateRunner(self.query_runner)
