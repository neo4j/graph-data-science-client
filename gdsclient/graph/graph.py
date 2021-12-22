class Graph:
    def __init__(self, name, query_runner):
        self._name = name
        self._query_runner = query_runner

    def name(self):
        return self._name

    def _graph_info(self):
        info = self._query_runner.run_query(
            "CALL gds.graph.list($graph_name)", {"graph_name": self._name}
        )

        if len(info) == 0:
            raise ValueError(f"There is no projected graph named '{self.name()}'")

        return info[0]

    def node_count(self):
        return self._graph_info()["nodeCount"]

    def relationship_count(self):
        return self._graph_info()["relationshipCount"]

    def node_properties(self, label):
        labels_to_props = self._graph_info()["schema"]["nodes"]
        if label not in labels_to_props.keys():
            raise ValueError(
                f"There is no node label '{label}' projected onto '{self.name()}'"
            )

        return list(labels_to_props[label].keys())

    def relationship_properties(self, type):
        types_to_props = self._graph_info()["schema"]["relationships"]
        if type not in types_to_props.keys():
            raise ValueError(
                f"There is no relationship type '{type}' projected onto '{self.name()}'"
            )

        return list(types_to_props[type].keys())

    def degree_distribution(self):
        return self._graph_info()["degreeDistribution"]

    def density(self):
        return self._graph_info()["density"]

    def memory_usage(self):
        return self._graph_info()["memoryUsage"]

    def size_in_bytes(self):
        return self._graph_info()["sizeInBytes"]
