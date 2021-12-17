from abc import ABC, abstractmethod


class QueryRunner(ABC):
    @abstractmethod
    def run_query(self, query, params={}):
        pass


class Neo4jQueryRunner(QueryRunner):
    def __init__(self, driver):
        self._driver = driver

    def run_query(self, query, params={}):
        with self._driver.session() as session:
            result = session.run(query, params)
            return result.data()
