from abc import ABC, abstractmethod

import neo4j


class QueryRunner(ABC):
    @abstractmethod
    def run_query(self, query, params={}):
        pass


class Neo4jQueryRunner(QueryRunner):
    def __init__(self, driver, db=neo4j.DEFAULT_DATABASE):
        self._driver = driver
        self._db = db

    def run_query(self, query, params={}):
        with self._driver.session(database=self._db) as session:
            result = session.run(query, params)
            return result.data()

    def set_database(self, db):
        self._db = db
