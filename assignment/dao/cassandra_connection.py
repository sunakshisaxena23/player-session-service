
from cassandra.cluster import Cluster
from cassandra.cqlengine import connection


def Singleton(cls):

    instances = {}
    def getInstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]
    return getInstance

class CassandraConnection:

    """will return a session for the cluster"""
    def create_connection(self, host, keyspace):
        """ Connect with specific host & port """
        connection.setup([host, ], "cqlengine", protocol_version=3)
        self._db_connection = Cluster()
        self._db_cur = self._db_connection.connect(keyspace=keyspace)

