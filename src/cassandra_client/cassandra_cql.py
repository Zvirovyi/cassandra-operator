#!/usr/bin/env python3
import logging
from typing import List, Optional
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider

logger = logging.getLogger(__name__)

class CassandraClient:
    def __init__(
        self,
        host_list: List[str],
        user: Optional[str] = None,
        password: Optional[str] = None
    ):
        
        self.host_list = host_list
        self.user = user
        self.password = password

        if self.user is not None and self.password is not None:
            self.auth_provider = PlainTextAuthProvider(username=self.user, password=self.password)

        return

    class _SessionContext:
        def __init__(self, client: 'CassandraClient', keyspace: Optional[str] = None):
            self.client = client
            self.keyspace = keyspace
            self.cluster = None
            self.session = None

        def __enter__(self) -> Session:
            self.cluster = Cluster(self.client.host_list, auth_provider=self.client.auth_provider)
            self.session = self.cluster.connect()
            if self.keyspace:
                self.session.set_keyspace(self.keyspace)
            return self.session

        def __exit__(self, exc_type, exc_val, exc_tb):
            if self.cluster:
                self.cluster.shutdown()

    def create_keyspace(self, keyspace: str) -> None:
        with self._SessionContext(self, keyspace=None) as session:
            query = """
                CREATE KEYSPACE IF NOT EXISTS %s
                WITH replication = {
                    'class': 'SimpleStrategy',
                    'replication_factor': 1
                }
            """ % keyspace
            session.execute(query)

    def create_table(self, keyspace: str, table_name: str) -> None:
        with self._SessionContext(self, keyspace=keyspace) as session:
            query = """
                CREATE TABLE IF NOT EXISTS %s (
                    id UUID PRIMARY KEY,
                    name text
                )
            """ % table_name
            session.execute(query)
