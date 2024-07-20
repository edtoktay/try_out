import psycopg2.pool
from psycopg2.extensions import register_adapter, AsIs
import numpy as np
from sql_operations_interface import SqlOperationsInterface


class PostgresOps(SqlOperationsInterface):
    def __init__(self, db, user, password, host, port=5432, min_connection=1, max_connection=3):
        self.db = db
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.min = min_connection
        self.max = max_connection
        self.pool = self.__create_pool()
        register_adapter(np.int64, AsIs)
        pass

    def __create_pool(self):
        return psycopg2.pool.SimpleConnectionPool(
            minconn=self.min,
            maxconn=self.max,
            database=self.db,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    def get_connection(self) -> any:
        conn = self.pool.getconn()
        conn.autocommit = True
        return conn

    def release_connection(self, connection) -> None:
        self.pool.putconn(conn=connection)
        pass

    def log_query(self, cursor: any, query: str, values: dict) -> str:
        return 'Running SQL: %s' % (cursor.mogrify(query=query, args=values))