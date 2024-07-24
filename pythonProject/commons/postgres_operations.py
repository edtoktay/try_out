import psycopg2.pool
from psycopg2.extensions import register_adapter, AsIs
import numpy as np
from sql_operations_interface import SqlOperationsInterface


class PostgresOps(SqlOperationsInterface):
    """
    A class that provides operations for interacting with a PostgreSQL database.

    Args:
        db (str): The name of the database.
        user (str): The username for authentication.
        password (str): The password for authentication.
        host (str): The host address of the database server.
        port (int, optional): The port number of the database server. Defaults to 5432.
        min_connection (int, optional): The minimum number of connections in the connection pool. Defaults to 1.
        max_connection (int, optional): The maximum number of connections in the connection pool. Defaults to 3.
    """

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
        """
        Creates a connection pool for PostgreSQL database.

        Returns:
            psycopg2.pool.SimpleConnectionPool: The connection pool object.
        """
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
        """
        Retrieves a connection from the connection pool.

        Returns:
            any: The database connection object.
        """
        conn = self.pool.getconn()
        conn.autocommit = True
        return conn

    def release_connection(self, connection) -> None:
        """
        Releases a connection back to the connection pool.

        Args:
            connection (any): The database connection object to be released.
        """
        self.pool.putconn(conn=connection)
        pass

    def log_query(self, cursor: any, query: str, values: dict) -> str:
        """
        Logs a SQL query.

        Args:
            cursor (any): The database cursor object.
            query (str): The SQL query string.
            values (dict): The dictionary of parameter values for the query.

        Returns:
            str: The formatted log message.
        """
        return 'Running SQL: %s' % (cursor.mogrify(query=query, vars=values))
