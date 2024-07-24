import snowflake.connector
from commons.sql_operations_interface import SqlOperationsInterface


class SnowflakeOps(SqlOperationsInterface):
    """
    A class that provides operations for interacting with Snowflake database.

    Args:
        db (str): The name of the Snowflake database.
        user (str): The username for connecting to Snowflake.
        password (str): The password for connecting to Snowflake.
        account (str): The Snowflake account URL.
        warehouse (str): The name of the Snowflake warehouse.
        schema (str): The name of the Snowflake schema.
        role (str): The name of the Snowflake role.

    Methods:
        get_connection: Establishes a connection to the Snowflake database.
        release_connection: Releases the connection to the Snowflake database.
        log_query: Logs the details of a query being executed.

    """

    def __init__(self, db, user, password, account, warehouse, schema, role):
        self.db = db
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.schema = schema
        self.role = role

    def get_connection(self) -> any:
        """
        Establishes a connection to the Snowflake database.

        Returns:
            any: The Snowflake connection object.

        """
        return snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.db,
            schema=self.schema,
            role=self.role
        )

    def release_connection(self, connection) -> None:
        """
        Releases the connection to the Snowflake database.

        Args:
            connection (any): The Snowflake connection object.

        Returns:
            None

        """
        connection.close()

    def log_query(self, cursor: any, query: str, values: dict) -> str:
        """
        Logs the details of a query being executed.

        Args:
            cursor (any): The Snowflake cursor object.
            query (str): The SQL query being executed.
            values (dict): The parameter values used in the query.

        Returns:
            str: The log message.

        """
        return 'Running Query {} for {}'.format(query, values)
