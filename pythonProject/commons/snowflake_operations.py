import snowflake.connector
from commons.sql_operations_interface import SqlOperationsInterface


class SnowflakeOps(SqlOperationsInterface):
    def __init__(self, db, user, password, account, warehouse, schema, role):
        self.db = db
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.schema = schema
        self.role = role
        pass

    def get_connection(self) -> any:
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
        connection.close()
        pass

    def log_query(self, cursor: any, query: str, values: dict) -> str:
        return 'Running Query {} for {}'.format(query, values)
