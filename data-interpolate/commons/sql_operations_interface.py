from abc import ABC, abstractmethod
from commons.tuple_to_dictionary import ResponseMapper
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class SqlOperationsInterface(ABC):
    """
    An abstract base class that defines the interface for performing SQL operations.
    """

    @abstractmethod
    def get_connection(self) -> any:
        """
        Abstract method to get a database connection.

        Returns:
            any: The database connection object.
        """
        pass

    @abstractmethod
    def release_connection(self, connection) -> None:
        """
        Abstract method to release a database connection.

        Args:
            connection: The database connection object.
        """
        pass

    @abstractmethod
    def log_query(self, cursor: any, query: str, values: dict) -> str:
        """
        Abstract method to log a SQL query.

        Args:
            cursor: The database cursor object.
            query (str): The SQL query.
            values (dict): The parameter values for the query.

        Returns:
            str: The formatted log message.
        """
        pass

    def fetch_raw(self, query: str, values: dict) -> list:
        """
        Fetches the results of a SQL query.

        Args:
            query (str): The SQL query.
            values (dict): The parameter values for the query.

        Returns:
            list: The fetched rows.
        """
        db_connection = self.get_connection()
        cursor = db_connection.cursor()
        try:
            logger.info(self.log_query(
                cursor=cursor, query=query, values=values))
            cursor.execute(query, values)
            return cursor.fetchall()
        except Exception as e:
            logger.error(e)
            raise e
        finally:
            cursor.close()
            self.release_connection(connection=db_connection)

    def fetch(self, query: str, values: dict, columns: list) -> list:
        """
        Fetches the results of a SQL query and maps them to a response format.

        Args:
            query (str): The SQL query.
            values (dict): The parameter values for the query.
            columns (list): The list of column names for mapping the response.

        Returns:
            list: The mapped response.
        """
        rows = self.fetch_raw(query=query, values=values)
        return ResponseMapper(keys=columns).map(rows=rows)

    def execute(self, query: str, values: dict) -> None:
        """
        Executes a SQL query.

        Args:
            query (str): The SQL query.
            values (dict): The parameter values for the query.
        """
        db_connection = self.get_connection()
        cursor = db_connection.cursor()
        try:
            logger.info(self.log_query(
                cursor=cursor, query=query, values=values))
            cursor.execute(query, values)
        except Exception as e:
            logger.error(e)
            raise e
        finally:
            cursor.close()
            self.release_connection(connection=db_connection)
