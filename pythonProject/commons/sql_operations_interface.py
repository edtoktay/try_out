from abc import ABC, abstractmethod
from tuple_to_dictionary import ResponseMapper
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class SqlOperationsInterface(ABC):

    @abstractmethod
    def get_connection(self) -> any:
        pass

    @abstractmethod
    def release_connection(self, connection) -> None:
        pass

    @abstractmethod
    def log_query(self, cursor: any, query: str, values: dict) -> str:
        pass

    def fetch(self, query: str, values: dict) -> list:
        db_connection = self.get_connection()
        cursor = db_connection.cursor()
        try:
            logger.info(self.log_query(cursor=cursor, query=query, values=values))
            cursor.execute()
            return cursor.fetchall()
        except Exception as e:
            logger.error(e)
            raise e
        finally:
            cursor.close()
            self.release_connection(connection=db_connection)
            pass

    def fetch(self, query: str, values: dict, columns: list) -> list:
        rows = self.fetch(query=query, values=values)
        return ResponseMapper(keys=columns).map(rows=rows)

    def execute(self, query: str, values: dict) -> None:
        db_connection = self.get_connection()
        cursor = db_connection.cursor()
        try:
            logger.info(self.log_query(cursor=cursor, query=query, values=values))
            cursor.execute()
        except Exception as e:
            logger.error(e)
            raise e
        finally:
            cursor.close()
            self.release_connection(connection=db_connection)
