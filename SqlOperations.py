import psycopg2.pool
import logging

class SqlOperations:

    def create_pool(self):
        return psycopg2.pool.SimpleConnectionPool(
            minconn=self.min,
            maxconn=self.max,
            database=self.db,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port 
        )

    def get_connection(self):
        return self.pool.getconn()
    
    def release_connection(self,conn):
        self.pool.putconn(conn)
        pass

    def query_with_return(self, sql, values):
        db_connection = self.get_connection()
        db_connection.autocommit = True
        try:
            cursor = db_connection.cursor()
            logging.info('Running SQL: %s' % (cursor.mogrify(sql, values)))
            cursor.execute(sql, values)
            return cursor.fetchall()
        except Exception as e:
            logging.error(e)
            raise e
        finally:
            self.release_connection(db_connection)
        pass

    def query_without_return(self, sql, values):
        db_connection = self.get_connection()
        db_connection.autocommit = True
        try:
            cursor = db_connection.cursor()
            logging.info('Running SQL: %s' % (cursor.mogrify(sql, values)))
            cursor.execute(sql, values)
        except Exception as e:
            logging.error(e)
            raise e
        finally:
            self.release_connection(db_connection)
        pass

    def __init__(self, db, user, password, host, port=5432, min=1, max=3):
        self.db = db
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.min = min
        self.max = max
        self.pool = self.create_pool()
