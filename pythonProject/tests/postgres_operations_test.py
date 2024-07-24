import unittest
from commons.postgres_operations import PostgresOps


class TestPostgresOps(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = "postgres_db"
        cls.user = "postgres"
        cls.password = "p.postgres"
        cls.host = "172.16.238.10"
        cls.port = 5432
        cls.postgres_ops = PostgresOps(
            db=cls.db,
            user=cls.user,
            password=cls.password,
            host=cls.host,
            port=cls.port
        )

    def test_create_pool(self):
        self.assertIsNotNone(self.postgres_ops.pool)

    def test_get_connection(self):
        conn = self.postgres_ops.get_connection()
        self.assertIsNotNone(conn)
        self.postgres_ops.release_connection(conn)

    def test_release_connection(self):
        conn = self.postgres_ops.get_connection()
        self.postgres_ops.release_connection(conn)
        self.assertFalse(conn.closed)

    def test_log_query(self):
        conn = self.postgres_ops.get_connection()
        cursor = conn.cursor()
        query = "SELECT %(val)s"
        values = {'val': 1}
        log_message = self.postgres_ops.log_query(cursor, query, values)
        self.assertIn("Running SQL:", log_message)
        cursor.close()
        self.postgres_ops.release_connection(conn)


if __name__ == '__main__':
    unittest.main()
