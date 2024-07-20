import unittest
import pytest
from testcontainers.postgres import PostgresContainer
from commons.postgres_operations import PostgresOps

postgres = PostgresContainer("postgres:16-alpine")


@pytest.fixture(scope='module', autouse=True)
def setup(request):
    postgres.start()

    def remove_container():
        postgres.stop()

    request.addfinalizer(remove_container)

    @pytest.fixture(scope='function', autouse=True)
    def check_connection():
        pg_ops = PostgresOps(
            db=postgres.POSTGRES_DB,
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(5432),
            user=postgres.POSTGRES_USER,
            password=postgres.POSTGRES_PASSWORD,
            min_connection=1,
            max_connection=2
        )
        result = pg_ops.fetch(query='SELECT version()', values={})
        unittest.TestCase.assertIsNotNone(result)

