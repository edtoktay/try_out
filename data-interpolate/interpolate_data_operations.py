from commons.postgres_operations import PostgresOps
import interpolate_sqls as queries


class DatabaseOps:
    """
    A class that performs database operations.

    Methods:
    - get_partition(id: int) -> Any: Retrieves a partition with the given ID from the database.
    - get_tag(id: int) -> Any: Retrieves a tag with the given ID from the database.
    - find_batch(timestamp) -> Any: Finds a batch with the given timestamp in the database.
    - finish_batch(id, timestamp) -> Any: Finishes a batch with the given ID and timestamp in the database.
    """

    def __init__(self):
        """
        Initializes an instance of the class.

        Parameters:
        - db (str): The name of the PostgreSQL database.
        - user (str): The username for the PostgreSQL connection.
        - password (str): The password for the PostgreSQL connection.
        - host (str): The host address for the PostgreSQL connection.
        """
        self.ops = PostgresOps(
            db="postgres_db",
            user="postgres",
            password="p.postgres",
            host="172.16.238.10"
        )

    def get_tag(self, id: int):
        """
        Retrieves a tag based on the given ID.

        Parameters:
            id (int): The ID of the tag to retrieve.

        Returns:
            The fetched tag.

        """
        return self.ops.fetch(query=queries.GET_TAG, values={"id": id}, columns=['id', 'tag_name'])

    def get_partition(self, id: int):
        """
        Retrieves a partition based on the given ID.

        Parameters:
            id (int): The ID of the partition to retrieve.

        Returns:
            The fetched partition.

        """
        return self.ops.fetch(query=queries.GET_PARTITION, values={"id": id}, columns=['id', 'start_time'])
