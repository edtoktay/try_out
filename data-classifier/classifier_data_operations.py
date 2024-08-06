import logging
from commons.postgres_operations import PostgresOps
import classifier_sqls as queries

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DatabaseOps:
    """
    A class that provides operations for interacting with a database.
    """

    def __init__(self):
        self.ops = PostgresOps(
            db="postgres_db",
            user="postgres",
            password="p.postgres",
            host="172.16.238.10"
        )

    def insert_batch(self, batch_name: str, start_time) -> None:
        """
        Inserts a new batch into the database.

        Args:
            batch_name (str): The name of the batch to be inserted.

        Returns:
            None
        """
        self.ops.execute(
            query=queries.INSERT_BATCH,
            values={'batchName': batch_name, 'startTime': start_time}
        )
        pass

    def update_batch_lot_size(self, batch_name: str, lot_size: int) -> None:
        """
        Updates the lot size of a batch in the database.

        Args:
            batch_name (str): The name of the batch to be updated.
            lot_size (int): The new lot size of the batch.

        Returns:
            None
        """
        self.ops.execute(
            query=queries.UPDATE_BATCH_LOT_SIZE,
            values={'batchName': batch_name, 'lotSize': lot_size}
        )
        pass

    def insert_tag(self, tag_name: str) -> list:
        """
        Inserts a new tag into the database.

        Args:
            tag_name (str): The name of the tag to be inserted.

        Returns:
            list: A list of dictionaries containing the details of the tags.
        """
        self.ops.execute(
            query=queries.INSERT_TAG,
            values={'tag_name': tag_name}
        )
        return self.ops.fetch(
            query=queries.GET_TAGS,
            values={},
            columns=['id', 'tag_name', 'is_batch_name',
                     'is_lot_size', 'is_control_tag']
        )

    def insert_partition(self, start_time) -> None:
        """
        Inserts a new partition into the database.

        Args:
            start_time: The start time of the partition to insert.

        Returns:
            A list of dictionaries representing all partitions after the insertion, each containing the following keys:
            - id: The partition ID.
            - start_time: The start time of the partition.
            - end_time: The end time of the partition.
        """
        try:
            self.ops.execute(
                query=queries.INSERT_PARTITION,
                values={'start_time': start_time}
            )
        except Exception as e:
            logger.error(f"Error inserting partition: {e}")
        pass

    def end_partition(self, partition_id: int, end_time) -> None:
        """
        Ends a partition in the database.

        Args:
            partition_id: The ID of the partition to end.
            end_time: The end time of the partition.

        Returns:
            A list of dictionaries representing all partitions after the update, each containing the following keys:
            - id: The partition ID.
            - start_time: The start time of the partition.
            - end_time: The end time of the partition.
        """
        try:
            self.ops.execute(
                query=queries.END_PARTITION,
                values={'id': partition_id, 'end_time': end_time}
            )
        except Exception as e:
            logger.error(f"Error ending partition: {e}")
        pass

    def get_tags(self) -> list:
        """
        Retrieves all tags from the database.

        Returns:
            list: A list of dictionaries containing the details of the tags.
        """
        return self.ops.fetch(
            query=queries.GET_TAGS,
            values={},
            columns=['id', 'tag_name', 'is_batch_name',
                     'is_lot_size', 'is_control_tag']
        )

    def get_latest_batch(self) -> dict:
        """
        Retrieves the latest batch information from the data classifier.

        Returns:
            dict: A dictionary containing the latest batch information with the following keys:
                - 'batch_id': The ID of the batch.
                - 'start_time': The start time of the batch.
                - 'end_time': The end time of the batch.
                - 'lot_size': The size of the batch.
        """
        return self.ops.fetch(
            query=queries.LATEST_BATCH,
            values={},
            columns=['batch_name', 'start_time', 'end_time', 'lot_size']
        )

    def get_latest_partition(self) -> dict:
        """
        Retrieves the latest partition information from the data classifier.

        Returns:
            dict: A dictionary containing the latest partition information with the following keys:
                - 'id': The ID of the partition.
                - 'start_time': The start time of the partition.
                - 'end_time': The end time of the partition.
        """
        return self.ops.fetch(
            query=queries.LATEST_PARTITION,
            values={},
            columns=['id', 'start_time', 'end_time', 'partition_sum']
        )

    def find_containing_partition(self, timestamp) -> dict:
        """
        Finds the partition containing the given timestamp.

        Args:
            timestamp: The timestamp to search for.

        Returns:
            dict: A dictionary containing the partition information with the following keys:
                - 'id': The ID of the partition.
                - 'start_time': The start time of the partition.
                - 'end_time': The end time of the partition.
        """
        return self.ops.fetch(
            query=queries.FIND_CONTAINING_PARTITION,
            values={'timestamp': timestamp},
            columns=['id', 'start_time', 'end_time']
        )
