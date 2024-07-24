from commons.postgres_operations import PostgresOps
import classifier.classifier_sqls as queries


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

    def get_partitions(self):
        """
        Retrieves the first three partitions from the database.

        Returns:
            A list of dictionaries representing the partitions, each containing the following keys:
            - id: The partition ID.
            - start_time: The start time of the partition.
            - end_time: The end time of the partition.
        """
        return self.ops.fetch(
            query=queries.FIRST_THREE_PARTITIONS,
            values={},
            columns=['id', 'start_time', 'end_time']
        )

    def get_tags(self):
        """
        Retrieves all tags from the database.

        Returns:
            A list of dictionaries representing the tags, each containing the following keys:
            - id: The tag ID.
            - tag_name: The name of the tag.
            - is_batch_name: A flag indicating if the tag is a batch name.
            - is_lot_size: A flag indicating if the tag is a lot size.
            - is_control_tag: A flag indicating if the tag is a control tag.
        """
        return self.ops.fetch(
            query=queries.GET_TAGS,
            values={},
            columns=[
                'id',
                'tag_name',
                'is_batch_name',
                'is_lot_size',
                'is_control_tag'
            ]
        )

    def insert_tag(self, tag_name):
        """
        Inserts a new tag into the database.

        Args:
            tag_name: The name of the tag to insert.

        Returns:
            A list of dictionaries representing all tags after the insertion, each containing the following keys:
            - id: The tag ID.
            - tag_name: The name of the tag.
            - is_batch_name: A flag indicating if the tag is a batch name.
            - is_lot_size: A flag indicating if the tag is a lot size.
            - is_control_tag: A flag indicating if the tag is a control tag.
        """
        self.ops.execute(
            query=queries.INSERT_TAG,
            values={'tag_name': tag_name}
        )
        return self.get_tags()

    def insert_partition(self, start_time):
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
        self.ops.execute(
            query=queries.INSERT_PARTITION,
            values={'start_time': start_time}
        )
        return self.get_partitions()

    def end_partition(self, partition_id, end_time):
        self.ops.execute(
            query=queries.END_PARTITION,
            values={
                'id': partition_id,
                'end_time': end_time
            }
        )
        return self.get_partitions()
