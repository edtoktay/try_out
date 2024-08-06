import datetime
import pandas as pd
from s3_bucket_operations import S3Ops

SUCCESS_FOLDER = 'success'
IGNORE_FOLDER = 'ignore'
BATCH_INFO_FOLDER = 'batch_info'


class ClassifierFileOps:
    """
    A class that provides file operations for the classifier.

    Args:
        bucket_name (str): The name of the S3 bucket.

    Methods:
        get_success_file_key(partition_id: int, tag_id: int, tag_name: str) -> str:
            Returns the key for the success file based on the partition ID, tag ID, and tag name.

        get_batch_info_file_key(batch_name: str) -> str:
            Returns the key for the batch info file based on the batch name.

        get_ignore_file_key() -> str:
            Returns the key for the ignore file.

        get_existing_batch_file(batch_name: str) -> pd.DataFrame:
            Returns the existing batch file as a pandas DataFrame based on the batch name.

        get_existing_success_file(partition_id: int, tag_id: int, tag_name: str) -> pd.DataFrame:
            Returns the existing success file as a pandas DataFrame based on the partition ID, tag ID, and tag name.

        save_ignore_file(data: pd.DataFrame) -> None:
            Saves the ignore file with the provided data.

        save_batch_info_file(data: pd.DataFrame, batch_name: str) -> None:
            Saves the batch info file with the provided data and batch name.

        save_success_file(data: pd.DataFrame, partition_id: int, tag_id: int, tag_name: str) -> None:
            Saves the success file with the provided data, partition ID, tag ID, and tag name.
    """

    def __init__(self, bucket_name: str) -> None:
        """
        Initializes the ClassifierFileOperations object.

        Args:
            bucket_name (str): The name of the S3 bucket.

        Returns:
            None
        """
        self.s3_ops = S3Ops(bucket_name=bucket_name)
        pass

    def get_success_file_key(self, partition_id: int, tag_id: int, tag_name: str) -> str:
        """
        Returns the key for the success file based on the given partition ID, tag ID, and tag name.

        Parameters:
            partition_id (int): The ID of the partition.
            tag_id (int): The ID of the tag.
            tag_name (str): The name of the tag.

        Returns:
            str: The key for the success file.
        """
        tag_name = tag_name.replace('/', '_')
        return f'{SUCCESS_FOLDER}/{partition_id}_{tag_id}/{tag_name}.csv'

    def get_batch_info_file_key(self, batch_name: str) -> str:
        """
        Returns the file key for the batch information file.

        Parameters:
            batch_name (str): The name of the batch.

        Returns:
            str: The file key for the batch information file.
        """
        return f'{BATCH_INFO_FOLDER}/{batch_name}.csv'

    def get_ignore_file_key(self) -> str:
        """
        Returns the key for the ignore file based on the current processing time.

        Returns:
            str: The key for the ignore file.
        """
        processing_time = datetime.datetime.now().strftime("%Y%m%d%H%M")
        return f'{IGNORE_FOLDER}/{processing_time}.csv'

    def get_existing_batch_file(self, batch_name: str) -> pd.DataFrame:
        """
        Retrieves the existing batch file data for the given batch name.

        Parameters:
            batch_name (str): The name of the batch.

        Returns:
            pd.DataFrame: The existing batch file data as a pandas DataFrame, 
                          containing the 'timestamp' and 'value' columns.
                          Returns None if the file does not exist.
        """
        file_key = self.get_batch_info_file_key(batch_name)
        if self.s3_ops.is_file_exists(file_key):
            existing_data = pd.read_csv(self.s3_ops.get_dataframe(file_key))
            return existing_data
        return None

    def get_existing_success_file(self, partition_id: int, tag_id: int, tag_name: str) -> pd.DataFrame:
        """
        Retrieves the existing success file for the given partition ID, tag ID, and tag name.

        Parameters:
            partition_id (int): The ID of the partition.
            tag_id (int): The ID of the tag.
            tag_name (str): The name of the tag.

        Returns:
            pd.DataFrame: The existing data from the success file, containing the 'timestamp' and 'value' columns.
            None: If the success file does not exist.
        """
        file_key = self.get_success_file_key(partition_id, tag_id, tag_name)
        if self.s3_ops.is_file_exists(file_key):
            existing_data = pd.read_csv(self.s3_ops.get_dataframe(
                file_key), usecols=['timestamp', 'value'])
            return existing_data
        return None

    def save_ignore_file(self, data: pd.DataFrame) -> None:
        """
        Saves the given DataFrame as an ignore file.

        Parameters:
            data (pd.DataFrame): The DataFrame to be saved as an ignore file.

        Returns:
            None
        """
        file_key = self.get_ignore_file_key()
        self.s3_ops.publish_dataframe(data, file_key)
        pass

    def save_batch_info_file(self, data: pd.DataFrame, batch_name: str) -> None:
        """
        Saves the given DataFrame as a batch info file.

        Args:
            data (pd.DataFrame): The DataFrame to be saved.
            batch_name (str): The name of the batch.

        Returns:
            None
        """
        file_key = self.get_batch_info_file_key(batch_name)
        self.s3_ops.publish_dataframe(data, file_key)
        pass

    def save_success_file(self, data: pd.DataFrame, partition_id: int, tag_id: int, tag_name: str) -> None:
        """
        Save the success file to S3.

        Args:
            data (pd.DataFrame): The data to be saved.
            partition_id (int): The partition ID.
            tag_id (int): The tag ID.
            tag_name (str): The tag name.

        Returns:
            None
        """
        file_key = self.get_success_file_key(partition_id, tag_id, tag_name)
        self.s3_ops.publish_dataframe(data, file_key)
        pass

    def is_batch_file_exists(self, batch_name: str) -> bool:
        """
        Checks if the batch file exists in the S3 bucket.

        Args:
            batch_name (str): The name of the batch.

        Returns:
            bool: True if the batch file exists, False otherwise.
        """
        return self.s3_ops.is_file_exists(self.get_batch_info_file_key(batch_name))

    def is_success_file_exists(self, partition_id: int, tag_id: int, tag_name: str) -> bool:
        """
        Checks if the success file exists in the S3 bucket.

        Args:
            partition_id (int): The ID of the partition.
            tag_id (int): The ID of the tag.
            tag_name (str): The name of the tag.

        Returns:
            bool: True if the success file exists, False otherwise.
        """
        return self.s3_ops.is_file_exists(self.get_success_file_key(partition_id, tag_id, tag_name))
