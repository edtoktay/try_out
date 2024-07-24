import pandas as pd
from s3_bucket_operations import S3Ops


class FileOps:
    """
    A class that provides file operations for persisting data.

    Args:
        bucket_name (str): The name of the S3 bucket to store the files.

    Attributes:
        s3 (S3BucketOperations): An instance of the S3BucketOperations class.

    Methods:
        persist_data: Persists the given data frame to a file in the S3 bucket.

    """

    def __init__(self, bucket_name) -> None:
        self.s3 = S3Ops(bucket_name)

    def persist_data(self, data_frame, file_name):
        """
        Persists the given data frame to a file in the S3 bucket.

        If the file already exists in the bucket, the data frame is appended to the existing data.
        The data frame is then sorted by timestamp, duplicates are removed, and the index is reset.
        Finally, the updated data frame is published to the S3 bucket.

        Args:
            data_frame (pandas.DataFrame): The data frame to be persisted.
            file_name (str): The name of the file to be created or updated.

        Returns:
            None

        """
        if self.s3.is_file_exists(file_name):
            recorded_data = self.s3.get_dataframe(file_name)
            data_frame = pd.concat([recorded_data, data_frame])
        data_frame = data_frame.sort_values(by=['timestamp'])
        data_frame = data_frame.drop_duplicates()
        data_frame = data_frame.reset_index(drop=True)
        self.s3.publish_dataframe(data_frame, file_name)
