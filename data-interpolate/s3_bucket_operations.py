from io import StringIO
import pandas as pd
import logging
import shutil
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class S3Ops:
    def __init__(self, bucket_name):
        self.bucket_name = '/home/deniz.toktay/Development/Workspaces/try_out/data-classifier/data/out/'
        pass

    def publish_dataframe(self, data_frame, file_name) -> None:
        """
        Publishes a pandas DataFrame to the S3 bucket.

        Args:
            data_frame (pandas.DataFrame): The DataFrame to be published.
            file_name (str): The name of the file to be created in the S3 bucket.

        """
        try:
            dir_name = os.path.dirname(self.bucket_name + file_name)
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
            csv_buffer = StringIO()
            data_frame.to_csv(csv_buffer, index=False)
            with open(self.bucket_name + file_name, 'w') as f:
                csv_buffer.seek(0)
                shutil.copyfileobj(csv_buffer, f)
        except Exception as e:
            logger.error(e)
        pass

    def get_dataframe(self, file_key) -> StringIO:
        """
        Retrieves a dataframe from an S3 bucket.

        Parameters:
        - file_key (str): The key of the file in the S3 bucket.

        Returns:
        - StringIO: A StringIO object containing the contents of the CSV file.

        Raises:
        - Exception: If there is an error retrieving the file from the S3 bucket.
        """

        try:
            file = open(self.bucket_name + file_key, 'r').read(1024)
            return StringIO(file)
        except Exception as e:
            logger.error(e)
            raise e
        pass

    def is_file_exists(self, file_key) -> bool:
        """
        Checks if a file exists in the S3 bucket.

        Args:
            file_key (str): The key of the file in the S3 bucket.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        try:
            return os.path.isfile(self.bucket_name + file_key)
        except Exception as e:
            logger.error(e)
            return False
        pass
