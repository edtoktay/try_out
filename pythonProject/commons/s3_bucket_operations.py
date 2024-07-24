from io import StringIO
import boto3
import botocore.exceptions
import pandas as pd
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class S3Ops:
    """
    A class that provides operations for interacting with an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.

    Attributes:
        bucket_name (str): The name of the S3 bucket.
        s3_resource (boto3.resources.factory.s3.ServiceResource): The S3 resource object.
        s3_client (boto3.resources.factory.s3.Client): The S3 client object.

    """

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3_resource = boto3.resource('s3')
        self.s3_client = boto3.client('s3')
        pass

    def publish_dataframe(self, data_frame, file_name):
        """
        Publishes a pandas DataFrame to the S3 bucket.

        Args:
            data_frame (pandas.DataFrame): The DataFrame to be published.
            file_name (str): The name of the file to be created in the S3 bucket.

        """
        try:
            csv_buffer = StringIO()
            data_frame.to_csv(csv_buffer)
            self.s3_resource.Object(self.bucket_name, file_name).put(
                Body=csv_buffer.getvalue())
        except Exception as e:
            logger.error(e)
        pass

    def get_dataframe(self, file_key):
        """
        Retrieves a pandas DataFrame from the S3 bucket.

        Args:
            file_key (str): The key of the file in the S3 bucket.

        Returns:
            pandas.DataFrame: The retrieved DataFrame.

        Raises:
            Exception: If an error occurs while retrieving the DataFrame.

        """
        try:
            csv_object = self.s3_client.get_object(
                Bucket=self.bucket_name, Key=file_key)
            body = csv_object['Body']
            csv_string = body.read().decode('utf-8')
            return pd.read_csv(StringIO(csv_string))
        except Exception as e:
            logger.error(e)
            raise e

    def is_file_exists(self, file_key):
        """
        Checks if a file exists in the S3 bucket.

        Args:
            file_key (str): The key of the file in the S3 bucket.

        Returns:
            bool: True if the file exists, False otherwise.

        Raises:
            botocore.exceptions.ClientError: If an error occurs while checking the file existence.

        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=file_key)
            logger.info(f"Key: '{file_key}' exists")
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.info(f"Key: '{file_key}' not exists")
                return False
            else:
                logger.error(e)
                raise e
