import unittest
from unittest.mock import patch, MagicMock

import mock
import pandas as pd
from commons.s3_bucket_operations import S3Ops
import botocore.exceptions


class TestS3Ops(unittest.TestCase):

    @patch('commons.s3_bucket_operations.boto3.resource')
    @patch('commons.s3_bucket_operations.boto3.client')
    def setUp(self, mock_boto3_client, mock_boto3_resource):
        self.mock_s3_resource = mock_boto3_resource.return_value
        self.mock_s3_client = mock_boto3_client.return_value
        self.bucket_name = 'test-bucket'
        self.s3_ops = S3Ops(self.bucket_name)

    def test_init(self):
        self.assertEqual(self.s3_ops.bucket_name, self.bucket_name)
        self.assertEqual(self.s3_ops.s3_resource, self.mock_s3_resource)
        self.assertEqual(self.s3_ops.s3_client, self.mock_s3_client)

    @patch('commons.s3_bucket_operations.StringIO')
    def test_publish_dataframe(self, mock_stringio):
        mock_df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        mock_buffer = mock_stringio.return_value
        mock_object = self.mock_s3_resource.Object.return_value

        self.s3_ops.publish_dataframe(mock_df, 'test.csv')

        mock_object.put.assert_called_once_with(Body=mock_buffer.getvalue())

    def test_get_dataframe(self):
        mock_body = MagicMock()
        mock_body.read.return_value.decode.return_value = 'col1,col2\n1,3\n2,4\n'
        self.mock_s3_client.get_object.return_value = {'Body': mock_body}

        df = self.s3_ops.get_dataframe('test.csv')

        self.mock_s3_client.get_object.assert_called_once_with(Bucket=self.bucket_name, Key='test.csv')
        pd.testing.assert_frame_equal(df, pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]}))

    def test_is_file_exists(self):
        self.mock_s3_client.head_object.return_value = {}

        result = self.s3_ops.is_file_exists('test.csv')

        self.mock_s3_client.head_object.assert_called_once_with(Bucket=self.bucket_name, Key='test.csv')
        self.assertTrue(result)

    def test_is_file_not_exists(self):
        self.mock_s3_client.head_object.side_effect = botocore.exceptions.ClientError(
            {'Error': {'Code': '404'}}, 'head_object')

        result = self.s3_ops.is_file_exists('test.csv')

        self.mock_s3_client.head_object.assert_called_once_with(Bucket=self.bucket_name, Key='test.csv')
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
