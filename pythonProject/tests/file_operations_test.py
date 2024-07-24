import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from file_operations import FileOps


class TestFileOps(unittest.TestCase):
    def setUp(self):
        self.bucket_name = 'test_bucket'
        self.file_name = 'test_file.csv'
        self.data_frame = pd.DataFrame(
            {'timestamp': [3, 1], 'data': ['C', 'A']})
        self.existing_data_frame = pd.DataFrame(
            {'timestamp': [2], 'data': ['B']})

    @patch('file_operations.S3BucketOperations')
    def test_init(self, mock_s3):
        file_ops = FileOps(self.bucket_name)
        mock_s3.assert_called_with(self.bucket_name)

    @patch('file_operations.S3BucketOperations')
    def test_persist_data_file_not_exists(self, mock_s3):
        mock_s3_instance = mock_s3.return_value
        mock_s3_instance.is_file_exists.return_value = False

        file_ops = FileOps(self.bucket_name)
        file_ops.persist_data(self.data_frame, self.file_name)

        mock_s3_instance.publish_dataframe.assert_called_once_with(
            self.data_frame.sort_values(by=['timestamp']).reset_index(drop=True), self.file_name)

    @patch('file_operations.S3BucketOperations')
    def test_persist_data_file_exists_no_duplicates(self, mock_s3):
        mock_s3_instance = mock_s3.return_value
        mock_s3_instance.is_file_exists.return_value = True
        mock_s3_instance.get_dataframe.return_value = self.existing_data_frame

        file_ops = FileOps(self.bucket_name)
        file_ops.persist_data(self.data_frame, self.file_name)

        expected_df = pd.concat([self.existing_data_frame, self.data_frame]).sort_values(
            by=['timestamp']).drop_duplicates().reset_index(drop=True)
        mock_s3_instance.publish_dataframe.assert_called_once_with(
            expected_df, self.file_name)

    @patch('file_operations.S3BucketOperations')
    def test_persist_data_file_exists_with_duplicates(self, mock_s3):
        mock_s3_instance = mock_s3.return_value
        mock_s3_instance.is_file_exists.return_value = True
        duplicate_data_frame = pd.DataFrame({'timestamp': [2], 'data': ['B']})
        combined_df = pd.concat(
            [self.existing_data_frame, duplicate_data_frame])
        mock_s3_instance.get_dataframe.return_value = combined_df

        file_ops = FileOps(self.bucket_name)
        file_ops.persist_data(duplicate_data_frame, self.file_name)

        expected_df = combined_df.sort_values(
            by=['timestamp']).drop_duplicates().reset_index(drop=True)
        mock_s3_instance.publish_dataframe.assert_called_once_with(
            expected_df, self.file_name)


if __name__ == '__main__':
    unittest.main()
