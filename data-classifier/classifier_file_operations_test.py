import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from classifier_file_operations import ClassifierFileOps


class TestClassifierFileOps(unittest.TestCase):

    @patch('classifier_file_operations.S3Ops')
    def setUp(self, MockS3Ops):
        self.mock_s3_ops = MockS3Ops.return_value
        self.bucket_name = 'test-bucket'
        self.file_ops = ClassifierFileOps(bucket_name=self.bucket_name)

    def test_get_success_file_key(self):
        partition_id = 1
        tag_id = 2
        tag_name = 'test/tag'
        expected_key = 'success/1_2/test_tag.csv'
        result = self.file_ops.get_success_file_key(
            partition_id, tag_id, tag_name)
        self.assertEqual(result, expected_key)

    def test_get_batch_info_file_key(self):
        batch_name = 'batch1'
        expected_key = 'batch_info/batch1.csv'
        result = self.file_ops.get_batch_info_file_key(batch_name)
        self.assertEqual(result, expected_key)

    @patch('classifier_file_operations.datetime')
    def test_get_ignore_file_key(self, mock_datetime):
        mock_datetime.datetime.now.return_value.strftime.return_value = '202301011200'
        expected_key = 'ignore/202301011200.csv'
        result = self.file_ops.get_ignore_file_key()
        self.assertEqual(result, expected_key)

    def test_get_existing_batch_file(self):
        batch_name = 'batch1'
        file_key = 'batch_info/batch1.csv'
        self.mock_s3_ops.is_file_exists.return_value = True
        self.mock_s3_ops.get_dataframe.return_value = 'mocked_csv_data'
        pd.read_csv = MagicMock(return_value=pd.DataFrame(
            {'timestamp': [], 'value': []}))

        result = self.file_ops.get_existing_batch_file(batch_name)
        self.mock_s3_ops.is_file_exists.assert_called_once_with(file_key)
        self.mock_s3_ops.get_dataframe.assert_called_once_with(file_key)
        pd.read_csv.assert_called_once_with('mocked_csv_data')
        self.assertIsInstance(result, pd.DataFrame)

    def test_get_existing_success_file(self):
        partition_id = 1
        tag_id = 2
        tag_name = 'test_tag'
        file_key = 'success/1_2/test_tag.csv'
        self.mock_s3_ops.is_file_exists.return_value = True
        self.mock_s3_ops.get_dataframe.return_value = 'mocked_csv_data'
        pd.read_csv = MagicMock(return_value=pd.DataFrame(
            {'timestamp': [], 'value': []}))

        result = self.file_ops.get_existing_success_file(
            partition_id, tag_id, tag_name)
        self.mock_s3_ops.is_file_exists.assert_called_once_with(file_key)
        self.mock_s3_ops.get_dataframe.assert_called_once_with(file_key)
        pd.read_csv.assert_called_once_with(
            'mocked_csv_data', usecols=['timestamp', 'value'])
        self.assertIsInstance(result, pd.DataFrame)

    def test_save_ignore_file(self):
        data = pd.DataFrame({'timestamp': [], 'value': []})
        file_key = 'ignore/202301011200.csv'
        self.file_ops.get_ignore_file_key = MagicMock(return_value=file_key)

        self.file_ops.save_ignore_file(data)
        self.file_ops.get_ignore_file_key.assert_called_once()
        self.mock_s3_ops.publish_dataframe.assert_called_once_with(
            data, file_key)

    def test_save_batch_info_file(self):
        data = pd.DataFrame({'timestamp': [], 'value': []})
        batch_name = 'batch1'
        file_key = 'batch_info/batch1.csv'
        self.file_ops.get_batch_info_file_key = MagicMock(
            return_value=file_key)

        self.file_ops.save_batch_info_file(data, batch_name)
        self.file_ops.get_batch_info_file_key.assert_called_once_with(
            batch_name)
        self.mock_s3_ops.publish_dataframe.assert_called_once_with(
            data, file_key)

    def test_save_success_file(self):
        data = pd.DataFrame({'timestamp': [], 'value': []})
        partition_id = 1
        tag_id = 2
        tag_name = 'test_tag'
        file_key = 'success/1_2/test_tag.csv'
        self.file_ops.get_success_file_key = MagicMock(return_value=file_key)

        self.file_ops.save_success_file(data, partition_id, tag_id, tag_name)
        self.file_ops.get_success_file_key.assert_called_once_with(
            partition_id, tag_id, tag_name)
        self.mock_s3_ops.publish_dataframe.assert_called_once_with(
            data, file_key)


if __name__ == '__main__':
    unittest.main()
