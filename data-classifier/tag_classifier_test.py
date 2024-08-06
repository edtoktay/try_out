import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from tag_data_classifier import DataClassifier


class TestDataClassifier(unittest.TestCase):

    @patch('tag_data_classifier.DatabaseOps')
    @patch('tag_data_classifier.ClassifierFileOps')
    def setUp(self, MockClassifierFileOps, MockDatabaseOps):
        # Mock the DatabaseOps and ClassifierFileOps
        self.mock_db_ops = MockDatabaseOps.return_value
        self.mock_file_ops = MockClassifierFileOps.return_value

        # Mock the tags returned by the database
        self.mock_db_ops.get_tags.return_value = [
            {'tag_name': 'batch_tag', 'is_batch_name': True},
            {'tag_name': 'lot_size_tag', 'is_lot_size': True},
            {'tag_name': 'control_tag', 'is_control_tag': True}
        ]

        # Create a sample DataFrame
        data = {
            'tagname': ['batch_tag', 'lot_size_tag', 'control_tag'],
            'timestamp': ['2023-01-01 00:00:00', '2023-01-01 00:01:00', '2023-01-01 00:02:00'],
            'value': ['batch1', '100', 'control_value']
        }
        self.data_frame = pd.DataFrame(data)
        self.data_frame['timestamp'] = pd.to_datetime(
            self.data_frame['timestamp'])

        # Initialize the DataClassifier
        self.classifier = DataClassifier(self.data_frame, 'test_bucket')

    @patch('tag_data_classifier.DataCleaner')
    def test_classify_data(self, MockDataCleaner):
        # Mock the DataCleaner
        MockDataCleaner.return_value.clean_data.return_value = self.data_frame

        # Mock the database operations
        self.mock_db_ops.get_latest_batch.return_value = None
        self.mock_db_ops.insert_batch.return_value = None
        self.mock_db_ops.update_batch_lot_size.return_value = None

        # Run the classify_data method
        self.classifier.classify_data()

        # Check that the batch information was processed and saved
        self.mock_file_ops.save_batch_info_file.assert_called()
        self.mock_file_ops.save_ignore_file.assert_called()

        # Verify that the ignored values DataFrame is saved correctly
        ignored_values = self.mock_file_ops.save_ignore_file.call_args[1]['data']
        self.assertTrue('tag_name' in ignored_values.columns)
        self.assertTrue('timestamp' in ignored_values.columns)
        self.assertTrue('value' in ignored_values.columns)
        self.assertTrue('status' in ignored_values.columns)
        self.assertTrue('reason' in ignored_values.columns)


if __name__ == '__main__':
    unittest.main()
