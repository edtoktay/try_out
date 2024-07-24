import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from classifier.tag_classifier import DataClassifier


class TestDataClassifier(unittest.TestCase):

    @patch('classifier.tag_classifier.DataCleaner')
    @patch('classifier.tag_classifier.DatabaseOps')
    def setUp(self, MockDatabaseOps, MockDataCleaner):
        # Mocking DataCleaner
        self.mock_data_cleaner = MockDataCleaner.return_value
        self.mock_data_cleaner.get_tag_data.return_value = {
            'control_tag': pd.DataFrame({'timestamp': [1, 2, 3], 'value': [0, 1, 2]}),
            'batch_tag': pd.DataFrame({'timestamp': [1, 2, 3], 'value': [10, 20, 30]}),
            'lot_size_tag': pd.DataFrame({'timestamp': [1, 2, 3], 'value': [100, 200, 300]})
        }

        # Mocking DatabaseOps
        self.mock_db_ops = MockDatabaseOps.return_value
        self.mock_db_ops.get_partitions.return_value = [
            {'id': 1, 'start_time': 0, 'end_time': None}]
        self.mock_db_ops.get_tags.return_value = [
            {'tag_name': 'control_tag', 'is_control_tag': True, 'is_batch_name': False, 'is_lot_size': False},
            {'tag_name': 'batch_tag', 'is_control_tag': False, 'is_batch_name': True, 'is_lot_size': False},
            {'tag_name': 'lot_size_tag', 'is_control_tag': False, 'is_batch_name': False, 'is_lot_size': True}
        ]

        # Initialize DataClassifier
        self.data_classifier = DataClassifier(csv_content="dummy_content")

    def test_initialization(self):
        self.assertEqual(len(self.data_classifier.partitions), 1)
        self.assertEqual(len(self.data_classifier.tags), 3)

    def test_find_tag(self):
        tag = self.data_classifier._DataClassifier__find_tag('control_tag')
        self.assertEqual(tag['tag_name'], 'control_tag')

    def test_find_control_tag(self):
        tag = self.data_classifier._DataClassifier__find_control_tag()
        self.assertTrue(tag['is_control_tag'])

    def test_find_batch_tag(self):
        tag = self.data_classifier._DataClassifier__find_batch_tag()
        self.assertTrue(tag['is_batch_name'])

    def test_find_lot_size_tag(self):
        tag = self.data_classifier._DataClassifier__find_lot_size_tag()
        self.assertTrue(tag['is_lot_size'])

    def test_find_active_partition(self):
        partition = self.data_classifier._DataClassifier__find_active_partition()
        self.assertIsNone(partition['end_time'])

    def test_check_dataframe_incremental(self):
        df = pd.DataFrame({'value': [1, 2, 3, 2, 5]})
        result_df = self.data_classifier._DataClassifier__check_dataframe_incremental(
            df)
        self.assertTrue(result_df['value'].is_monotonic_increasing)

    def test_handle_control_tag(self):
        control_tag_response, ignore_tag = self.data_classifier._DataClassifier__handle_control_tag()
        self.assertEqual(len(control_tag_response), 2)
        self.assertEqual(len(ignore_tag), 1)

    def test_classify_data(self):
        control_tag_data, ignore_tag = self.data_classifier.classify_data()
        self.assertEqual(len(control_tag_data), 2)
        self.assertEqual(len(ignore_tag), 1)


if __name__ == '__main__':
    unittest.main()
