import unittest
import pandas as pd
from unittest.mock import patch, MagicMock
from classifier.tag_classifier import DataClassifier


class TestDataClassifier(unittest.TestCase):

    def setUp(self):
        # Mock CSV content
        self.csv_content = "tag,value\nA,1\nA,2\nA,3\nB,1\nB,2\nB,1"

        # Mock DataCleaner
        self.mock_data_cleaner = MagicMock()
        self.mock_data_cleaner.get_tag_data.return_value = {
            'A': pd.DataFrame({'value': [1, 2, 3]}),
            'B': pd.DataFrame({'value': [1, 2, 1]})
        }

        # Mock DatabaseOps
        self.mock_db_ops = MagicMock()
        self.mock_db_ops.get_partitions.return_value = ['partition1', 'partition2']
        self.mock_db_ops.get_tags.return_value = ['A', 'B']

        # Patch the imports in DataClassifier
        patcher1 = patch('classifier.tag_classifier.DataCleaner', return_value=self.mock_data_cleaner)
        patcher2 = patch('classifier.tag_classifier.DatabaseOps', return_value=self.mock_db_ops)
        self.addCleanup(patcher1.stop)
        self.addCleanup(patcher2.stop)
        self.mock_data_cleaner_class = patcher1.start()
        self.mock_db_ops_class = patcher2.start()

        self.data_classifier = DataClassifier(csv_content=self.csv_content)

    def test_initialization(self):
        self.mock_data_cleaner_class.assert_called_once_with(csv_content=self.csv_content)
        self.mock_db_ops_class.assert_called_once()
        self.assertEqual(self.data_classifier.partitions, ['partition1', 'partition2'])
        self.assertEqual(self.data_classifier.tags, ['A', 'B'])
        self.assertIn('A', self.data_classifier.tag_partitioned_data)
        self.assertIn('B', self.data_classifier.tag_partitioned_data)

    def test_check_dataframe_incremental(self):
        df_incremental = pd.DataFrame({'value': [1, 2, 3]})
        df_non_incremental = pd.DataFrame({'value': [1, 2, 1]})

        self.data_classifier._DataClassifier__check_dataframe_incremental(df_incremental)
        self.assertTrue(df_incremental.equals(pd.DataFrame({'value': [1, 2, 3]})))

        self.data_classifier._DataClassifier__check_dataframe_incremental(df_non_incremental)


if __name__ == '__main__':
    unittest.main()
