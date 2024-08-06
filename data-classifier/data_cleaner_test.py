import unittest
import pandas as pd
from data_cleaner import DataCleaner


class TestDataCleaner(unittest.TestCase):

    def setUp(self):
        self.data = pd.DataFrame({
            'timestamp': [
                '2023-01-01 00:00:00', '2023-01-01 00:01:00', '2023-01-01 00:01:00',
                '2023-01-01 00:02:00', '2023-01-01 00:03:00', '2023-01-01 00:04:00', '2023-01-01 00:05:00'
            ],
            'value': [10, 20, 30, 30, 40, 40, 39]
        })
        self.data['timestamp'] = pd.to_datetime(self.data['timestamp'])

    def test_initialization(self):
        cleaner = DataCleaner(self.data)
        self.assertIsInstance(cleaner.data, pd.DataFrame)
        self.assertEqual(len(cleaner.data), 7)

    def test_clear_duplicate_timestamps(self):
        cleaner = DataCleaner(self.data)
        cleaned_data = cleaner._DataCleaner__clear_duplicate_timestamps(
            cleaner.data)
        self.assertEqual(len(cleaned_data), 6)
        self.assertFalse(cleaned_data['timestamp'].duplicated().any())

    def test_clear_duplicated_values(self):
        cleaner = DataCleaner(self.data)
        cleaned_data = cleaner._DataCleaner__clear_duplicated_values(
            cleaner.data)
        self.assertEqual(len(cleaned_data), 5)
        self.assertFalse(cleaned_data['value'].duplicated().any())

    def test_clean_data(self):
        cleaner = DataCleaner(self.data)
        cleaned_data = cleaner.clean_data(check_sort_order=True)
        self.assertEqual(len(cleaned_data), 3)
        self.assertFalse(cleaned_data['timestamp'].duplicated().any())
        self.assertFalse(cleaned_data['value'].duplicated().any())


if __name__ == "__main__":
    unittest.main()
