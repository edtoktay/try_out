import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from interpolate.tag_interpolation import TagInterpolation


class TestTagInterpolation(unittest.TestCase):

    def setUp(self):
        self.csv_content = "timestamp,value,partition_id,tag_id,tag_name\n" \
                           "2023-01-01 00:00:00,10,1,100,'Tag1'\n" \
                           "2023-01-01 00:01:00,-1,1,100,'Tag1'\n" \
                           "2023-01-01 00:02:00,30,1,100,'Tag1'"
        self.bucket_name = "test_bucket"

    @patch('tag_interpolation.FileOps')
    def test_interpolate_regular_data(self, mock_file_ops):
        ti = TagInterpolation(self.csv_content, self.bucket_name)
        ti.interpolate()
        expected_data = pd.DataFrame({
            'partition_id': [1, 1, 1],
            'tag_id': [100, 100, 100],
            'tag_name': ['Tag1', 'Tag1', 'Tag1'],
            'timestamp': pd.to_datetime(["2023-01-01 00:00:00", "2023-01-01 00:01:00", "2023-01-01 00:02:00"]),
            # Assuming linear interpolation between 10 and 30 for the missing value
            'value': [10.0, 20.0, 30.0]
        })
        pd.testing.assert_frame_equal(ti.data.reset_index(
            drop=True), expected_data.reset_index(drop=True))

    @patch('tag_interpolation.FileOps')
    def test_file_persistence(self, mock_file_ops):
        ti = TagInterpolation(self.csv_content, self.bucket_name)
        ti.interpolate()
        mock_file_ops.return_value.persist_data.assert_called_once_with(
            ti.data,
            'Tag1/1/100.csv'
        )


if __name__ == '__main__':
    unittest.main()
