import unittest
from unittest.mock import patch, MagicMock
from classifier.classifier_data_operations import DatabaseOps


class TestDatabaseOps(unittest.TestCase):

    @patch('classifier.classifier_data_operations.ops')
    @patch('classifier.classifier_data_operations.queries')
    def setUp(self, mock_queries, mock_ops):
        self.db_ops = DatabaseOps()
        self.mock_ops = mock_ops
        self.mock_queries = mock_queries

    def test_get_partitions(self):
        expected_result = [
            {'id': 1, 'start_time': '2023-01-01 00:00:00', 'end_time': '2023-01-01 01:00:00'},
            {'id': 2, 'start_time': '2023-01-01 01:00:00', 'end_time': '2023-01-01 02:00:00'},
            {'id': 3, 'start_time': '2023-01-01 02:00:00', 'end_time': '2023-01-01 03:00:00'}
        ]
        self.mock_ops.fetch.return_value = expected_result
        result = self.db_ops.get_partitions()
        self.assertEqual(result, expected_result)
        self.mock_ops.fetch.assert_called_once_with(
            query=self.mock_queries.FIRST_THREE_PARTITIONS,
            values={},
            columns=['id', 'start_time', 'end_time']
        )

    def test_get_tags(self):
        expected_result = [
            {'id': 1, 'tag_name': 'tag1', 'is_batch_name': True, 'is_lot_size': False, 'is_control_tag': True},
            {'id': 2, 'tag_name': 'tag2', 'is_batch_name': False, 'is_lot_size': True, 'is_control_tag': False}
        ]
        self.mock_ops.fetch.return_value = expected_result
        result = self.db_ops.get_tags()
        self.assertEqual(result, expected_result)
        self.mock_ops.fetch.assert_called_once_with(
            query=self.mock_queries.GET_TAGS,
            values={},
            columns=['id', 'tag_name', 'is_batch_name', 'is_lot_size', 'is_control_tag']
        )

    def test_insert_tag(self):
        tag_name = 'new_tag'
        expected_result = [
            {'id': 1, 'tag_name': 'tag1', 'is_batch_name': True, 'is_lot_size': False, 'is_control_tag': True},
            {'id': 2, 'tag_name': 'tag2', 'is_batch_name': False, 'is_lot_size': True, 'is_control_tag': False},
            {'id': 3, 'tag_name': 'new_tag', 'is_batch_name': False, 'is_lot_size': False, 'is_control_tag': False}
        ]
        self.mock_ops.fetch.return_value = expected_result
        result = self.db_ops.insert_tag(tag_name)
        self.assertEqual(result, expected_result)
        self.mock_ops.execute.assert_called_once_with(
            query=self.mock_queries.INSERT_TAG,
            values={'tag_name': tag_name}
        )
        self.mock_ops.fetch.assert_called_once_with(
            query=self.mock_queries.GET_TAGS,
            values={},
            columns=['id', 'tag_name', 'is_batch_name', 'is_lot_size', 'is_control_tag']
        )

    def test_insert_partition(self):
        start_time = '2023-01-01 00:00:00'
        expected_result = [
            {'id': 1, 'start_time': '2023-01-01 00:00:00', 'end_time': '2023-01-01 01:00:00'},
            {'id': 2, 'start_time': '2023-01-01 01:00:00', 'end_time': '2023-01-01 02:00:00'},
            {'id': 3, 'start_time': '2023-01-01 02:00:00', 'end_time': '2023-01-01 03:00:00'},
            {'id': 4, 'start_time': '2023-01-01 00:00:00', 'end_time': None}
        ]
        self.mock_ops.fetch.return_value = expected_result
        result = self.db_ops.insert_partition(start_time)
        self.assertEqual(result, expected_result)
        self.mock_ops.execute.assert_called_once_with(
            query=self.mock_queries.INSERT_PARTITION,
            values={'start_time': start_time}
        )
        self.mock_ops.fetch.assert_called_once_with(
            query=self.mock_queries.FIRST_THREE_PARTITIONS,
            values={},
            columns=['id', 'start_time', 'end_time']
        )

    def test_end_partition(self):
        partition_id = 1
        end_time = '2023-01-01 01:00:00'
        expected_result = [
            {'id': 1, 'start_time': '2023-01-01 00:00:00', 'end_time': '2023-01-01 01:00:00'},
            {'id': 2, 'start_time': '2023-01-01 01:00:00', 'end_time': '2023-01-01 02:00:00'},
            {'id': 3, 'start_time': '2023-01-01 02:00:00', 'end_time': '2023-01-01 03:00:00'}
        ]
        self.mock_ops.fetch.return_value = expected_result
        result = self.db_ops.end_partition(partition_id, end_time)
        self.assertEqual(result, expected_result)
        self.mock_ops.execute.assert_called_once_with(
            query=self.mock_queries.END_PARTITION,
            values={'id': partition_id, 'end_time': end_time}
        )
        self.mock_ops.fetch.assert_called_once_with(
            query=self.mock_queries.FIRST_THREE_PARTITIONS,
            values={},
            columns=['id', 'start_time', 'end_time']
        )


if __name__ == '__main__':
    unittest.main()
