import unittest
from unittest.mock import MagicMock, patch
from classifier_data_operations import DatabaseOps
import classifier_sqls as queries


class TestDatabaseOps(unittest.TestCase):

    def setUp(self):
        self.db_ops = DatabaseOps()
        self.db_ops.ops = MagicMock()

    def test_insert_batch(self):
        self.db_ops.insert_batch('batch1', '2023-10-01 00:00:00')
        self.db_ops.ops.execute.assert_called_once_with(
            query=queries.INSERT_BATCH,
            values={'batchName': 'batch1', 'startTime': '2023-10-01 00:00:00'}
        )

    def test_update_batch_lot_size(self):
        self.db_ops.update_batch_lot_size('batch1', 100)
        self.db_ops.ops.execute.assert_called_once_with(
            query=queries.UPDATE_BATCH_LOT_SIZE,
            values={'batchName': 'batch1', 'lotSize': 100}
        )

    def test_insert_tag(self):
        self.db_ops.ops.fetch.return_value = [
            {'id': 1, 'tag_name': 'tag1', 'is_batch_name': False, 'is_lot_size': False, 'is_control_tag': False}]
        result = self.db_ops.insert_tag('tag1')
        self.db_ops.ops.execute.assert_called_once_with(
            query=queries.INSERT_TAG,
            values={'tag_name': 'tag1'}
        )
        self.db_ops.ops.fetch.assert_called_once_with(
            query=queries.GET_TAGS,
            values={},
            columns=['id', 'tag_name', 'is_batch_name',
                     'is_lot_size', 'is_control_tag']
        )
        self.assertEqual(result, [
                         {'id': 1, 'tag_name': 'tag1', 'is_batch_name': False, 'is_lot_size': False, 'is_control_tag': False}])

    def test_insert_partition(self):
        self.db_ops.insert_partition('2023-10-01 00:00:00')
        self.db_ops.ops.execute.assert_called_once_with(
            query=queries.INSERT_PARTITION,
            values={'start_time': '2023-10-01 00:00:00'}
        )

    def test_end_partition(self):
        self.db_ops.end_partition(1, '2023-10-01 01:00:00')
        self.db_ops.ops.execute.assert_called_once_with(
            query=queries.END_PARTITION,
            values={'id': 1, 'end_time': '2023-10-01 01:00:00'}
        )

    def test_get_tags(self):
        self.db_ops.ops.fetch.return_value = [
            {'id': 1, 'tag_name': 'tag1', 'is_batch_name': False, 'is_lot_size': False, 'is_control_tag': False}]
        result = self.db_ops.get_tags()
        self.db_ops.ops.fetch.assert_called_once_with(
            query=queries.GET_TAGS,
            values={},
            columns=['id', 'tag_name', 'is_batch_name',
                     'is_lot_size', 'is_control_tag']
        )
        self.assertEqual(result, [
                         {'id': 1, 'tag_name': 'tag1', 'is_batch_name': False, 'is_lot_size': False, 'is_control_tag': False}])

    def test_get_latest_batch(self):
        self.db_ops.ops.fetch.return_value = {
            'batch_id': 1, 'start_time': '2023-10-01 00:00:00', 'end_time': '2023-10-01 01:00:00', 'lot_size': 100}
        result = self.db_ops.get_latest_batch()
        self.db_ops.ops.fetch.assert_called_once_with(
            query=queries.LATEST_BATCH,
            values={},
            columns=['batch_id', 'start_time', 'end_time', 'lot_size']
        )
        self.assertEqual(result, {'batch_id': 1, 'start_time': '2023-10-01 00:00:00',
                         'end_time': '2023-10-01 01:00:00', 'lot_size': 100})

    def test_get_latest_partition(self):
        self.db_ops.ops.fetch.return_value = {
            'id': 1, 'start_time': '2023-10-01 00:00:00', 'end_time': '2023-10-01 01:00:00'}
        result = self.db_ops.get_latest_partition()
        self.db_ops.ops.fetch.assert_called_once_with(
            query=queries.LATEST_PARTITION,
            values={},
            columns=['id', 'start_time', 'end_time']
        )
        self.assertEqual(result, {
                         'id': 1, 'start_time': '2023-10-01 00:00:00', 'end_time': '2023-10-01 01:00:00'})

    def test_find_containing_partition(self):
        self.db_ops.ops.fetch.return_value = {
            'id': 1, 'start_time': '2023-10-01 00:00:00', 'end_time': '2023-10-01 01:00:00'}
        result = self.db_ops.find_containing_partition('2023-10-01 00:30:00')
        self.db_ops.ops.fetch.assert_called_once_with(
            query=queries.FIND_CONTAINING_PARTITION,
            values={'timestamp': '2023-10-01 00:30:00'},
            columns=['id', 'start_time', 'end_time']
        )
        self.assertEqual(result, {
                         'id': 1, 'start_time': '2023-10-01 00:00:00', 'end_time': '2023-10-01 01:00:00'})


if __name__ == '__main__':
    unittest.main()
