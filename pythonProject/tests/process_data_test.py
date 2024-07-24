import unittest
from unittest.mock import MagicMock, patch
from classifier.process_data import ProcessData


class TestProcessData(unittest.TestCase):
    def setUp(self):
        self.mock_data = 'mock_data'
        self.mock_bucket_name = 'mock_bucket'
        self.process_data = ProcessData(self.mock_data, self.mock_bucket_name)

    @patch('process_data.DataClassifier')
    @patch('process_data.DatabaseOps')
    @patch('process_data.FileOps')
    def test_init(self, MockFileOps, MockDatabaseOps, MockDataClassifier):
        self.assertIsInstance(self.process_data.classifier, MockDataClassifier)
        self.assertIsInstance(self.process_data.file_ops, MockFileOps)

    def test_get_batch_tag(self):
        self.process_data.all_tags = [{'tag_name': 'batch', 'is_batch_name': True}, {
            'tag_name': 'not_batch', 'is_batch_name': False}]
        batch_tag = self.process_data._ProcessData__get_batch_tag()
        self.assertEqual(batch_tag['tag_name'], 'batch')

    def test_get_lot_size_tag(self):
        self.process_data.all_tags = [{'tag_name': 'lot', 'is_lot_size': True}, {
            'tag_name': 'not_lot', 'is_lot_size': False}]
        lot_tag = self.process_data._ProcessData__get_lot_size_tag()
        self.assertEqual(lot_tag['tag_name'], 'lot')

    def test_get_tag(self):
        self.process_data.all_tags = [
            {'tag_name': 'specific_tag'}, {'tag_name': 'other_tag'}]
        tag = self.process_data._ProcessData__get_tag('specific_tag')
        self.assertEqual(tag['tag_name'], 'specific_tag')
        tag = self.process_data._ProcessData__get_tag('nonexistent_tag')
        self.assertIsNone(tag)

    @patch('process_data.pd.concat')
    @patch('process_data.datetime.datetime.now')
    @patch('process_data.DataClassifier.classify_data')
    @patch('process_data.FileOps.persist_data')
    def test_process(self, mock_persist_data, mock_classify_data, mock_datetime, mock_concat):
        mock_datetime.return_value.strftime.return_value = '20230101'
        mock_classify_data.return_value = (
            {'batch': pd.DataFrame()}, pd.DataFrame())
        self.process_data._ProcessData__get_batch_tag = MagicMock(
            return_value={'tag_name': 'batch'})
        self.process_data._ProcessData__get_lot_size_tag = MagicMock(
            return_value={'tag_name': 'lot'})
        self.process_data.process()
        mock_persist_data.assert_called()


if __name__ == '__main__':
    unittest.main()
