import unittest
from commons.tuple_to_dictionary import ResponseMapper


class ResponseMapperTests(unittest.TestCase):
    def setUp(self):
        self.mapper = ResponseMapper(['name', 'age', 'city'])

    def test_map_with_empty_rows(self):
        rows = []
        expected = []
        result = self.mapper.map(rows)
        self.assertEqual(result, expected)

    def test_map_with_single_row(self):
        rows = [('John', 25, 'New York',)]
        expected = {'name': 'John', 'age': 25, 'city': 'New York'}
        result = self.mapper.map(rows)
        self.assertEqual(result, expected)

    def test_map_with_multiple_rows(self):
        rows = [('John', 25, 'New York'), ('Alice', 30, 'London')]
        expected = [{'name': 'John', 'age': 25, 'city': 'New York'}, {
            'name': 'Alice', 'age': 30, 'city': 'London'}]
        result = self.mapper.map(rows)
        self.assertEqual(result, expected)

    def test_map_with_none_rows(self):
        rows = None
        expected = []
        result = self.mapper.map(rows)
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
