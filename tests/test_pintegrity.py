import unittest
import os
import sys
from mock import patch, Mock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pintegrity.pintegrity import Database


class Database_Test(unittest.TestCase):

    @patch('pintegrity.pintegrity.Database')
    def setUp(self, mock_db):
        mock_db.create_table()
        self.db = Database(mock_db)

    def test_build_insert_query(self):
        insert_values = {'k1':'v1', 'k2':'v2'}
        query, values = self.db.build_insert_query('table', **insert_values)
        self.assertEqual(query, 'INSERT INTO table(k2, k1) VALUES (?, ?)')
        self.assertTupleEqual(values, ('v2', 'v1'))

