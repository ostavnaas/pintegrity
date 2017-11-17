import unittest
import os
import sys
from mock import patch, Mock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pintegrity.pintegrity import Database


class Database_Test(unittest.TestCase):

    def setUp(self):
        self.db = Database(':memory:')
        query = """INSERT INTO files(file_path, file_name, last_modify, file_hash)
                   VALUES (?, ?, ?, ?)"""
        c = self.db.connect.cursor()
        c.execute(query, ('path', 'name', 'datastamp', 'hash'))
        self.db.connect.commit()


    def test_build_insert_query(self):
        insert_values = {'k1': 'v1', 'k2': 'v2'}
        query, values = self.db.build_insert_query('table', **insert_values)
        self.assertEqual(query, 'INSERT INTO table(k2, k1) VALUES (?, ?)')
        self.assertTupleEqual(values, ('v2', 'v1'))

    def test_query_file(self):
        res = self.db.query_file('path', 'name')
        self.assertEqual(res, (1, 'hash', 'path', 'name'))
