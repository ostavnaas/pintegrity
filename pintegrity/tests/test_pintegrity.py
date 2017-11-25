import unittest
import os
import sys
#from mock import patch, Mock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pintegrity import Database


class Database_Test(unittest.TestCase):

    def setUp(self):
        self.db = Database(':memory:')
        query = """INSERT INTO files(file_path,
                                     file_name,
                                     last_modify,
                                     file_hash,
                                     file_removed,
                                     file_corrupted)
                   VALUES (?, ?, ?, ?, '0','0' )"""
        c = self.db.connect.cursor()
        c.execute(query, ('path', 'name', 'datastamp', 'hash'))
        self.db.connect.commit()

    def test_build_insert_query(self):
        insert_values = {'k1': 'v1', 'k2': 'v2'}
        query, values = self.db.build_insert_query('table', **insert_values)
        self.assertTupleEqual(values, ('v1', 'v2'))
        self.assertEqual(query, 'INSERT INTO table(k1, k2) VALUES (?, ?)')
        self.assertTupleEqual(values, ('v1', 'v2'))

    def test_query_file(self):
        res = self.db.query_file('path', 'name')
        self.assertEqual(res, (1, u'path', u'name', u'hash'))

    def test_query_all_files(self):
        for row in self.db.query_all_files():
            self.assertEqual(row, (1, u'path', u'name'))
