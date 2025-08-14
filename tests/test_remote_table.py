import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

import unittest
from remote_table import RemoteTable

class TestRemoteTable(unittest.TestCase):
    def test_csv(self):
        path = os.path.join(os.path.dirname(__file__), 'data', 'color.csv')
        table = RemoteTable(path)
        rows = list(table)
        self.assertTrue(len(rows) > 0)
        self.assertIsInstance(rows[0], tuple)

    def test_json(self):
        path = os.path.join(os.path.dirname(__file__), 'data', 'data_no_root.json')
        table = RemoteTable(path)
        rows = list(table)
        self.assertTrue(len(rows) > 0)

    def test_html(self):
        path = os.path.join(os.path.dirname(__file__), 'data', 'table.html')
        table = RemoteTable(path)
        rows = list(table)
        self.assertTrue(len(rows) > 0)

if __name__ == '__main__':
    unittest.main()
