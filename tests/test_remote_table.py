import sys
import os
# Ensure the package source directory is on sys.path (src/remote_table)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SRC = os.path.join(ROOT, 'src')
sys.path.insert(0, SRC)

import unittest
import importlib
import importlib.util
from unittest import mock

# Ensure we can import the package from src/
RemoteTable = None
core_mod = None
try:
    from remote_table import RemoteTable
    import src.remote_table.core as core_mod
except Exception:
    # If import fails, individual tests will assert/skips based on available deps
    RemoteTable = None
    try:
        import src.remote_table.core as core_mod
    except Exception:
        core_mod = None


def has_module(name: str) -> bool:
    return importlib.util.find_spec(name) is not None

class TestRemoteTable(unittest.TestCase):
    def test_csv(self):
        path = os.path.join(os.path.dirname(__file__), 'data', 'color.csv')
        if not has_module('pandas') or RemoteTable is None:
            self.skipTest('pandas or remote_table not available')
        table = RemoteTable(path)
        rows = list(table)
        self.assertTrue(len(rows) > 0)
        self.assertIsInstance(rows[0], tuple)

    def test_json(self):
        path = os.path.join(os.path.dirname(__file__), 'data', 'data_no_root.json')
        if not has_module('pandas') or RemoteTable is None:
            self.skipTest('pandas or remote_table not available')
        table = RemoteTable(path)
        rows = list(table)
        self.assertTrue(len(rows) > 0)

    def test_html(self):
        path = os.path.join(os.path.dirname(__file__), 'data', 'table.html')
        if not has_module('bs4') or RemoteTable is None:
            self.skipTest('bs4 or remote_table not available')
        table = RemoteTable(path)
        rows = list(table)
        self.assertTrue(len(rows) > 0)

    def test_header_promotion(self):
        # CSV where first row is headers
        path = os.path.join(os.path.dirname(__file__), 'data', 'color.csv')
        if not has_module('pandas') or RemoteTable is None:
            self.skipTest('pandas or remote_table not available')
        table = RemoteTable(path, headers='first_row', as_dict=True)
        rows = list(table)
        self.assertTrue(len(rows) > 0)
        # rows should be dicts with keys from the CSV header
        self.assertIsInstance(rows[0], dict)
        # the CSV header file uses 'en','es','ru'
        self.assertIn('en', rows[0].keys())

    def test_missing_dependency_html(self):
        # Simulate bs4 missing to ensure helpful ImportError is raised
        if core_mod is None:
            self.skipTest('core module not importable')
        path = os.path.join(os.path.dirname(__file__), 'data', 'table.html')
        # Patch core._lazy_import to raise ImportError for bs4
        orig_lazy = core_mod._lazy_import
        def fake_lazy(name, hint=None):
            if name.startswith('bs4'):
                raise ImportError("Missing optional dependency 'bs4'. Install it with e.g. `pip install beautifulsoup4`")
            return orig_lazy(name, hint)
        # Patch the _lazy_import used by the installed package namespace
        with mock.patch('remote_table.core._lazy_import', side_effect=fake_lazy):
            with self.assertRaises(ImportError) as cm:
                RemoteTable(path)
            self.assertIn('beautifulsoup4', str(cm.exception))

if __name__ == '__main__':
    unittest.main()
