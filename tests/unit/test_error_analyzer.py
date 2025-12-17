import unittest
from unittest.mock import MagicMock
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.services.error_analyzer import ErrorAnalyzer
from src.services.dune_client import DuneService

class TestErrorAnalyzer(unittest.TestCase):
    def setUp(self):
        # Mock DuneService since we might use it later for deeper analysis
        self.mock_dune = MagicMock(spec=DuneService)
        self.analyzer = ErrorAnalyzer(self.mock_dune)

    def test_column_not_found_block_time(self):
        error = "Column 'block_time' cannot be resolved"
        sql = "SELECT block_time FROM ethereum.transactions"
        result = self.analyzer.analyze(error, sql)
        
        self.assertEqual(result["error_type"], "ColumnNotFound")
        self.assertIn("evt_block_time", result["suggestion"])

    def test_column_not_found_generic(self):
        error = "Column 'my_col' not found"
        sql = "SELECT my_col FROM uniswap.trades"
        result = self.analyzer.analyze(error, sql)
        
        self.assertEqual(result["error_type"], "ColumnNotFound")
        self.assertIn("searching Spellbook", result["suggestion"])
        self.assertIn("uniswap.trades", result["suggestion"])

    def test_table_not_found(self):
        error = "Table 'bad.table' cannot be resolved"
        sql = "SELECT * FROM bad.table"
        result = self.analyzer.analyze(error, sql)
        
        self.assertEqual(result["error_type"], "TableNotFound")
        self.assertIn("bad.table", result["suggestion"])
        self.assertIn("search_spellbook", result["suggestion"])

    def test_unknown_error(self):
        error = "Something went wrong"
        sql = "SELECT *"
        result = self.analyzer.analyze(error, sql)
        
        self.assertEqual(result["error_type"], "Unknown")
        self.assertIsNone(result["suggestion"])

if __name__ == "__main__":
    unittest.main()