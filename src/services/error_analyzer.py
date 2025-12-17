import re
import logging
from typing import Dict, Optional, Any
from .dune_client import DuneService

logger = logging.getLogger(__name__)

class ErrorAnalyzer:
    def __init__(self, dune_service: DuneService):
        self.dune = dune_service

    def analyze(self, error_message: str, sql: str) -> Dict[str, Any]:
        """
        Analyze a query error and SQL to provide suggestions.
        """
        analysis = {
            "error_type": "Unknown",
            "original_error": error_message,
            "suggestion": None
        }

        # 1. Check for "Column not found"
        # Example error: "Column 'block_time' cannot be resolved" or "Column 'block_time' not found"
        col_match = re.search(r"Column '(\w+)'", error_message, re.IGNORECASE)
        if col_match:
            bad_col = col_match.group(1)
            analysis["error_type"] = "ColumnNotFound"
            
            # Heuristic: Check for common renames
            if bad_col == "block_time":
                analysis["suggestion"] = "In many raw decoded tables, 'block_time' is named 'evt_block_time'. Try using 'evt_block_time'."
            elif bad_col == "address":
                analysis["suggestion"] = "Try 'contract_address' or 'wallet_address' depending on context."
            else:
                # Advanced: Search spellbook for table usage
                # Extract table name from SQL (very basic regex)
                table_match = re.search(r"FROM\s+([\w\.]+)", sql, re.IGNORECASE)
                if table_match:
                    table_name = table_match.group(1)
                    analysis["suggestion"] = f"Column '{bad_col}' not found in '{table_name}'. Try searching Spellbook for '{table_name}' to see valid columns."
                else:
                    analysis["suggestion"] = f"Column '{bad_col}' appears incorrect. Check schema."

        # 2. Check for "Table not found"
        elif "Table" in error_message and ("not found" in error_message or "cannot be resolved" in error_message):
             analysis["error_type"] = "TableNotFound"
             
             # Extract likely table
             table_match = re.search(r"Table '([\w\.]+)'", error_message, re.IGNORECASE)
             if table_match:
                 bad_table = table_match.group(1)
                 # Suggest searching spellbook
                 analysis["suggestion"] = f"Table '{bad_table}' does not exist. Use 'search_spellbook' to find the correct table name."

        return analysis