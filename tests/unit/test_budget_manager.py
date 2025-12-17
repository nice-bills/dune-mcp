import unittest
import sys
import os

# Ensure src is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.services.budget_manager import BudgetManager, BudgetConfig, BudgetExceededError

class TestBudgetManager(unittest.TestCase):
    def setUp(self):
        # Default config for testing
        self.config = BudgetConfig(
            max_queries=2,
            max_credits=10.0,
            max_schema_calls=2
        )
        self.budget = BudgetManager(self.config)

    def test_initial_state(self):
        status = self.budget.get_status()
        self.assertEqual(status["queries"]["used"], 0)
        self.assertEqual(status["credits"]["used"], 0.0)
        self.assertEqual(status["schema_calls"]["used"], 0)

    def test_query_tracking(self):
        # Should allow execution
        self.budget.check_can_execute_query(estimated_cost=1.0)
        self.budget.track_execution(cost=1.0)
        
        status = self.budget.get_status()
        self.assertEqual(status["queries"]["used"], 1)
        self.assertEqual(status["credits"]["used"], 1.0)

    def test_query_limit_enforcement(self):
        # Execute 2 queries (max allowed)
        self.budget.track_execution(1.0)
        self.budget.track_execution(1.0)
        
        # 3rd query should fail
        with self.assertRaises(BudgetExceededError) as cm:
            self.budget.check_can_execute_query(1.0)
        
        self.assertIn("Session limit reached", str(cm.exception))

    def test_credit_limit_enforcement(self):
        # Execute expensive query
        self.budget.track_execution(9.0) # 1.0 remaining
        
        # Try query costing 2.0 (total 11.0 > 10.0)
        with self.assertRaises(BudgetExceededError) as cm:
            self.budget.check_can_execute_query(estimated_cost=2.0)
            
        self.assertIn("Credit limit exceeded", str(cm.exception))

    def test_schema_limit_enforcement(self):
        self.budget.track_schema_access()
        self.budget.track_schema_access()
        
        # 3rd schema call should fail
        with self.assertRaises(BudgetExceededError) as cm:
            self.budget.check_can_access_schema()
            
        self.assertIn("Schema access limit reached", str(cm.exception))

if __name__ == "__main__":
    unittest.main()