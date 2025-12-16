import sys
import os

# Add project root to path so imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from src.config import Config
    from src.services.budget_manager import BudgetManager
    from src.main import get_account_status
    
    print("SUCCESS: Imports are working!")
    print(f"Budget Limit: {Config.MAX_CREDITS}")
    
    # We won't call get_account_status() because it needs a real API key 
    # and we don't want to burn credits or fail on auth in a basic sanity check.
    # But just checking the import is enough to prove the structure is valid.
    
except ImportError as e:
    print(f"FAILURE: Import error: {e}")
except Exception as e:
    print(f"FAILURE: {e}")
