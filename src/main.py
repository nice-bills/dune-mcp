from mcp.server.fastmcp import FastMCP
from typing import List, Optional, Dict, Any
import sys
import os

# Add project root to path so 'from src...' imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.config import Config
from src.services.budget_manager import BudgetManager, BudgetConfig, BudgetExceededError
from src.services.cache import CacheManager
from src.services.dune_client import DuneService
from src.services.data_processor import DataProcessor

# Initialize Services
config = Config()
budget_config = BudgetConfig(
    max_queries=config.MAX_QUERIES,
    max_credits=config.MAX_CREDITS,
    max_schema_calls=config.MAX_SCHEMA_CALLS
)

budget_manager = BudgetManager(budget_config)
cache_manager = CacheManager()
dune_service = DuneService(cache_manager)
data_processor = DataProcessor(export_dir=config.EXPORT_DIR)

# Initialize MCP Server
mcp = FastMCP("Dune Analytics")

@mcp.tool()
def get_account_status() -> str:
    """
    Check your Dune Analytics credit budget and usage.
    Always run this first to see if you can afford to run queries.
    """
    try:
        usage = dune_service.get_usage()
        
        # Parse usage object
        # UsageResponse(billing_periods=[BillingPeriod(credits_included=..., credits_used=...)], ...)
        if hasattr(usage, 'billing_periods') and usage.billing_periods:
            current = usage.billing_periods[0]
            limit = current.credits_included
            used = current.credits_used
            remaining = limit - used
            
            return (
                f"Dune Account Status:\n"
                f"- Credits Used: {int(used)}\n"
                f"- Credits Limit: {int(limit)}\n"
                f"- Remaining: {int(remaining)}\n"
                f"- Period: {current.start_date} to {current.end_date}"
            )
        
        return f"Dune Account Status: {usage}"
    except Exception as e:
        return f"Could not fetch account status: {str(e)}"

@mcp.tool()
def get_session_budget() -> str:
    """
    Check your remaining session limits (queries, credits, schema calls).
    This is the internal safety guard for this specific chat session.
    """
    status = budget_manager.get_status()
    return (
        f"Session Budget:\n"
        f"- Queries: {status['queries']['used']}/{status['queries']['limit']} used\n"
        f"- Credits: {status['credits']['used']}/{status['credits']['limit']} used\n"
        f"- Schema Calls: {status['schema_calls']['used']}/{status['schema_calls']['limit']} used"
    )

@mcp.tool()
def search_public_queries(query: str) -> str:
    """
    Search for existing public queries on Dune.
    Use this instead of writing new SQL whenever possible.
    Returns a summary list of matching queries.
    """
    results = dune_service.search_queries(query)
    if not results:
        return f"No public queries found matching '{query}'."
    
    # Format as string summary
    summary = []
    for q in results[:10]: # Limit to 10
        summary.append(f"ID: {q.get('id')} | Name: {q.get('name')} | Owner: {q.get('owner')}")
    
    return "\n".join(summary)

@mcp.tool()
def get_query_details(query_id: int) -> str:
    """
    Get the full SQL, description, and parameters for a specific query.
    Use this to understand what a query does before running it.
    """
    try:
        details = dune_service.get_query(query_id)
        return (
            f"Query ID: {details['id']}\n"
            f"Name: {details['name']}\n"
            f"Description: {details['description']}\n"
            f"Parameters: {details['parameters']}\n"
            f"SQL:\n{details['sql']}"
        )
    except Exception as e:
        return f"Error fetching details: {str(e)}"

@mcp.tool()
def execute_query(query_id: int, params: Optional[Dict[str, Any]] = None) -> str:
    """
    Execute a specific query ID. 
    CHECKS BUDGET FIRST. Returns a Job ID to track progress.
    """
    try:
        # 1. Check Session Budget (Query Count)
        # We don't know exact cost yet, so we pass 0 for now, or an estimate if we had one.
        budget_manager.check_can_execute_query(estimated_cost=0) 
        
        # 2. Execute
        job_id = dune_service.execute_query(query_id, params)
        
        # 3. Track (Assume minimal cost for now until we get result metadata)
        budget_manager.track_execution(cost=0) 
        
        return f"Execution started. Job ID: {job_id}. Use 'get_job_status' to check progress."
    
    except BudgetExceededError as e:
        return f"EXECUTION DENIED: {str(e)}"
    except Exception as e:
        return f"Error executing query: {str(e)}"

@mcp.tool()
def get_job_status(job_id: str) -> str:
    """
    Check the status of a running query job (PENDING, COMPLETED, FAILED).
    """
    try:
        state = dune_service.get_status(job_id)
        return f"Job {job_id} is {state}"
    except Exception as e:
        return f"Error checking status: {str(e)}"

@mcp.tool()
def get_job_results_summary(job_id: str) -> str:
    """
    Get a PREVIEW and SUMMARY of the results. 
    Does NOT return the full dataset to save tokens.
    """
    try:
        # Check status first
        state = dune_service.get_status(job_id)
        if state != "QUERY_STATE_COMPLETED" and state != "COMPLETED":
            return f"Job is not complete (Status: {state}). Please wait."

        raw_result = dune_service.get_result(job_id)
        processed = data_processor.process_results(raw_result, limit=5)
        
        return (
            f"Row Count: {processed['row_count']}\n"
            f"Columns: {processed['columns']}\n"
            f"Preview (First 5 rows): {processed['preview']}\n"
            f"Stats: {processed.get('summary_stats', 'N/A')}\n"
            f"Tip: To see all data, use 'export_results_to_csv'."
        )
    except Exception as e:
        return f"Error fetching results: {str(e)}"

@mcp.tool()
def export_results_to_csv(job_id: str) -> str:
    """
    Download the full dataset to a CSV file for analysis in Excel/Python.
    """
    try:
        raw_result = dune_service.get_result(job_id)
        path = data_processor.export_to_csv(raw_result, job_id)
        return f"Success! Data saved to: {path}"
    except Exception as e:
        return f"Error exporting data: {str(e)}"

if __name__ == "__main__":
    mcp.run()
