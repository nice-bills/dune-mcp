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
from src.services.error_analyzer import ErrorAnalyzer

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
error_analyzer = ErrorAnalyzer(dune_service)

# Initialize MCP Server
mcp = FastMCP("Dune Analytics")

@mcp.tool()
def analyze_query_error(error_message: str, query_sql: str) -> str:
    """
    Analyze a failed query execution to provide actionable suggestions for fixing it.
    Use this when 'execute_query' returns an error to understand what went wrong
    and how to fix it (e.g. suggesting correct column names).
    """
    analysis = error_analyzer.analyze(error_message, query_sql)
    
    response = [f"Error Type: {analysis['error_type']}"]
    if analysis['suggestion']:
        response.append(f"Suggestion: {analysis['suggestion']}")
    else:
        response.append("No specific suggestion found. Please check the SQL syntax and schema manually.")
        
    return "\n".join(response)

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
    CRITICAL: Use this tool to discover table names and schema patterns by inspecting 
    SQL from similar queries. DO NOT GUESS table names.
    Returns a summary list of matching queries.
    """
    results = dune_service.search_queries(query)
    
    # Handle WAF/Error
    if isinstance(results, dict) and "error" in results:
        return f"Error: {results['error']}"
        
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
    Use this to understand what a query does before running it, or to 
    extract table names from working SQL.
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
def get_table_schema(table_name: str) -> str:
    """
    Get the column definitions for a specific table.
    ⚠️ WARNING: This tool executes a 'SELECT * LIMIT 0' query which CONSUMES CREDITS.
    Use this ONLY if you know the table name but cannot find its schema via existing queries.
    """
    try:
        # 1. Check Budget (This counts as a query execution)
        budget_manager.check_can_execute_query(estimated_cost=1) # Assume 1 credit base cost
        
        # 2. Execute
        schema = dune_service.get_table_schema(table_name)
        
        # 3. Track Budget
        budget_manager.track_execution(cost=1) # Track the spend
        
        cols = schema.get("columns", [])
        col_strs = [f"- {c['name']} ({c['type']})" for c in cols]
        
        return f"Schema for '{table_name}':\n" + "\n".join(col_strs)

    except BudgetExceededError as e:
        return f"SCHEMA ACCESS DENIED: {str(e)}"
    except Exception as e:
        return f"Error fetching schema: {str(e)}"

@mcp.tool()
def execute_query(query_id: int, params: Optional[Dict[str, Any]] = None) -> str:
    """
    Execute a specific query ID. 
    CHECKS BUDGET FIRST. Returns a Job ID to track progress.
    WARNING: Ensure query_id is valid. Do not execute queries with guessed table names.
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
def analyze_results(job_id: str) -> str:
    """
    Perform advanced statistical analysis on query results.
    Detects outliers (Z-score > 3) and heuristic trends in numeric data.
    Use this to find anomalies or changes in data over time.
    """
    try:
        # Check status first
        state = dune_service.get_status(job_id)
        if state != "QUERY_STATE_COMPLETED" and state != "COMPLETED":
            return f"Job is not complete (Status: {state}). Cannot analyze."

        analysis = dune_service.analyze_result(job_id, data_processor)
        
        if "error" in analysis:
            return f"Analysis Failed: {analysis['error']}"
            
        summary = [f"Analysis for Job {job_id} ({analysis['row_count']} rows):"]
        
        for col, stats in analysis.get("numeric_analysis", {}).items():
            summary.append(f"\nColumn: {col}")
            summary.append(f"  Mean: {stats.get('mean'):.2f} | StdDev: {stats.get('std_dev'):.2f}")
            
            outliers = stats.get("outlier_count", 0)
            if outliers > 0:
                summary.append(f"  ⚠️ Outliers detected: {outliers} values (>3 sigma)")
                summary.append(f"  Sample outliers: {stats.get('top_outliers')}")
            else:
                summary.append("  No significant outliers.")
                
            if "trend_heuristic" in stats:
                summary.append(f"  Trend: {stats['trend_heuristic']}")
                
        return "\n".join(summary)
    except Exception as e:
        return f"Error analyzing results: {str(e)}"

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

@mcp.tool()
def list_user_queries(handle: Optional[str] = None, limit: int = 10) -> str:
    """
    List queries created by a specific user handle.
    If 'handle' is omitted, tries to use the 'DUNE_USER_HANDLE' from env config.
    """
    target_handle = handle or config.DUNE_USER_HANDLE
    
    if not target_handle:
        return (
            "Error: No user handle provided. "
            "Please provide a 'handle' argument or set DUNE_USER_HANDLE in your .env file."
        )

    user_id = dune_service.get_user_id_by_handle(target_handle)
    
    # Check for sentinel -1 (Blocked)
    if user_id == -1:
        return "Error: Public search is currently blocked by Dune's security. Please use 'search_spellbook' or 'get_query_details' instead."
        
    if not user_id:
        return f"Error: Could not find user with handle '{target_handle}'."
        
    results = dune_service.list_user_queries(user_id, limit)
    
    # Handle WAF/Error in results
    if isinstance(results, dict) and "error" in results:
        return f"Error: {results['error']}"

    if not results:
        return f"No queries found for user '{target_handle}'."

    summary = []
    for q in results:
        summary.append(f"ID: {q.get('id')} | Name: {q.get('name')} | Owner: {q.get('owner')}")
    
    return "\n".join(summary)

@mcp.tool()
def search_spellbook(keyword: str) -> str:
    """
    Search the Dune Spellbook (GitHub) for official table definitions and logic.
    Use this to find high-quality, community-vetted table schemas (e.g. for 'uniswap').
    Returns a list of matching file paths (SQL models and YAML schemas).
    """
    results = dune_service.search_spellbook(keyword)
    if not results:
        return f"No results found in Spellbook for '{keyword}'."
        
    summary = []
    for f in results[:15]: # Limit to top 15 matches
        summary.append(f"[{f['type']}] {f['path']}")
    
    return "\n".join(summary)

@mcp.tool()
def get_spellbook_file_content(path: str) -> str:
    """
    Fetch the content of a file from the Dune Spellbook repository.
    Use this to read the SQL logic or Schema definition of a file found via 'search_spellbook'.
    'path' should be the relative path returned by search (e.g., 'models/dex/uniswap/trades.sql').
    """
    content = dune_service.get_spellbook_file_content(path)
    if not content:
        return f"Error: Could not fetch content for '{path}'."
    
    return f"File: {path}\n\n{content}"

if __name__ == "__main__":
    mcp.run()
