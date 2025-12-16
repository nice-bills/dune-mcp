import os
import logging
import requests
from typing import List, Dict, Any, Optional
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dune_client.types import QueryParameter
from dotenv import load_dotenv

from .cache import CacheManager

load_dotenv()
logger = logging.getLogger(__name__)

class DuneService:
    def __init__(self, cache_manager: CacheManager):
        self.api_key = os.getenv("DUNE_API_KEY")
        self.base_url = os.getenv("DUNE_API_BASE_URL", "https://api.dune.com/api/v1")
        
        if not self.api_key:
            raise ValueError("DUNE_API_KEY environment variable not set")
            
        self.client = DuneClient(self.api_key)
        self.cache = cache_manager

    def search_queries(self, query: str) -> List[Dict[str, Any]]:
        """
        Search for public queries. 
        Note: The official SDK lacks a search method, so we use the raw API endpoint.
        """
        # Attempt to use the community/public search endpoint if it exists
        # endpoint: /query?q=... (This is often not public API)
        # Fallback: List user's queries if search is unavailable.
        
        # NOTE: Dune Public API v1 doesn't have a generic "Search all queries" endpoint documented freely.
        # It mostly allows operating on specific query IDs.
        # However, for the sake of the MCP "Query Reuse" principle, we will simulate this
        # by searching the USER'S library, which is accessible.
        
        # If we truly can't search public, we might need to restrict this tool to 
        # "search_my_queries".
        
        # Let's try to list user queries (often useful enough for analysts).
        # We'll use a raw request for this as SDK list support is sparse.
        
        # Mocking for MVP if API is restrictive:
        # We can return an empty list or a message saying "Search not supported in V1 API".
        # BUT, let's try a direct request to list queries.
        
        return [] # Placeholder until we confirm the endpoint.

    def get_query(self, query_id: int) -> Dict[str, Any]:
        cache_key = str(query_id)
        cached = self.cache.get("query", cache_key)
        if cached:
            return cached

        try:
            query = self.client.get_query(query_id)
            # Serialize
            data = {
                "id": query.base.query_id,
                "name": query.base.name,
                "description": query.base.description or "",
                "sql": query.sql,
                "parameters": [p.to_dict() for p in query.base.parameters] if query.base.parameters else []
            }
            self.cache.set("query", cache_key, data)
            return data
        except Exception as e:
            logger.error(f"Error fetching query {query_id}: {e}")
            raise

    def execute_query(self, query_id: int, params: Optional[Dict[str, Any]] = None) -> str:
        # We want to start execution and return ID, NOT wait.
        # client.execute_query() waits.
        # client.execute() (base method) usually returns the response with job_id.
        
        query = QueryBase(
            query_id=query_id, 
            params=[QueryParameter.from_dict(p) for p in (params or [])]
        )
        
        # Using the lower-level execute method to get job_id without waiting
        # The SDK implementation of execute() typically performs the POST /execute call
        try:
            # We construct the execution payload manually if SDK doesn't expose non-blocking nicely
            # Or checking SDK: client.execute(query) -> ExecutionResult (which contains job_id)
            # wait... client.execute() waits for completion loop.
            
            # Use raw request for async trigger to be safe and efficient
            url = f"{self.base_url}/query/{query_id}/execute"
            payload = {"query_parameters": {p["name"]: p["value"] for p in (params or [])}}
            headers = {"X-Dune-Api-Key": self.api_key}
            
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            return data["execution_id"]
            
        except Exception as e:
            logger.error(f"Error executing query {query_id}: {e}")
            raise

    def get_status(self, job_id: str) -> str:
        cached = self.cache.get("status", job_id)
        if cached:
            return cached
            
        status = self.client.get_execution_status(job_id)
        state = status.state # e.g. QUERY_STATE_COMPLETED
        
        # Normalize state
        state_str = str(state).replace("ExecutionState.", "")
        
        if state_str in ["COMPLETED", "FAILED", "CANCELLED"]:
            self.cache.set("status", job_id, state_str)
            
        return state_str

    def get_result(self, job_id: str) -> Any:
        return self.client.get_execution_results(job_id)
        
    def get_usage(self) -> Dict[str, Any]:
        """Returns the credit usage info."""
        # Raw request as get_usage might return complex object
        url = f"{self.base_url}/auth/usage" # Verify endpoint
        # The SDK has client.get_usage(), let's use that if it works
        try:
            # Note: SDK get_usage might be for generic usage or specific endpoint
            # Let's rely on SDK
            # Inspecting check_dune.py output: 'get_usage' exists!
            # It likely returns a pydantic model or dict.
            return self.client.get_usage() # We will inspect this return type at runtime
        except Exception as e:
            logger.error(f"Error getting usage: {e}")
            # Fallback mock
            return {"error": str(e)}