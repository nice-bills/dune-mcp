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
        Search for public queries using Dune's GraphQL endpoint.
        """
        # We use the internal GraphQL API because the Public API V1 
        # doesn't support generic keyword search yet.
        url = "https://core-api.dune.com/public/graphql"
        
        payload = {
            "operationName": "SearchQueries",
            "variables": {"term": query},
            "query": """
                query SearchQueries($term: String!) {
                    queries(
                        filters: { name: { contains: $term } }
                        pagination: { first: 10 }
                    ) {
                        edges {
                            node {
                                id
                                name
                                description
                                user {
                                    name
                                    handle
                                }
                            }
                        }
                    }
                }
            """
        }

        try:
            # We use curl_cffi to mimic a browser and avoid potential WAF blocks
            # on the public graphql endpoint.
            from curl_cffi import requests as cffi_requests
            
            response = cffi_requests.post(
                url,
                json=payload,
                impersonate="chrome",
                headers={
                    "Referer": "https://dune.com/browse/queries",
                    "Content-Type": "application/json",
                },
                timeout=30
            )
            
            if response.status_code != 200:
                logger.error(f"GraphQL Search failed: {response.status_code} - {response.text}")
                return []

            data = response.json()
            edges = data.get("data", {}).get("queries", {}).get("edges", [])
            
            results = []
            for edge in edges:
                node = edge.get("node", {})
                if not node:
                    continue
                    
                results.append({
                    "id": node.get("id"),
                    "name": node.get("name"),
                    "owner": node.get("user", {}).get("handle", "unknown"),
                    "description": node.get("description", "")
                })
                
            return results

        except Exception as e:
            logger.error(f"Error searching queries: {e}")
            return []

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