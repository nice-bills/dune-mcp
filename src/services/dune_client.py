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

    def _get_graphql_response(self, payload: Dict[str, Any], timeout: int = 30) -> Optional[Dict[str, Any]]:
        url = "https://core-api.dune.com/public/graphql"
        try:
            from curl_cffi import requests as cffi_requests
            response = cffi_requests.post(
                url,
                json=payload,
                impersonate="chrome",
                headers={
                    "Referer": "https://dune.com/browse/queries",
                    "Content-Type": "application/json",
                },
                timeout=timeout
            )
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            return response.json()
        except Exception as e:
            logger.error(f"GraphQL request failed: {e}")
            return None

    def _github_api_request(self, url: str) -> Optional[Dict[str, Any]]:
        headers = {"Accept": "application/vnd.github.v3+json"}
        # For higher rate limits, user can set GITHUB_TOKEN in .env
        github_token = os.getenv("GITHUB_TOKEN")
        if github_token:
            headers["Authorization"] = f"token {github_token}"
            
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"GitHub API request failed for {url}: {e}")
            return None

    def search_spellbook(self, keyword: str) -> List[Dict[str, Any]]:
        """
        Searches the duneanalytics/spellbook GitHub repository for SQL models and schema files.
        """
        base_url = "https://api.github.com/search/code"
        repo = "repo:duneanalytics/spellbook"
        
        # Search for .sql files
        sql_query = f"{keyword} {repo} in:file extension:sql"
        sql_url = f"{base_url}?q={sql_query}"
        sql_results = self._github_api_request(sql_url)
        
        # Search for schema.yml files (which might contain definitions)
        yaml_query = f"{keyword} {repo} in:file filename:schema.yml"
        yaml_url = f"{base_url}?q={yaml_query}"
        yaml_results = self._github_api_request(yaml_url)
        
        found_files = []
        if sql_results and sql_results.get("items"):
            for item in sql_results["items"]:
                found_files.append({
                    "name": item["name"],
                    "path": item["path"],
                    "url": item["html_url"],
                    "type": "sql_model",
                    "repo_url": item["url"] # API URL for file content
                })
        
        if yaml_results and yaml_results.get("items"):
            for item in yaml_results["items"]:
                found_files.append({
                    "name": item["name"],
                    "path": item["path"],
                    "url": item["html_url"],
                    "type": "schema_definition",
                    "repo_url": item["url"]
                })
        
        return found_files

    def get_spellbook_file_content(self, path: str) -> Optional[str]:
        """
        Fetches the raw content of a file from the duneanalytics/spellbook GitHub repository.
        'path' should be the full path within the repository (e.g., 'models/dex/uniswap/trades.sql').
        """
        # GitHub raw content URL pattern
        raw_url = f"https://raw.githubusercontent.com/duneanalytics/spellbook/main/{path}"
        try:
            response = requests.get(raw_url, timeout=15)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Failed to fetch content for {path} from GitHub: {e}")
            return None

    def get_user_id_by_handle(self, handle: str) -> Optional[int]:
        payload = {
            "operationName": "FindUser",
            "variables": {"name": handle},
            "query": """
                query FindUser($name: String!) {
                    users(filters: { name: { equals: $name } }) {
                        edges {
                            node {
                                id
                                name
                                handle
                            }
                        }
                    }
                }
            """
        }
        
        response_data = self._get_graphql_response(payload)
        if response_data:
            edges = response_data.get("data", {}).get("users", {}).get("edges", [])
            if edges:
                # Assuming handle is unique, take the first result
                user_id = edges[0].get("node", {}).get("id")
                try:
                    return int(user_id)
                except (ValueError, TypeError):
                    logger.error(f"Could not convert user ID '{user_id}' to int for handle '{handle}'")
                    return None
        return None

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

    def list_user_queries(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """
        List queries for a given user ID using Dune's GraphQL endpoint.
        """
        url = "https://core-api.dune.com/public/graphql" # Redundant, but good for clarity.
        
        payload = {
            "operationName": "ListUserQueries",
            "variables": {"userId": user_id, "limit": limit},
            "query": """
                query ListUserQueries($userId: Int!, $limit: Int!) {
                    queries(
                        filters: { userId: { equals: $userId } }
                        pagination: { first: $limit }
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

        response_data = self._get_graphql_response(payload)
        if response_data:
            edges = response_data.get("data", {}).get("queries", {}).get("edges", [])
            
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
        return []

    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Fetches the schema (columns and types) for a table by running a
        'SELECT * FROM table LIMIT 0' query.
        WARNING: This consumes Dune credits!
        """
        sql = f"SELECT * FROM {table_name} LIMIT 0"
        
        # We need to execute and wait for the result to get metadata
        # create_query usually requires a name, but we can use execute_query with raw sql 
        # via the query_id (if we had one) or generic execute.
        # dune-client 1.x allows executing raw SQL via `execute_query` if we pass a Query object 
        # but usually we need a query ID.
        
        # Actually, the official SDK/API usually requires a Query ID to execute anything.
        # We can't just send raw SQL without an existing Query ID container unless we create one.
        # BUT, we can use the "Query ID 0" or "Ad-hoc" mode if supported, or we must create a query.
        
        # Let's check if we can use `client.run_sql` or similar (from our introspection earlier).
        # We saw `run_sql`. Let's try that.
        
        try:
            # run_sql takes (query_sql, performance=...)
            # It returns a ResultsResponse
            result = self.client.run_sql(
                query_sql=sql,
                performance="medium" # medium is usually fine/cheapest
            )
            
            # The result object has 'meta' -> 'columns'
            # We need to inspect the structure of 'result'
            # Usually result.result.metadata.column_names / column_types
            
            if not result or not result.result:
                return {"error": "No result returned"}

            meta = result.result.metadata
            columns = []
            if meta:
                for col in meta.columns:
                    columns.append({"name": col.name, "type": col.type})
            
            return {
                "table": table_name,
                "columns": columns
            }
            
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}")
            raise

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