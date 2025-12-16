from cachetools import TTLCache
from typing import Any, Optional, Literal

# Cache Types
CacheType = Literal["schema", "query", "status", "budget"]

class CacheManager:
    def __init__(self, 
                 ttl_schema: int = 86400, 
                 ttl_query: int = 86400, 
                 ttl_status: int = 300):
        
        # Schemas rarely change -> Long TTL (24h)
        self.schema_cache = TTLCache(maxsize=100, ttl=ttl_schema)
        
        # Query metadata (SQL, params) rarely changes -> Long TTL (24h)
        self.query_cache = TTLCache(maxsize=500, ttl=ttl_query)
        
        # Job status changes frequently -> Short TTL (5m)
        self.status_cache = TTLCache(maxsize=100, ttl=ttl_status)
        
        # User budget -> Short TTL (5m)
        self.budget_cache = TTLCache(maxsize=1, ttl=300)

    def get(self, cache_type: CacheType, key: str) -> Optional[Any]:
        if cache_type == "schema":
            return self.schema_cache.get(key)
        elif cache_type == "query":
            return self.query_cache.get(key)
        elif cache_type == "status":
            return self.status_cache.get(key)
        elif cache_type == "budget":
            return self.budget_cache.get(key)
        return None

    def set(self, cache_type: CacheType, key: str, value: Any):
        if cache_type == "schema":
            self.schema_cache[key] = value
        elif cache_type == "query":
            self.query_cache[key] = value
        elif cache_type == "status":
            self.status_cache[key] = value
        elif cache_type == "budget":
            self.budget_cache[key] = value

    def clear(self):
        self.schema_cache.clear()
        self.query_cache.clear()
        self.status_cache.clear()
        self.budget_cache.clear()
