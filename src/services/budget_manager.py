from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

class BudgetExceededError(Exception):
    """Raised when a budget limit is exceeded."""
    pass

@dataclass
class BudgetConfig:
    max_queries: int = 5
    max_credits: float = 100.0
    max_schema_calls: int = 3

class BudgetManager:
    def __init__(self, config: BudgetConfig):
        self.config = config
        self.query_count = 0
        self.schema_calls = 0
        self.used_credits = 0.0
        
    def check_can_execute_query(self, estimated_cost: float = 0.0) -> None:
        """
        Check if a new query can be executed within the session budget.
        Raises BudgetExceededError if limits would be breached.
        """
        # Check query count limit
        if self.query_count >= self.config.max_queries:
            raise BudgetExceededError(
                f"Session limit reached: Maximum {self.config.max_queries} queries allowed. "
                "Please search for existing queries or optimize your workflow."
            )
            
        # Check credit limit
        if self.used_credits + estimated_cost > self.config.max_credits:
            remaining = self.config.max_credits - self.used_credits
            raise BudgetExceededError(
                f"Credit limit exceeded: Query costs ~{estimated_cost} credits, "
                f"but only {remaining:.2f} remaining in session budget."
            )
            
    def check_can_access_schema(self) -> None:
        """
        Check if schema access is allowed.
        """
        if self.schema_calls >= self.config.max_schema_calls:
            raise BudgetExceededError(
                f"Schema access limit reached: Max {self.config.max_schema_calls} calls allowed. "
                "Schema lookups are expensive. Please use 'search_public_queries' instead."
            )
            
    def track_execution(self, cost: float) -> None:
        """Record a successful execution."""
        self.query_count += 1
        self.used_credits += cost
        logger.info(f"Budget update: {self.query_count}/{self.config.max_queries} queries, "
                    f"{self.used_credits:.2f}/{self.config.max_credits} credits used.")

    def track_schema_access(self) -> None:
        """Record a schema access."""
        self.schema_calls += 1
        logger.info(f"Budget update: {self.schema_calls}/{self.config.max_schema_calls} schema calls.")

    def get_status(self) -> dict:
        return {
            "queries": {
                "used": self.query_count,
                "limit": self.config.max_queries,
                "remaining": self.config.max_queries - self.query_count
            },
            "credits": {
                "used": self.used_credits,
                "limit": self.config.max_credits,
                "remaining": self.config.max_credits - self.used_credits
            },
            "schema_calls": {
                "used": self.schema_calls,
                "limit": self.config.max_schema_calls,
                "remaining": self.config.max_schema_calls - self.schema_calls
            }
        }
