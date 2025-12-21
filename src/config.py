import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    DUNE_API_KEYS = [k.strip() for k in os.getenv("DUNE_API_KEYS", "").split(",") if k.strip()] or [os.getenv("DUNE_API_KEY")]
    # Remove None values if DUNE_API_KEY was also missing
    DUNE_API_KEYS = [k for k in DUNE_API_KEYS if k]
    
    DUNE_API_KEY = DUNE_API_KEYS[0] if DUNE_API_KEYS else None
    
    MAX_QUERIES = int(os.getenv("MAX_QUERIES_PER_SESSION", 5))
    MAX_CREDITS = float(os.getenv("MAX_CREDITS_PER_SESSION", 100.0))
    MAX_SCHEMA_CALLS = int(os.getenv("MAX_SCHEMA_CALLS_PER_SESSION", 3))
    EXPORT_DIR = os.getenv("EXPORT_DIRECTORY", "./dune_exports")
    DUNE_USER_HANDLE = os.getenv("DUNE_USER_HANDLE")
