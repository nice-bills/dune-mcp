import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    DUNE_API_KEY = os.getenv("DUNE_API_KEY")
    MAX_QUERIES = int(os.getenv("MAX_QUERIES_PER_SESSION", 5))
    MAX_CREDITS = float(os.getenv("MAX_CREDITS_PER_SESSION", 100.0))
    MAX_SCHEMA_CALLS = int(os.getenv("MAX_SCHEMA_CALLS_PER_SESSION", 3))
    EXPORT_DIR = os.getenv("EXPORT_DIRECTORY", "./dune_exports")
    DUNE_USER_HANDLE = os.getenv("DUNE_USER_HANDLE")
