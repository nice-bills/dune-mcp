from dune_client.client import DuneClient
try:
    from dune_client.query import QueryBase
    print("QueryBase found")
except ImportError:
    print("QueryBase NOT found")

try:
    from dune_client.types import QueryParameter
    print("QueryParameter found")
except ImportError:
    print("QueryParameter NOT found")
