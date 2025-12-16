import dune_client
from dune_client.client import DuneClient

print("Client methods:")
for method in dir(DuneClient):
    if not method.startswith("_"):
        print(f" - {method}")
