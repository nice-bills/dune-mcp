from dune_client.client import DuneClient
import inspect

print("Methods on DuneClient:")
for name, method in inspect.getmembers(DuneClient, predicate=inspect.isfunction):
    if not name.startswith("_"):
        print(f"- {name}")
