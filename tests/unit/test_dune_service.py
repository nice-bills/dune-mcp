import unittest
from unittest.mock import MagicMock, patch
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.services.dune_client import DuneService
from src.services.cache import CacheManager

class TestDuneService(unittest.TestCase):
    def setUp(self):
        # Mock CacheManager
        self.mock_cache = MagicMock(spec=CacheManager)
        self.mock_cache.get.return_value = None
        
        # Patch DuneClient to avoid needing API key
        with patch("src.services.dune_client.DuneClient") as MockClient:
            self.service = DuneService(self.mock_cache)
            self.mock_dune_client = MockClient.return_value

    @patch("src.services.dune_client.requests.post") # Patching requests inside our method wrapper
    def test_search_queries_waf_block(self, mock_post):
        # Setup mock to simulate WAF 403
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.text = "Access Denied"
        mock_post.return_value = mock_response
        
        # We need to patch the import inside the method, or the method uses local import
        # In `_get_graphql_response`, we do `from curl_cffi import requests`.
        # Patching that specific local import is hard. 
        # Easier strategy: Patch `_get_graphql_response` directly if we want to test `search_queries`,
        # OR mock `curl_cffi.requests.post` globally.
        pass

    @patch("curl_cffi.requests.post")
    def test_search_queries_success(self, mock_post):
        # Setup mock for success
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "queries": {
                    "edges": [
                        {
                            "node": {
                                "id": 123,
                                "name": "Test Query",
                                "description": "Desc",
                                "user": {"handle": "tester"}
                            }
                        }
                    ]
                }
            }
        }
        mock_post.return_value = mock_response

        results = self.service.search_queries("test")
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], 123)
        self.assertEqual(results[0]["owner"], "tester")

    @patch("curl_cffi.requests.post")
    def test_waf_handling(self, mock_post):
        # Setup mock for Cloudflare block
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.text = "Access Denied"
        mock_post.return_value = mock_response

        # search_queries calls _get_graphql_response
        result = self.service.search_queries("blocked_term")
        
        # Should return the error dict (with the descriptive message)
        self.assertIsInstance(result, dict)
        self.assertIn("Public search is currently blocked", result.get("error"))

if __name__ == "__main__":
    unittest.main()