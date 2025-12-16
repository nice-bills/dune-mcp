from curl_cffi import requests
import json

def test_graphql_bypass():
    url = "https://core-api.dune.com/public/graphql"
    
    # Introspection Query: Ask the schema for available query fields
    payload = {
        "operationName": "IntrospectionQuery",
        "variables": {},
        "query": """
            query IntrospectionQuery {
                __schema {
                    queryType {
                        fields {
                            name
                            description
                        }
                    }
                }
            }
        """
    }

    # Backup: Try the 'queries' list query if search fails
    # payload = { ... }

    print(f"🕵️  Probing GraphQL {url}...")
    
    try:
        response = requests.post(
            url,
            json=payload,
            impersonate="chrome",
            headers={
                "Referer": "https://dune.com/browse/queries",
                "Content-Type": "application/json",
                # Sometimes an 'x-hasura-api-key' or similar is needed, 
                # but public queries should be open.
            },
            timeout=15
        )
        
        print(f"Detailed Status: {response.status_code}")
        print(f"Raw Response Preview: {response.text[:1000]}")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ JSON Parsed Successfully")
            print(json.dumps(data, indent=2)[:500])
            
    except Exception as e:
        print(f"💥 Error: {e}")

if __name__ == "__main__":
    test_graphql_bypass()
