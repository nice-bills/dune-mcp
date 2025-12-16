import subprocess
import json
import sys
import os

def test_tool_search():
    server_script = os.path.join("src", "main.py")
    
    print(f"🔌 Starting server: uv run {server_script} ...")
    process = subprocess.Popen(
        ["uv", "run", server_script],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=0
    )

    def send_request(req):
        json_str = json.dumps(req)
        process.stdin.write(json_str + "\n")
        process.stdin.flush()

    def read_response():
        line = process.stdout.readline()
        if not line:
            return None
        return json.loads(line)

    # 1. Initialize
    print("Step 1: Handshake...")
    send_request({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "1.0"}
        }
    })
    read_response() # Skip init response
    
    # 2. Notify Initialized
    send_request({
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
    })

    # 3. Call Tool
    print("Step 2: Calling 'search_public_queries' with query='dex'வுகளை...")
    send_request({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "search_public_queries",
            "arguments": {
                "query": "dex"
            }
        }
    })

    # 4. Read Result
    response = read_response()
    
    if response and "result" in response:
        print("\n✅ Tool Result:")
        content = response["result"].get("content", [])
        for item in content:
            print(item["text"])
    elif response and "error" in response:
        print(f"\n❌ Tool Error: {response['error']}")
    else:
        print(f"\n❌ Unknown Response: {response}")

    process.terminate()

if __name__ == "__main__":
    test_tool_search()
