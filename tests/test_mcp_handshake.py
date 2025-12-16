import subprocess
import json
import sys
import os

def test_handshake():
    # Path to your main.py
    server_script = os.path.join("src", "main.py")
    
    # Start the MCP server process
    print(f"🔌 Starting server: uv run {server_script} ...")
    process = subprocess.Popen(
        ["uv", "run", server_script],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=0 # Unbuffered
    )

    # JSON-RPC 2.0 Initialize Request
    init_request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "test-client",
                "version": "1.0"
            }
        }
    }

    print("📤 Sending 'initialize' request...")
    
    # Write to the server's stdin
    json_str = json.dumps(init_request)
    process.stdin.write(json_str + "\n")
    process.stdin.flush()

    # Read from the server's stdout
    print("cw Waiting for response...")
    response_line = process.stdout.readline()
    
    if response_line:
        print("\n✅ Server Responded:")
        try:
            resp = json.loads(response_line)
            print(json.dumps(resp, indent=2))
        except json.JSONDecodeError:
            print(f"Received non-JSON: {response_line}")
    else:
        print("\n❌ No response received.")
        print("Stderr content:")
        print(process.stderr.read())

    # Clean up
    process.terminate()

if __name__ == "__main__":
    test_handshake()
