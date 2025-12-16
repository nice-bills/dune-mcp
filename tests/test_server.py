import asyncio
import os
import sys
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

async def run_test():
    # Define how to run the server
    server_params = StdioServerParameters(
        command="uv",
        args=["run", "src/main.py"],
        env=os.environ.copy() # Pass current env (contains API keys if set)
    )

    print(f"Starting server: {server_params.command} {' '.join(server_params.args)}")

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            # 1. Initialize
            await session.initialize()
            print("\nServer Initialized")

            # 2. List Tools
            tools = await session.list_tools()
            print(f"\nAvailable Tools ({len(tools.tools)}):")
            for tool in tools.tools:
                print(f"  - {tool.name}: {tool.description[:60]}...")

            # 3. Call 'get_account_status' (Real API Check)
            print("\nTesting Tool: get_account_status...")
            try:
                result = await session.call_tool("get_account_status")
                print(f"   Result: {result.content[0].text}")
            except Exception as e:
                print(f"   Error calling tool: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(run_test())
        print("\nTest completed successfully!")
    except Exception as e:
        print(f"\nTest Failed: {e}")