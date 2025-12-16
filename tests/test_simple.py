import asyncio
import os
import sys
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def run_test():
    server_params = StdioServerParameters(
        command="uv",
        args=["run", "tests/simple_server.py"],
        env=os.environ.copy()
    )

    print("Starting simple server...")
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            print("Initialized!")
            tools = await session.list_tools()
            print(f"Tools: {[t.name for t in tools.tools]}")

if __name__ == "__main__":
    asyncio.run(run_test())
