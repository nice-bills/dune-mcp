from mcp.server.fastmcp import FastMCP

mcp = FastMCP("Simple Server")

@mcp.tool()
def hello() -> str:
    return "Hello world"

if __name__ == "__main__":
    mcp.run()
