# Dune MCP (Model Context Protocol)

A **defensive, token-aware** MCP server for Dune Analytics.

This project enables LLMs (like Claude, or custom agents) to securely interact with Dune Analytics. It acts as a "Smart Gateway" that prioritizes **Query Reuse** and **Budget Safety** over raw SQL generation, protecting your API credits and reducing token consumption.

## 🚀 Quick Start

### Option 1: Install via PyPI (Recommended)

1.  **Install:**
    ```bash
    # The easiest way to manage tools
    uv tool install dune-mcp
    
    # OR using pip
    pip install dune-mcp
    ```

2.  **Configure Claude Desktop:**
    Add this to your `claude_desktop_config.json`:
    
    ```json
    {
      "mcpServers": {
        "dune": {
          "command": "dune-mcp",
          "env": {
            "DUNE_API_KEY": "your_api_key",
            "DUNE_USER_HANDLE": "your_username",
            "GITHUB_TOKEN": "optional_github_token"
          }
        }
      }
    }
    ```

### Option 2: Run from Source

1.  **Clone & Setup:**
    ```bash
    git clone https://github.com/nice-bills/dune-mcp.git
    cd dune-mcp
    uv sync
    cp .env.example .env
    # Edit .env with your keys
    ```

2.  **Configure Claude Desktop:**
    ```json
    {
      "mcpServers": {
        "dune": {
          "command": "uv",
          "args": ["run", "src/main.py"],
          "cwd": "/absolute/path/to/dune-mcp"
        }
      }
    }
    ```

## Usage

Once connected, you can ask Claude things like:
*   "Find queries about Uniswap volume on Base."
*   "List my recent queries."
*   "Execute query ID 12345."
*   "Analyze the results of the last query."

### Zero-Credit Schema Discovery

*   **Google-like Search:** Reverse-engineered GraphQL integration allows searching public queries by keyword (e.g., "uniswap volume").
*   **Portfolio Browsing:** List queries by user handle to access your own or others' work.
*   **Budget Manager:** Deterministic guards that prevent credit exhaustion.
*   **Token-Optimized:** Returns "Indices" (summaries) instead of raw schemas. Results are previewed (top 5 rows), not streamed in full.
*   **Query Reuse First:** Tools encourage searching existing community queries before generating new SQL.
*   **CSV Export:** "Escape hatch" to download full datasets to disk instead of flooding the LLM context.

## Toolset

1.  `search_public_queries(query)`: Search existing queries by keyword (free & fast).
2.  `list_user_queries(handle, limit)`: List queries by user (e.g., "bils").
3.  `search_spellbook(keyword)`: Search the official Dune Spellbook GitHub repo for tables (e.g., "uniswap").
4.  `get_spellbook_file_content(path)`: View the SQL or schema of a Spellbook file.
5.  `get_query_details(query_id)`: Inspect SQL and parameters (on demand).
6.  `get_table_schema(table_name)`: Get columns for a specific table (Costs Credits).
7.  `execute_query(query_id)`: Run a query (async, budget-checked).
8.  `get_job_status(job_id)`: Poll for completion.
9.  `get_job_results_summary(job_id)`: Get a lightweight preview (5 rows + stats).
10. `export_results_to_csv(job_id)`: Download the full dataset.
11. `analyze_results(job_id)`: Detect outliers and trends in data.
12. `analyze_query_error(error_message, query_sql)`: Get AI-driven fix suggestions for failed queries.

## Installation

This project uses `uv` for fast package management.

```bash
# 1. Clone the repo
git clone https://github.com/nice-bills/dune-mcp.git
cd dune-mcp

# 2. Setup config
cp .env.example .env
# Edit .env and add your DUNE_API_KEY
```

### Configuration
Add your handle to `.env` to allow the MCP to auto-detect your queries:
```bash
DUNE_API_KEY=your_key
DUNE_USER_HANDLE=your_username # Optional
```

## Usage

### Option 1: Claude Desktop
Add this to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "dune": {
      "command": "uv",
      "args": ["run", "-m", "src.main", "--directory", "/path/to/dune-mcp"]
    }
  }
}
```

### Option 2: MCP Inspector (Web UI)
Test the tools interactively in your browser.

```bash
npx @modelcontextprotocol/inspector uv run -m src.main
```

## Best Practices

### Zero-Credit Schema Discovery
To find table names or column structures without consuming Dune credits:
1.  **Use `search_spellbook("keyword")`** to find official Dune Spellbook models (SQL files) and schema definitions (`schema.yml`) that match your topic.
2.  **Use `get_spellbook_file_content("path/to/file.sql")`** to view the SQL or schema definition. This directly gives you the table name and its structure.
3.  **Alternatively, use `search_public_queries("topic")`** to find existing queries on the topic.
4.  **Use `get_query_details(id)`** to inspect their SQL.
5.  Extract the table names (e.g., `uniswap_v3_ethereum.Factory_evt_PoolCreated`) from the SQL.

This "Rosetta Stone" approach is faster, safer, and cheaper than blindly querying the schema.

## Safety Principles

1.  **Never stream raw data:** 100k rows = Token Death. We stream previews + stats.
2.  **Two-Phase Reasoning:** Plan (Search/Estimate) → Execute.
3.  **MCP Does the Boring Work:** We calculate min/max/avg in Python, not the LLM.

## License
MIT