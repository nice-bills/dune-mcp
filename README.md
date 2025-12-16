# Dune MCP (Model Context Protocol)

A **defensive, token-aware** MCP server for Dune Analytics.

This project enables LLMs (like Claude, or custom agents) to securely interact with Dune Analytics. It acts as a "Smart Gateway" that prioritizes **Query Reuse** and **Budget Safety** over raw SQL generation, protecting your API credits and reducing token consumption.

## Features

*   **Budget Manager:** Deterministic guards that prevent credit exhaustion. Set hard limits on queries per session.
*   **Token-Optimized:** Returns "Indices" (summaries) instead of raw schemas. Results are previewed (top 5 rows), not streamed in full.
*   **Query Reuse First:** Tools encourage searching existing community queries before generating new SQL.
*   **Aggressive Caching:** In-memory caching for schemas and query metadata to reduce API latency.
*   **CSV Export:** "Escape hatch" to download full datasets to disk instead of flooding the LLM context.

## Toolset

1.  `search_public_queries`: Find existing queries (saves credits).
2.  `get_query_details`: Inspect SQL and parameters (on demand).
3.  `execute_query`: Run a query (async, budget-checked).
4.  `get_job_status`: Poll for completion.
5.  `get_job_results_summary`: Get a lightweight preview (5 rows + stats).
6.  `export_results_to_csv`: Download the full dataset.
7.  `get_account_status`: Check remaining credits.
8.  `get_session_budget`: Check session-specific safety limits.

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

## Usage

### Option 1: MCP Inspector (Web UI)
Test the tools interactively in your browser.

```bash
npx @modelcontextprotocol/inspector uv run -m src.main
```

### Option 2: Claude Desktop
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

## Architecture

*   **Language:** Python 3.12+
*   **SDKs:** `mcp` (Official Python SDK), `dune-client`, `pandas`
*   **Structure:**
    *   `src/tools`: Tool definitions
    *   `src/services`: Business logic (Budget, Cache, Dune API)
    *   `tests`: Sanity checks

## Safety Principles

1.  **Never stream raw data:** 100k rows = Token Death. We stream previews + stats.
2.  **Two-Phase Reasoning:** Plan (Search/Estimate) → Execute.
3.  **MCP Does the Boring Work:** We calculate min/max/avg in Python, not the LLM.

## License
MIT
