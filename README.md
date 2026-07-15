# n8n-nodes-duckdb-quack

n8n community node for [DuckDB](https://duckdb.org/) — in-process OLAP with local files, in-memory databases, and the high-performance [Quack](https://duckdb.org/docs/current/quack/overview) remote protocol.

## Installation

1. In your n8n Docker compose, use the Debian image:
   ```yaml
   image: docker.n8n.io/n8nio/n8n:latest-debian
   ```
2. Install via **Settings → Community Nodes** → enter `n8n-nodes-duckdb-quack`

## Environment

Requires DuckDB ≥ v1.5.3. The `@duckdb/node-api` native module requires glibc (not Alpine/musl). The Debian-based n8n image is mandatory.

### Windows: Upgrading the package

Windows locks loaded native DLLs — upgrading the `@duckdb/node-api` native module while n8n is running will fail. If this happens, the node may appear as **corrupted** in the UI.

**To recover:** delete the corrupted node from **Settings → Community Nodes**, then reinstall it while n8n is stopped.

> **Strongly recommended:** test and develop this node on a Unix machine (Linux/macOS). Docker (`n8nio/n8n:latest-debian`) is the most reliable environment. Windows is supported but native module upgrades are fragile.

## Credentials

### Local Mode (File / In-Memory)

| Field | Description |
|-------|-------------|
| Connection Mode | `Local File / In-Memory RAM` |
| File Path | `:memory:` for volatile scratchpads, or an absolute path (e.g., `/data/analytics.db`) |
| Auto-Install Extensions | Optional comma-separated list (e.g., `spatial, fts`). Core extension `httpfs` is loaded automatically. |

Multiple credentials pointing to the same file path share a single DuckDB instance.

### Remote Mode (Quack Protocol)

| Field | Description |
|-------|-------------|
| Connection Mode | `Remote Quack Server (HTTP/2 Protocol)` |
| Remote Server URI | Quack server address (e.g., `quack:localhost:9494`, `quack:192.168.1.50:9494`) |
| Authentication Token | Server token (minimum 4 characters) |
| Disable SSL Encryption | Check for plain HTTP connections (localhost or trusted private subnets). Non-local URIs default to HTTPS. |
| Auto-Install Extensions | Optional comma-separated list |

> **Note:** Quack is currently in beta (DuckDB v1.5.3+). The protocol and function names are subject to change until DuckDB v2.0 (September 2026).

## Operations

### Table Resource

| Operation | Description | Local | Remote |
|-----------|-------------|-------|--------|
| List Columns | Inspect column names, types, and definitions | ✅ | ✅ |
| List Tables | Fetch all tables in the current catalog | ✅ | ✅ |
| Read Table | Stream records (JSON or Parquet output) | ✅ | ✅ |
| Write / Append Rows | Insert data (Append or Overwrite modes) | ✅ | ✅ |
| Update Rows | Modify records using SQL WHERE clause and SET column-value pairs | ✅ | ✅ |
| Delete Rows | Remove records matching a SQL WHERE condition (required safety guard) | ✅ | ✅ |

Remote write operations use batch SQL INSERT to avoid appender chunk limits over HTTP.

### Query Resource

| Operation | Description | Local | Remote |
|-----------|-------------|-------|--------|
| Select (Custom SQL) | Execute arbitrary SQL queries | ✅ | ✅ |
| Stateless Quack Query | Single round-trip query bypassing ATTACH | — | ✅ |
| Persist Memory to Disk | Save in-memory database to a `.db` file | ✅ | — |

## Usage Examples

### Local In-Memory

1. Create a credential: Connection Mode = `Local`, File Path = `:memory:`
2. Populate data with **Table → Write** (Overwrite mode, table name `employees`)
3. Query with **Table → Read** or **Query → Select (Custom SQL)** (`SELECT * FROM employees WHERE score > 90`)
4. Save with **Query → Persist Memory to Disk** → `/home/user/my_data.db`
5. Create a new credential pointing to `my_data.db` — the data is immediately available

Multiple credentials with `:memory:` share the same database instance, just like multiple credentials pointing to the same file.

### Remote Quack Server

**Option 1: WSL2 (recommended for Windows development)**

1. In a WSL2 terminal, install DuckDB CLI:
   ```bash
   curl https://install.duckdb.org | sh
   export PATH="$HOME/.duckdb/cli/latest:$PATH"
   ```
2. Start the server:
   ```sql
   duckdb
   INSTALL quack;
   LOAD quack;
   CREATE TABLE products AS SELECT * FROM (VALUES (1, 'Widget', 9.99), (2, 'Gadget', 24.50)) t(id, name, price);
   CALL quack_serve('quack:0.0.0.0:9494', token='my_token', allow_other_hostname:=true);
   ```
3. WSL2 auto-forwards `localhost` — use `quack:localhost:9494` in n8n with Disable SSL checked

**Option 2: Windows (DuckDB CLI)**

1. Install the DuckDB CLI:
   ```powershell
   winget install DuckDB.cli
   ```
2. Start an interactive DuckDB session:
   ```powershell
   duckdb
   ```
3. In the DuckDB shell, start the Quack server:
   ```sql
   INSTALL quack;
   LOAD quack;
   CALL quack_serve('quack:localhost:9494', token='my_token', allow_other_hostname:=true);
   ```
4. Keep the terminal open. In n8n, use credential `quack:localhost:9494` with Disable SSL checked and token `my_token`.

### Persist Memory to File

1. Build data in `:memory:` with Write operations
2. Use **Query → Persist Memory to Disk** with a `.db` file path
3. The file is created as a fully functional DuckDB database — all tables are copied with their original types
4. The `.db` file is immediately unlocked after the operation completes and can be opened by other processes
5. Create a new Local credential pointing to that file to access it later

## Architecture

### Connection Model

| Mode | DuckDB Instance | Caching |
|------|----------------|---------|
| Local `:memory:` | Single shared instance per n8n process | ✅ Keyed by path |
| Local file | Instance per file path | ✅ Keyed by path |
| Remote Quack | `:memory:` instance per server host | ✅ Keyed by host |

Extensions are loaded once per instance and cached — subsequent executions skip the load step.

### Remote Query Strategy

The node uses two approaches for remote Quack queries:

| Approach | Used For | Why |
|----------|----------|-----|
| `quack_query()` | Reads (List, Read, Select, Stateless) | Single round-trip, no streaming conflicts |
| `ATTACH` + session | Writes, Persist | Needs multi-statement sessions |

This avoids the "Multiple streaming scans" Quack protocol limitation.

## Features

### Write Mode: Column Types

Tables created by **Write / Append Rows** default to `VARCHAR`. When **Write Mode** is **Overwrite / Recreate**, a **Column Types** option appears:

| Option | Behavior |
|--------|----------|
| **VARCHAR** | All columns as text — safe, preserves everything |
| **Auto-Detect** | DuckDB infers: `95` → `INTEGER`, `95.5` → `DOUBLE`, `"2024-01-15"` → `DATE`, `true` → `BOOLEAN` |

Auto-Detect uses `CREATE OR REPLACE TABLE ... AS SELECT * FROM (VALUES ...)` for server-side type inference.

### Multi-Statement SQL

The **Select (Custom SQL)** operation supports multi-statement blocks — DDL, DML, and PRAGMA execute before the final SELECT:

```sql
DROP TABLE IF EXISTS employees;
CREATE TABLE employees (id INTEGER, name VARCHAR);
INSERT INTO employees VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM employees ORDER BY id;
```

Intermediate statement failures (e.g., index already exists) are silently skipped — the final SELECT always runs.

### Extension Loading

Core extension `httpfs` loads automatically. Additional extensions via comma-separated list: `spatial, fts, sqlite_scanner`. Community repo syntax: `gsheets FROM community`. DuckDB's JIT autoload is enabled by default.

## License

MIT
