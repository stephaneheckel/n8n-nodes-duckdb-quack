# The DuckDB & Quack n8n node

[DuckDB](https://duckdb.org/) is an in-process SQL OLAP database management system. It runs embedded directly within the host application — no separate server process needed — and supports the full SQL standard with advanced analytical functions, windowing, and direct querying of Parquet, CSV, and JSON files. Often described as "SQLite for analytics," it is designed for high-performance analytical workloads on datasets ranging from megabytes to hundreds of gigabytes.

[Quack](https://duckdb.org/docs/current/quack/overview) is DuckDB's native remote-access protocol, built on HTTP/2 with gRPC streaming. It allows a DuckDB server to expose in-memory databases over the network, enabling multiple clients to query, write, and manage data through a high-performance binary protocol. Quack is currently in beta (DuckDB v1.5.3+) and is evolving rapidly — the protocol and function names are subject to change until the stable release in DuckDB v2.0.

The `n8n-nodes-duckdb-quack` repository is a community-built custom node for the n8n automation platform. It integrates DuckDB — a high-performance, in-process analytical database engine — directly into n8n workflows.

### High-Level Features

- **In-Workflow Analytical Querying:** It allows n8n workflows to execute fast, analytical SQL queries directly on tabular data, acting as an embedded, serverless database node.

- **Large Dataset Aggregation & Transformation:** It enables advanced data manipulation (like complex aggregations, joining distinct datasets, and filtering) that might otherwise hit memory limits or run too slowly using standard JavaScript-based n8n nodes.

- **Flexible Storage & Connection Modes:** The node allows you to connect to DuckDB in three distinct ways depending on your architecture:

  - **In-Memory (`:memory:`):** Perfect for transient data processing. It spins up a blazing-fast database entirely in RAM to query, join, or filter your workflow's incoming data, then wipes clean when the execution finishes.

  - **Physical File (`.db`):** Connects directly to a physical database file stored on your server or local volume. This allows you to persist data across workflow executions, append new logs, or query an existing persistent datastore.

  - **Quack (Remote):** Connects remotely to manage and process data outside the immediate local file environment.

- **Native Node.js Bindings:** The node utilizes native DuckDB API bindings (`@duckdb/node-api`), ensuring highly efficient execution and performance within the Node.js runtime environment of n8n. Because it relies on native glibc compiling, the node is typically designed to work seamlessly with n8n deployments running on Debian-based Docker containers (`:latest-debian`).

**This node has been tested on the following configurations:**

| Platform | n8n Installation | DuckDB Runtime |
|----------|-----------------|----------------|
| Hostinger | Coolify (n8n standard image) | Docker container `docker-compose.yml` |
| Windows 11 | npm (local installation) | WSL2 (DuckDB CLI) |
| Windows 11 | npm (local installation) | DuckDB 1.5.4 |

> **Strongly recommended:** Unix machine (Linux/macOS). Windows is supported but native module upgrades are fragile as Windows locks loaded native DLLs 

## Installation

> **⚠️ Work in Progress** — this community node is provided "as is." APIs, features, and behavior may change. We welcome feedback and contributions.

1. Install the node via n8n menu **Settings → Community Nodes** → enter `n8n-nodes-duckdb-quack`

## Environment

- Requires DuckDB ≥ v1.5.4. 
- The `@duckdb/node-api` native module requires glibc. The Debian-based n8n is required (not Alpine/musl). However, the node has been successfully tested on standard n8n installation.
   ```yaml
   image: docker.n8n.io/n8nio/n8n:latest-debian
   ```

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

> **Note:** Quack is currently in beta (DuckDB v1.5.4+). The protocol and function names are subject to change until DuckDB v2.0 (September 2026?).

## Operations

### Table Resource

| Operation | Description | Local | Remote |
|-----------|-------------|-------|--------|
| List Columns | Inspect column names, types, and definitions | ✅ | ✅ |
| List Tables | Fetch all tables in the current catalog | ✅ | ✅ |
| Read Table | Stream records (JSON, CSV or Parquet output) | ✅ | ✅ |
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

### Server Resource

| Operation | Description | Local | Remote |
|-----------|-------------|-------|--------|
| Get Server Info | Retrieve version, uptime, and configuration | — | ✅ |
| List Sessions | Active sessions with IDs and connected databases | — | ✅ |

## Usage Examples

### Local In-Memory

1. Create a credential: Connection Mode = `Local`, File Path = `:memory:`
2. Populate data with **Table → Write** (Overwrite mode, table name `employees`)
3. Query with **Table → Read** or **Query → Select (Custom SQL)** (`SELECT * FROM employees WHERE score > 90`)
4. Save with **Query → Persist Memory to Disk** → `/shared/my_backup.db`
5. Create a new credential pointing to `/shared/my_backup.db` — the data is immediately available

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

**Option 3: Docker Compose (Tested via Coolify)**

The repository includes a self-contained `docker-compose.yml` that starts a persistent Quack server with automatic `.db` file discovery.

1. In your **Coolify** dashboard, add a new service and use `docker-compose.yml` directly for your compose file. Redeploys do not destroy data — volumes persist across restarts.

2. In n8n, create a Remote Quack credential:
   - **Remote Server URI:** `quack:<server-ip>:9494` (use `quack:localhost:9494` if n8n runs on the same host)
   - **Authentication Token:** the value of `QUACK_TOKEN` 
   - **Disable SSL Encryption:** check this (direct HTTP/2, no TLS)

### Shared Directory Between n8n and DuckDB Containers

If n8n and the DuckDB Quack server run on the same host (separate Docker containers), use a shared bind mount so both containers access the same `.db` files. The n8n node writes persist/export files, and the DuckDB server can `ATTACH` them — both see the same directory.

**1. Create the shared directory on the host:**

```bash
sudo mkdir -p /data/shared-duckdb
sudo chown -R 1000:1000 /data/shared-duckdb
sudo chmod 775 /data/shared-duckdb
```

UID/GID `1000:1000` matches the `node` user in both the n8n and `node:20-slim` images.

**2. Mount it in the n8n compose file:**

```yaml
services:
  n8n:
    volumes:
      - '/data/shared-duckdb:/shared'
```

**3. Mount it in the DuckDB compose file:**

```yaml
services:
  duckdb-server:
    volumes:
      - '/data/shared-duckdb:/shared'
    environment:
      - DB_DIR=/shared
```

The DuckDB compose file included in this repo uses `/shared` out of the box. The container also runs `chown -R node:node /shared` at startup to ensure the server process can write, then launches via `su node`.

**4. In your n8n workflows:**

Use `/shared/` as the base path for all file operations:

| Operation | Field | Example |
|-----------|-------|---------|
| Persist Memory to Disk | Target Disk Path | `/shared/my_backup.db` |
| Read Table (Parquet/CSV) | File Path | `/shared/export.parquet` |
| Select (Custom SQL) | File Path | `/shared/query_result.csv` |


| Path in SQL | Physical location | Survives redeploy? |
|-------------|-------------------|--------------------|
| `/shared/*.db` | Host directory `/data/shared-duckdb/` | ✅ Yes — independent of both containers |
| `:memory:` | Container RAM only | ❌ Lost on restart |

Any `.db` file in `/shared/` is listed in the server logs at startup.

### Monitoring & Resource Limits

The compose file includes sensible defaults:

| Setting | Value |
|---------|-------|
| Memory limit | 1 GB (hard cap) |
| Log rotation | 10 MB × 3 files (30 MB max) |
| Healthcheck | TCP probe every 30s on port 9494 |

To check live resource usage:

```bash
docker stats duckdb-quack-server
```

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

Intermediate statement failures (e.g., index already exists) are displayed in the console.

### Extension Loading

Core extension `httpfs` loads automatically. Additional extensions via comma-separated list: `spatial, fts, sqlite_scanner`. Community repo syntax: `gsheets FROM community`. DuckDB's JIT autoload is enabled by default.

## License

MIT
