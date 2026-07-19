import {
  IExecuteFunctions,
  INodeExecutionData,
  INodeType,
  INodeTypeDescription,
  ILoadOptionsFunctions,
  INodePropertyOptions,
  NodeApiError,
  NodeConnectionTypes,
  NodeOperationError,
} from "n8n-workflow";
import type { INode, Icon, IDataObject, JsonObject } from "n8n-workflow";
import { DuckDBInstance } from "@duckdb/node-api";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

// ---------------------------------------------------------------------------
// Instance cache: lives for the process lifetime (n8n's runtime).
// Growth is bounded by design — all remote Quack connections share one
// :memory: instance (quack_ prefix stripped), so only local .db file paths
// create separate instances (typically 1-3 in a real workflow).
// No eviction needed; adding it would break loadOptions dropdowns that share
// this cache and can't coordinate with the editor lifecycle.
//
// Uses a Promise cache to avoid TOCTOU races: concurrent callers for the
// same key share the in-flight DuckDBInstance.create() promise instead of
// racing to open the same file (which would fail on Windows due to exclusive
// file locking).
// ---------------------------------------------------------------------------
const instanceCache = new Map<string, DuckDBInstance>();
const instancePromises = new Map<string, Promise<DuckDBInstance>>();

async function getOrCreateInstance(
  key: string,
  path: string,
): Promise<DuckDBInstance> {
  const dbPath = path.startsWith("quack_") ? ":memory:" : path;

  const entry = instanceCache.get(key);
  if (entry) {
    // If file-backed and the file was deleted on disk, evict the stale cache.
    // :memory: and Quack instances are network-attached — never evicted.
    const isFileBacked =
      dbPath !== ":memory:" && !dbPath.startsWith("quack_");
    if (isFileBacked && !fs.existsSync(dbPath)) {
      try {
        entry.closeSync();
      } catch (_e) {
        /* already closed */
      }
      instanceCache.delete(key);
      // fall through to create fresh instance
    } else {
      return entry;
    }
  }

  const inFlight = instancePromises.get(key);
  if (inFlight) return inFlight;

  const promise = DuckDBInstance.create(dbPath).then((inst) => {
    instanceCache.set(key, inst);
    instancePromises.delete(key);
    return inst;
  });
  instancePromises.set(key, promise);
  return promise;
}

// ---------------------------------------------------------------------------
// SQL injection guard — whitelist pattern: only valid SQL identifier characters.
// Allows schema-qualified names (e.g., main.employees, target_db.main.orders) but
// blocks subqueries, semicolons, and any injection payload.
// ---------------------------------------------------------------------------
const VALID_TABLE_NAME = /^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$/;

function validateTableName(
  name: string,
  node: INode,
  itemIndex: number,
): string {
  const trimmed = name.trim();
  if (!trimmed) return trimmed;
  if (!VALID_TABLE_NAME.test(trimmed)) {
    throw new NodeOperationError(
      node,
      `Invalid identifier "${trimmed}". Use only letters, digits, underscores, and dots (e.g., employees or main.employees).`,
      { itemIndex },
    );
  }
  return trimmed;
}

// ---------------------------------------------------------------------------
// Extensions that are ALWAYS loaded (core infrastructure).
// parquet & json are built-in since DuckDB 1.0 — only httpfs is needed.
// ---------------------------------------------------------------------------
const CORE_EXTENSIONS = ["httpfs"];

// Parse an extension spec like "gsheets FROM community" into { name, installClause }
function parseExtensionSpec(spec: string): { name: string; install: string } {
  const trimmed = spec.trim();
  const parts = trimmed.split(/\s+/);
  const name = parts[0];
  return { name, install: trimmed };
}

// Track which credential instances have had extensions loaded
const loadedInstances = new Set<string>();

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------
export class DuckDbQuack implements INodeType {
  // Serialize loadOptions calls to avoid concurrent Quack connections
  private static _loadLock: Promise<void> = Promise.resolve();

  methods = {
    loadOptions: {
      async getTables(
        this: ILoadOptionsFunctions,
      ): Promise<INodePropertyOptions[]> {
        const run = async () => {
          const credentials = await this.getCredentials("duckDbQuackApi");
          const instancePath =
            credentials.connectionType === "local"
              ? (credentials.filePath as string) || ":memory:"
              : `quack_${credentials.host || "localhost"}`;
          const cacheKey = instancePath;
          const instance = await getOrCreateInstance(cacheKey, instancePath);
          const connection = await instance.connect();
          try {
            if (credentials.connectionType === "remote") {
              const host =
                (credentials.host as string) || "quack:localhost:9494";
              const token = credentials.token as string;
              const disableSsl = credentials.disableSsl as boolean;
              const tokenArg = token
                ? `, token := '${token.replace(/'/g, "''")}'`
                : "";
              const sslArg = disableSsl ? ", disable_ssl := true" : "";
              const result = await connection.runAndReadAll(
                `FROM quack_query('${host.replace(/'/g, "''")}', 'SHOW ALL TABLES'${tokenArg}${sslArg});`,
              );
              const rows = result.getRowObjectsJson();
              return rows.map((row: Record<string, unknown>) => ({
                name: `[${row.schema}] ${row.name}${row.temporary ? " (Temp)" : ""}`,
                value: `${row.schema}.${row.name}`,
              }));
            }
            // Local: tables live in default database — no ATTACH needed
            const result = await connection.runAndReadAll("SHOW ALL TABLES;");
            const rows = result.getRowObjectsJson();
            return rows.map((row: Record<string, unknown>) => ({
              name: `[${row.schema}] ${row.name}${row.temporary ? " (Temp)" : ""}`,
              value: `${row.schema}.${row.name}`,
            }));
          } catch (error) {
            return [
              {
                name: `Error fetching metadata: ${(error as Error).message}`,
                value: "error",
              },
            ];
          } finally {
            connection.closeSync();
          }
        };
        // Serialize: ensure only one loadOptions call runs at a time
        const prev = DuckDbQuack._loadLock;
        let release!: () => void;
        DuckDbQuack._loadLock = new Promise<void>((r) => {
          release = r;
        });
        await prev;
        try {
          return await run();
        } finally {
          release();
        }
      },
    },
  };

  description: INodeTypeDescription = {
    displayName: "DuckDB Client",
    name: "duckDbQuack",
    icon: { light: "file:duckdb.svg", dark: "file:duckdb.svg" } as Icon,
    group: ["transform"],
    version: 1,
    subtitle: '={{$parameter["operation"] + ": " + $parameter["resource"]}}',
    description:
      "Open, Read, Write, and Query data using local DuckDB files or high-speed Quack remote endpoints",
    defaults: { name: "DuckDB Engine" },
    inputs: [NodeConnectionTypes.Main],
    outputs: [NodeConnectionTypes.Main],
    usableAsTool: true,
    credentials: [{ name: "duckDbQuackApi", required: true }],
    properties: [
      // --- RESOURCE ---
      {
        displayName: "Resource",
        name: "resource",
        type: "options",
        noDataExpression: true,
        options: [
          { name: "Query / Administration", value: "query" },
          { name: "Server", value: "server" },
          { name: "Table", value: "table" },
        ],
        default: "table",
      },

      // ======================== TABLE ========================
      {
        displayName: "Operation",
        name: "operation",
        type: "options",
        displayOptions: { show: { resource: ["table"] } },
        noDataExpression: true,
        options: [
          {
            name: "List Columns",
            value: "listColumns",
            action: "List columns of a table",
            description: "Inspect structural properties and column definitions",
          },
          {
            name: "List Tables",
            value: "listTables",
            action: "List tables in a database",
            description: "Fetch all tables visible in the current catalog",
          },
          {
            name: "Read Table",
            value: "read",
            action: "Read rows from a table",
            description: "Stream records from an active table",
          },
          {
            name: "Write / Append Rows",
            value: "write",
            action: 'Write append rows to a table',
            description: "Insert or map incoming data rows into a table",
          },
          {
            name: "Update Rows",
            value: "update",
            action: "Update rows in a table",
            description:
              "Modify records using SQL WHERE clause and SET column-value pairs",
          },
          {
            name: "Delete Rows",
            value: "delete",
            action: "Delete rows from a table",
            description: "Remove records matched by a WHERE condition",
          },
        ],
        default: "read",
      },

      // ======================== SERVER ========================
      {
        displayName: "Operation",
        name: "operation",
        type: "options",
        displayOptions: { show: { resource: ["server"] } },
        noDataExpression: true,
        options: [
          {
            name: "Get Server Info",
            value: "serverInfo",
            action: "Get server info",
            description: "Retrieve version, uptime, and configuration from a Quack server",
          },
          {
            name: "List Sessions",
            value: "listSessions",
            action: "List server sessions",
            description: "Retrieve active connections with IDs, queries, and states",
          },
        ],
        default: "serverInfo",
      },
      {
        displayName: "Table Name or ID",
        name: "tableName",
        type: "options",
        typeOptions: { loadOptionsMethod: "getTables" },
        displayOptions: {
          show: {
            resource: ["table"],
            operation: ["listColumns", "read", "update", "delete"],
          },
        },
        default: "",
        required: true,
        description:
          'Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>',
      },
      {
        displayName: "Table Name",
        name: "tableName",
        type: "string",
        displayOptions: {
          show: { resource: ["table"], operation: ["write"] },
        },
        default: "",
        required: true,
        placeholder: "my_table",
        description:
          "Name of the table to write to (e.g., employees). For in-memory databases, use just the table name. The table is auto-created if missing.",
      },
      {
        displayName: "Filter (WHERE Clause)",
        name: "whereClause",
        type: "string",
        displayOptions: { show: { resource: ["table"], operation: ["read"] } },
        default: "",
        placeholder: "score > 80 AND name LIKE 'A%'",
        description:
          "SQL WHERE conditions applied at the database level before data reaches n8n",
      },
      {
        displayName: "Limit",
        name: "limit",
        type: "number",
								typeOptions: {
									minValue: 1,
								},
        displayOptions: { show: { resource: ["table"], operation: ["read"] } },
        default: 50,
        placeholder: "100",
        description: 'Max number of results to return',
      },
      {
        displayName: "Output Format",
        name: "outputFormat",
        type: "options",
        displayOptions: { show: { resource: ["table"], operation: ["read"] } },
        options: [
          { name: "Parquet File (Binary)", value: "parquet" },
          { name: "CSV File", value: "csv" },
          { name: "Standard JSON Array", value: "json" },
        ],
        default: "json",
      },
      {
        displayName: "File Path",
        name: "filePath",
        type: "string",
        displayOptions: {
          show: {
            resource: ["table"],
            operation: ["read"],
            outputFormat: ["parquet", "csv"],
          },
        },
        default: "",
        placeholder: "/data/export.parquet",
        description:
          "Absolute path to write the output file. When set, data is streamed directly to disk. Leave empty to return the file as binary data (Parquet) or inline rows (JSON). Required for CSV.",
      },
      {
        displayName: "Include Header Row",
        name: "csvHeader",
        type: "boolean",
        displayOptions: {
          show: {
            resource: ["table"],
            operation: ["read"],
            outputFormat: ["csv"],
          },
        },
        default: true,
        description: "Whether to write column names as the first row of the CSV file",
      },
      {
        displayName: "Write Mode",
        name: "writeMode",
        type: "options",
        displayOptions: { show: { resource: ["table"], operation: ["write"] } },
        options: [
          { name: "Append Rows", value: "append" },
          { name: "Overwrite / Recreate", value: "overwrite" },
        ],
        default: "append",
      },
      {
        displayName: "Column Types",
        name: "columnTypes",
        type: "options",
        displayOptions: {
          show: {
            resource: ["table"],
            operation: ["write"],
            writeMode: ["overwrite"],
          },
        },
        options: [
          {
            name: 'VARCHAR (Safe — All Types Preserved as Text)',
            value: "varchar",
          },
          {
            name: 'Auto-Detect (Infer INT, DOUBLE, DATE From Data)',
            value: "auto",
          },
        ],
        default: "varchar",
        description:
          "VARCHAR preserves everything as text. Auto-detect uses DuckDB type inference on VALUES for proper integers, floats, and dates.",
      },

      {
        displayName: "Filter (WHERE Clause)",
        name: "updateWhereClause",
        type: "string",
        displayOptions: {
          show: { resource: ["table"], operation: ["update"] },
        },
        default: "",
        required: true,
        placeholder: 'ID=42 OR status=\'pending\'',
        description:
          "SQL WHERE conditions. Required as a safety guard — empty WHERE = no update.",
      },
      {
        displayName: "Set Columns",
        name: "setColumns",
        type: "string",
        displayOptions: {
          show: { resource: ["table"], operation: ["update"] },
        },
        default: "",
        required: true,
        placeholder: "status='done', score=95",
        description:
          "Comma-separated column=value pairs (e.g. \"status='done', score=95\"). String values need single quotes.",
      },
      {
        displayName: "Filter (WHERE Clause)",
        name: "deleteWhereClause",
        type: "string",
        displayOptions: {
          show: { resource: ["table"], operation: ["delete"] },
        },
        default: "",
        required: true,
        placeholder: 'ID=1 OR status=\'inactive\'',
        description:
          "SQL WHERE conditions. Required as a safety guard — empty WHERE = no delete.",
      },

      // ======================== QUERY ========================
      {
        displayName: "Operation",
        name: "operation",
        type: "options",
        displayOptions: { show: { resource: ["query"] } },
        noDataExpression: true,
        options: [
          {
            name: "Persist Memory to Disk",
            value: "persist",
            action: "Persist memory to disk",
            description:
              "Snapshot transient :memory: state to a file-backed database",
          },
          {
            name: "Select (Custom SQL)",
            value: "select",
            action: "Execute custom SQL query",
            description: "Execute raw multi-line SQL queries",
          },
          {
            name: "Stateless Quack Query",
            value: "stateless",
            action: 'Stateless quack query',
            description:
              "Single round-trip query bypassing ATTACH (remote only)",
          },
        ],
        default: "select",
      },
      {
        displayName: "SQL Query",
        name: "sqlQuery",
        type: "string",
        typeOptions: { alwaysOpenEditWindow: true },
        displayOptions: {
          show: { resource: ["query"], operation: ["select", "stateless"] },
        },
        default: "SELECT * FROM target_db.main.orders LIMIT 100;",
        required: true,
      },
      {
        displayName: "Output Format",
        name: "queryOutputFormat",
        type: "options",
        displayOptions: {
          show: { resource: ["query"], operation: ["select"] },
        },
        options: [
          { name: "Parquet File (Binary)", value: "parquet" },
          { name: "CSV File", value: "csv" },
          { name: "Standard JSON Array", value: "json" },
        ],
        default: "json",
      },
      {
        displayName: "File Path",
        name: "queryFilePath",
        type: "string",
        displayOptions: {
          show: {
            resource: ["query"],
            operation: ["select"],
            queryOutputFormat: ["parquet", "csv"],
          },
        },
        default: "",
        placeholder: "/data/export.parquet",
        description:
          "Absolute path to write the output file. When set, data is streamed directly to disk. Leave empty to return the file as binary data (Parquet) or inline rows (JSON). Required for CSV.",
      },
      {
        displayName: "Include Header Row",
        name: "queryCsvHeader",
        type: "boolean",
        displayOptions: {
          show: {
            resource: ["query"],
            operation: ["select"],
            queryOutputFormat: ["csv"],
          },
        },
        default: true,
        description: "Whether to write column names as the first row of the CSV file",
      },
      {
        displayName: "Target Disk Path",
        name: "targetDiskPath",
        type: "string",
        displayOptions: {
          show: { resource: ["query"], operation: ["persist"] },
        },
        default: "",
        required: true,
        placeholder: "/home/user/my_data.db",
        description:
          "Path to the .db file to create. All in-memory tables will be copied to this file.",
      },
      {
        displayName: "Force Overwrite",
        name: "forceOverwrite",
        type: "boolean",
        displayOptions: {
          show: { resource: ["query"], operation: ["persist"] },
        },
        default: false,
        description: "Whether to overwrite if the target file already exists",
      },
    ],
  };

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const items = this.getInputData();
    const returnData: INodeExecutionData[] = [];
    const resource = this.getNodeParameter("resource", 0) as string;
    const credentials = await this.getCredentials("duckDbQuackApi");

    const instancePath =
      credentials.connectionType === "local"
        ? (credentials.filePath as string) || ":memory:"
        : `quack_${credentials.host || "localhost"}`;
    const cacheKey = instancePath;

    const instance = await getOrCreateInstance(cacheKey, instancePath);
    const connection = await instance.connect();

    try {
      // --- Bootstrap extensions (once per instance) ---
      if (!loadedInstances.has(cacheKey)) {
        // Enable auto-install for known extensions (JIT loading)
        await connection.run(`SET autoload_known_extensions = true;`);
        await connection.run(`SET autoinstall_known_extensions = true;`);
        for (const ext of CORE_EXTENSIONS) {
          const spec = parseExtensionSpec(ext);
          await connection.run(`INSTALL ${spec.install};`);
          await connection.run(`LOAD ${spec.name};`);
        }
        if (credentials.autoLoadExtensions) {
          const explicitList = (credentials.autoLoadExtensions as string)
            .split(",")
            .map((e) => e.trim())
            .filter((e) => e.length > 0);
          for (const ext of explicitList) {
            const spec = parseExtensionSpec(ext);
            await connection.run(`INSTALL ${spec.install};`);
            await connection.run(`LOAD ${spec.name};`);
          }
        }
        loadedInstances.add(cacheKey);
      }

      // --- Ensure user extensions are loaded (defensive reload) ---
      if (credentials.autoLoadExtensions) {
        const reloadList = (credentials.autoLoadExtensions as string)
          .split(",")
          .map((e) => e.trim())
          .filter((e) => e.length > 0);
        for (const ext of reloadList) {
          try {
            await connection.run(`LOAD ${parseExtensionSpec(ext).name};`);
          } catch (_e) {
            // Extension might not be installed yet — INSTALL first
            const spec = parseExtensionSpec(ext);
            await connection.run(`INSTALL ${spec.install};`);
            await connection.run(`LOAD ${spec.name};`);
          }
        }
      }

      // --- Connect: remote ATTACH needed for mutate operations + query ---
      const isRemote = credentials.connectionType === "remote";
      const needsAttach =
        isRemote &&
        ((resource === "query" &&
          this.getNodeParameter("operation", 0) !== "persist") ||
          (resource === "table" &&
            ["write", "update", "delete", "read"].includes(
              this.getNodeParameter("operation", 0) as string,
            )));

      if (needsAttach) {
        const host = (credentials.host as string) || "quack:localhost:9494";
        const token = credentials.token as string;
        const disableSsl = credentials.disableSsl as boolean;

        await connection.run(`INSTALL quack;`);
        await connection.run(`LOAD quack;`);
        if (token) {
          await connection.run(
            `CREATE OR REPLACE SECRET (TYPE quack, TOKEN '${token.replace(/'/g, "''")}', SCOPE '${host.replace(/'/g, "''")}');`,
          );
        }
        const sslFlag = disableSsl ? ", DISABLE_SSL true" : "";
        // Retry ATTACH: same pattern as runRemoteQuery (3 attempts, 200ms delay)
        for (let attempt = 0; attempt < 3; attempt++) {
          try {
            await connection.run(
              `ATTACH '${host.replace(/'/g, "''")}' AS target_db (TYPE quack${sslFlag});`,
            );
            break;
          } catch (error) {
            if (attempt === 2) throw error;
            try {
              await connection.run(`DETACH target_db;`);
            } catch (_e2) {
              /* ignore */
            }
            await new Promise((resolve) => setTimeout(resolve, 200));
          }
        }
        await connection.run(`USE target_db;`);
      }
      // Local: instance already owns the file — no ATTACH needed

      // --- Helpers ---
      const exportParquet = async (sql: string, filename: string) => {
        const tmpFile = path.join(
          os.tmpdir(),
          `n8n_duck_${Date.now()}.parquet`,
        );
        try {
          await connection.run(
            `COPY (${sql}) TO '${tmpFile.replace(/'/g, "''")}' (FORMAT 'PARQUET');`,
          );
          const fileBuffer = fs.readFileSync(tmpFile);
          const binaryData = await this.helpers.prepareBinaryData(
            fileBuffer,
            filename,
            "application/vnd.apache.parquet",
          );
          return binaryData;
        } finally {
          try {
            fs.unlinkSync(tmpFile);
          } catch (_e) {
            /* already gone */
          }
        }
      };

      // Helper: stream query results directly to a file on disk (no binary pipeline)
      const exportToFile = async (
        sql: string,
        destPath: string,
        format: string,
        csvHeader: boolean,
      ) => {
        const formatClause =
          format === "parquet"
            ? "PARQUET"
            : `CSV, HEADER ${csvHeader}`;
        await connection.run(
          `COPY (${sql}) TO '${destPath.replace(/'/g, "''")}' (FORMAT ${formatClause});`,
        );
      };

      // Helper: run a query or DML via stateless quack_query (avoids ATTACH streaming conflicts)
      const runRemoteQuery = async (
        creds: Record<string, unknown>,
        sql: string,
      ): Promise<Array<Record<string, unknown>>> => {
        const host = (creds.host as string) || "quack:localhost:9494";
        const token = creds.token as string;
        const disableSsl = creds.disableSsl as boolean;
        const escapedSql = sql.replace(/'/g, "''");
        const tokenArg = token
          ? `, token := '${token.replace(/'/g, "''")}'`
          : "";
        const sslArg = disableSsl ? ", disable_ssl := true" : "";
        // Retry transient failures (server restart, WSL2 network timing)
        for (let attempt = 0; attempt < 3; attempt++) {
          try {
            const result = await connection.runAndReadAll(
              `FROM quack_query('${host.replace(/'/g, "''")}', '${escapedSql}'${tokenArg}${sslArg});`,
            );
            return result.getRowObjectsJson();
          } catch (error) {
            const msg = (error as Error).message;
            if (
              attempt < 2 &&
              (msg.includes("Invalid connection id") ||
                msg.includes("Invalid Input Error"))
            ) {
              await new Promise((resolve) => setTimeout(resolve, 200));
              continue;
            }
            throw error;
          }
        }
        throw new Error("unreachable");
      };

      // ========================== TABLE ==========================
      if (resource === "table") {
        const op = this.getNodeParameter("operation", 0) as string;

        if (op === "listColumns") {
          const rawTable = this.getNodeParameter("tableName", 0) as string;
          const table = validateTableName(rawTable, this.getNode(), 0);
          const sql = `DESCRIBE ${table}`;
          const rows = isRemote
            ? await runRemoteQuery(credentials, sql)
            : (await connection.runAndReadAll(`${sql};`)).getRowObjectsJson();
          for (const row of rows) {
            returnData.push({
              json: row as unknown as IDataObject,
              pairedItem: { item: 0 },
            });
          }
        } else if (op === "listTables") {
          const sql = "SHOW ALL TABLES";
          const rows = isRemote
            ? await runRemoteQuery(credentials, sql)
            : (await connection.runAndReadAll(`${sql};`)).getRowObjectsJson();
          for (const row of rows) {
            returnData.push({
              json: row as unknown as IDataObject,
              pairedItem: { item: 0 },
            });
          }
        } else if (op === "read") {
          const rawTable = this.getNodeParameter("tableName", 0) as string;
          const table = validateTableName(rawTable, this.getNode(), 0);
          const format = this.getNodeParameter("outputFormat", 0) as string;
          const whereClause = this.getNodeParameter(
            "whereClause",
            0,
            "",
          ) as string;
          const limit = this.getNodeParameter("limit", 0, "") as
            | number
            | string;

          let sql = `SELECT * FROM ${table}`;
          if (whereClause && whereClause.trim()) {
            sql += ` WHERE ${whereClause.trim()}`;
          }
          if (limit && Number(limit) > 0) {
            sql += ` LIMIT ${Number(limit)}`;
          }

          if (format === "parquet" || format === "csv") {
            const filePath = this.getNodeParameter("filePath", 0, "") as string;
            if (filePath && filePath.trim()) {
              // Stream directly to disk — no memory overhead
              const csvHeader =
                format === "csv"
                  ? (this.getNodeParameter("csvHeader", 0) as boolean)
                  : false;
              // Count rows before export via ATTACH (single query, works local + remote)
              const countRows = (
                await connection.runAndReadAll(
                  `SELECT COUNT(*) AS cnt FROM (${sql}) AS _sub;`,
                )
              ).getRowObjectsJson();
              const rows = Number(
                (countRows[0] as Record<string, unknown> | undefined)
                  ?.cnt ?? 0,
              );

              await exportToFile(sql, filePath.trim(), format, csvHeader);
              returnData.push({
                json: {
                  exported: true,
                  path: filePath.trim(),
                  rows,
                } as unknown as IDataObject,
                pairedItem: { item: 0 },
              });
            } else if (format === "csv") {
              throw new NodeOperationError(
                this.getNode(),
                "CSV output requires a File Path.",
                { itemIndex: 0 },
              );
            } else {
              // Parquet without file path: existing binary pipeline
              const binaryData = await exportParquet(sql, `${table}.parquet`);
              returnData.push({
                json: {
                  rowCount: "Streamed to Parquet file",
                } as unknown as IDataObject,
                binary: { data: binaryData },
                pairedItem: { item: 0 },
              });
            }
          } else {
            const rows = isRemote
              ? await runRemoteQuery(credentials, sql)
              : (await connection.runAndReadAll(`${sql};`)).getRowObjectsJson();
            for (const row of rows) {
              returnData.push({
                json: row as unknown as IDataObject,
                pairedItem: { item: 0 },
              });
            }
          }
        } else if (op === "write") {
          const rawTable = this.getNodeParameter("tableName", 0) as string;
          const table = validateTableName(rawTable, this.getNode(), 0);
          const mode = this.getNodeParameter("writeMode", 0) as string;
          const columnTypes = this.getNodeParameter(
            "columnTypes",
            0,
            "varchar",
          ) as string;

          if (items.length === 0) {
            returnData.push({
              json: { rows_inserted: 0 } as unknown as IDataObject,
              pairedItem: { item: 0 },
            });
          } else {
            if (mode === "overwrite") {
              await connection.run(`DROP TABLE IF EXISTS ${table};`);
            }

            const sampleRow = items[0].json;
            const columns = Object.keys(sampleRow);

            if (columns.length === 0) {
              throw new NodeOperationError(
                this.getNode(),
                "No columns found in input data. Provide at least one key-value pair.",
                { itemIndex: 0 },
              );
            }

            if (columnTypes === "auto" && mode === "overwrite") {
              // Auto-detect: build VALUES clause, let DuckDB infer types
              const valueRows = items.map((item) => {
                const vals = columns.map((col) => {
                  const val = item.json[col];
                  if (val === undefined || val === null) return "NULL";
                  if (typeof val === "boolean") return val ? "TRUE" : "FALSE";
                  if (typeof val === "string") {
                    // Detect ISO dates: YYYY-MM-DD
                    if (/^\d{4}-\d{2}-\d{2}$/.test(val)) return `DATE '${val}'`;
                    // Detect timestamps: YYYY-MM-DD[ T]HH:MM:SS[.fff]
                    if (
                      /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$/.test(
                        val,
                      )
                    )
                      return `TIMESTAMP '${val}'`;
                    return `'${val.replace(/'/g, "''")}'`;
                  }
                  return String(val);
                });
                return `(${vals.join(", ")})`;
              });
              const createSql = `CREATE OR REPLACE TABLE ${table} AS SELECT * FROM (VALUES ${valueRows.join(", ")}) t(${columns.join(", ")});`;
              await connection.run(createSql);
              returnData.push({
                json: { rows_inserted: items.length } as unknown as IDataObject,
                pairedItem: { item: 0 },
              });
            } else {
              // VARCHAR fallback (or append to existing table)
              const colDefs = columns.map((c) => `${c} VARCHAR`).join(", ");
              await connection.run(
                `CREATE TABLE IF NOT EXISTS ${table} (${colDefs});`,
              );

              if (isRemote) {
                // Quack: batch INSERT via SQL (appender has chunk limits over HTTP)
                const valueRows: string[] = [];
                let inserted = 0;
                for (let i = 0; i < items.length; i++) {
                  const vals = columns.map((col) => {
                    const val = items[i].json[col];
                    if (val === undefined || val === null) return "NULL";
                    return `'${String(val).replace(/'/g, "''")}'`;
                  });
                  valueRows.push(`(${vals.join(", ")})`);
                  inserted++;
                }
                if (valueRows.length > 0) {
                  await connection.run(
                    `INSERT INTO ${table} (${columns.join(", ")}) VALUES ${valueRows.join(", ")};`,
                  );
                }
                returnData.push({
                  json: { rows_inserted: inserted } as unknown as IDataObject,
                  pairedItem: { item: 0 },
                });
              } else {
                // Local: use appender for efficiency
                const appender = await connection.createAppender(table);
                let inserted = 0;
                for (let i = 0; i < items.length; i++) {
                  const row = items[i].json;
                  try {
                    for (const col of columns) {
                      const val = row[col];
                      if (val === undefined || val === null) {
                        appender.appendNull();
                      } else {
                        appender.appendVarchar(String(val));
                      }
                    }
                    appender.endRow();
                    inserted++;
                  } catch (itemError) {
                    if (this.continueOnFail()) {
                      returnData.push({
                        json: {
                          error: (itemError as Error).message,
                        } as unknown as IDataObject,
                        error: itemError as NodeOperationError,
                        pairedItem: { item: i },
                      });
                      continue;
                    }
                    throw new NodeApiError(
                      this.getNode(),
                      itemError as unknown as JsonObject,
                      { itemIndex: i },
                    );
                  }
                }
                appender.closeSync();
                returnData.push({
                  json: { rows_inserted: inserted } as unknown as IDataObject,
                  pairedItem: { item: 0 },
                });
              }
            }
          }
        } else if (op === "update") {
          const rawTable = this.getNodeParameter("tableName", 0) as string;
          const table = isRemote
            ? rawTable
            : validateTableName(rawTable, this.getNode(), 0);
          const whereClause = (
            this.getNodeParameter("updateWhereClause", 0) as string
          ).trim();
          const setColumns = (
            this.getNodeParameter("setColumns", 0) as string
          ).trim();

          if (!whereClause) {
            throw new NodeOperationError(
              this.getNode(),
              "UPDATE requires a WHERE clause. Use SQL Query for unconstrained updates.",
              { itemIndex: 0 },
            );
          }
          if (!setColumns) {
            throw new NodeOperationError(
              this.getNode(),
              "UPDATE requires Set Columns. Provide comma-separated column=value pairs.",
              { itemIndex: 0 },
            );
          }

          // Count matching rows first
          const countSql = `SELECT COUNT(*) AS cnt FROM ${rawTable} WHERE ${whereClause};`;
          const countRows = isRemote
            ? await runRemoteQuery(credentials, countSql)
            : (await connection.runAndReadAll(countSql)).getRowObjectsJson();
          const updated = Number(countRows[0]?.cnt ?? 0);

          const sql = `UPDATE ${table} SET ${setColumns} WHERE ${whereClause};`;

          // Remote: send UPDATE via quack_query (ATTACH doesn't support DML on Quack tables)
          if (isRemote) {
            await runRemoteQuery(credentials, sql);
          } else {
            await connection.run(sql);
          }
          returnData.push({
            json: { rows_updated: updated } as unknown as IDataObject,
            pairedItem: { item: 0 },
          });
        } else if (op === "delete") {
          const rawTable = this.getNodeParameter("tableName", 0) as string;
          const table = isRemote
            ? rawTable
            : validateTableName(rawTable, this.getNode(), 0);
          const whereClause = (
            this.getNodeParameter("deleteWhereClause", 0) as string
          ).trim();

          if (!whereClause) {
            throw new NodeOperationError(
              this.getNode(),
              "DELETE requires a WHERE clause. Use SQL Query for unconstrained deletes.",
              { itemIndex: 0 },
            );
          }

          // Count matching rows first
          const countSql = `SELECT COUNT(*) AS cnt FROM ${rawTable} WHERE ${whereClause};`;
          const countRows = isRemote
            ? await runRemoteQuery(credentials, countSql)
            : (await connection.runAndReadAll(countSql)).getRowObjectsJson();
          const deleted = Number(countRows[0]?.cnt ?? 0);

          // Remote: send DELETE via quack_query (ATTACH doesn't support DML on Quack tables)
          if (isRemote) {
            await runRemoteQuery(
              credentials,
              `DELETE FROM ${rawTable} WHERE ${whereClause};`,
            );
          } else {
            const sql = `DELETE FROM ${table} WHERE ${whereClause};`;
            await connection.run(sql);
          }
          returnData.push({
            json: { rows_deleted: deleted } as unknown as IDataObject,
            pairedItem: { item: 0 },
          });
        }
      }

      // ========================== QUERY ==========================
      else if (resource === "query") {
        const op = this.getNodeParameter("operation", 0) as string;

        if (op === "persist") {
          // Remote: persist tables from the Quack-attached database.
          // Local: only :memory: can be persisted (file-backed DBs are already on disk).
          if (
            !isRemote &&
            (credentials.filePath as string) !== ":memory:"
          ) {
            throw new NodeOperationError(
              this.getNode(),
              'Local Persist Memory to Disk requires File Path set to ":memory:".',
              { itemIndex: 0 },
            );
          }
          const dest = this.getNodeParameter("targetDiskPath", 0) as string;
          const overwrite = this.getNodeParameter(
            "forceOverwrite",
            0,
          ) as boolean;

          if (fs.existsSync(dest)) {
            if (overwrite) {
              fs.unlinkSync(dest);
              // Clear cached instance so next read gets fresh data
              instanceCache.delete(dest);
            } else {
              throw new NodeOperationError(
                this.getNode(),
                `Target file already exists: ${dest}. Enable "Force Overwrite" to replace it.`,
                { itemIndex: 0 },
              );
            }
          }


          await connection.run(
            `ATTACH '${dest.replace(/'/g, "''")}' AS disk_db;`,
          );
          try {
            // Enumerate tables on main connection (has target_db from ATTACH).
            // Run CTAS on a separate connection to avoid Quack's
            // "Multiple streaming scans" limitation.
            let tableNames: string[] = [];
            if (isRemote) {
              const allTables = (
                await connection.runAndReadAll("SHOW ALL TABLES;")
              ).getRowObjectsJson();
              tableNames = allTables
                .filter(
                  (t: Record<string, unknown>) => t.database === "target_db",
                )
                .map((t: Record<string, unknown>) => t.name as string);
            } else {
              const localTables = (
                await connection.runAndReadAll(
                  "SELECT table_name FROM information_schema.tables WHERE table_schema='main';",
                )
              ).getRowObjectsJson();
              tableNames = localTables.map(
                (t: Record<string, unknown>) => t.table_name as string,
              );
            }

            if (tableNames.length === 0) {
              returnData.push({
                json: {
                  success: false,
                  message: "No tables found — nothing to persist",
                } as unknown as IDataObject,
                pairedItem: { item: 0 },
              });
            } else if (isRemote) {
              // Remote persist: Quack ATTACH prohibits CTAS/INSERT from
              // streaming sources. Use quack_query (stateless) to fetch
              // data, then INSERT locally into disk_db.
              const copyConn = await instance.connect();
              try {
                let copied = 0;
                for (const name of tableNames) {
                  const rows = await runRemoteQuery(
                    credentials,
                    `SELECT * FROM ${name};`,
                  );
                  if (rows.length === 0) continue;
                  const cols = Object.keys(rows[0]);
                  const colDefs = cols.map((c) => `${c} VARCHAR`).join(", ");
                  await copyConn.run(
                    `CREATE TABLE IF NOT EXISTS disk_db.main.${name} (${colDefs});`,
                  );
                  const valueRows = rows.map((row) => {
                    const vals = cols.map((col) => {
                      const val = row[col];
                      if (val === null || val === undefined) return "NULL";
                      return `'${String(val).replace(/'/g, "''")}'`;
                    });
                    return `(${vals.join(", ")})`;
                  });
                  if (valueRows.length > 0) {
                    await copyConn.run(
                      `INSERT INTO disk_db.main.${name} (${cols.join(", ")}) VALUES ${valueRows.join(", ")};`,
                    );
                  }
                  copied++;
                }
                returnData.push({
                  json: {
                    success: true,
                    message: `Saved ${copied} tables to ${dest}`,
                  } as unknown as IDataObject,
                  pairedItem: { item: 0 },
                });
              } finally {
                copyConn.closeSync();
              }
            } else {
              // Local persist: CTAS works natively
              let copied = 0;
              for (const name of tableNames) {
                await connection.run(
                  `CREATE TABLE IF NOT EXISTS disk_db.main.${name} AS SELECT * FROM main.${name};`,
                );
                copied++;
              }
              returnData.push({
                json: {
                  success: copied > 0,
                  message:
                    copied > 0
                      ? `Saved ${copied} tables to ${dest}`
                      : `No tables found in memory — nothing to persist`,
                } as unknown as IDataObject,
                pairedItem: { item: 0 },
              });
            }
          } finally {
            try {
              await connection.run(`DETACH disk_db;`);
            } catch (_e) {
              /* ignore */
            }
            instanceCache.delete(dest);
          }
        } else if (op === "select") {
          const sql = this.getNodeParameter("sqlQuery", 0) as string;
          const format = this.getNodeParameter(
            "queryOutputFormat",
            0,
          ) as string;

          if (format === "parquet" || format === "csv") {
            const queryFilePath = this.getNodeParameter("queryFilePath", 0, "") as string;
            if (queryFilePath && queryFilePath.trim()) {
              // Stream directly to disk — no memory overhead
              const csvHeader =
                format === "csv"
                  ? (this.getNodeParameter("queryCsvHeader", 0) as boolean)
                  : false;
              await exportToFile(sql, queryFilePath.trim(), format, csvHeader);
              returnData.push({
                json: {
                  exported: true,
                  path: queryFilePath.trim(),
                } as unknown as IDataObject,
                pairedItem: { item: 0 },
              });
            } else if (format === "csv") {
              throw new NodeOperationError(
                this.getNode(),
                "CSV output requires a File Path.",
                { itemIndex: 0 },
              );
            } else {
              // Parquet without file path: existing binary pipeline
              const binaryData = await exportParquet(sql, "query_output.parquet");
              returnData.push({
                json: {
                  rowCount: "Query output saved to Parquet format",
                } as unknown as IDataObject,
                binary: { data: binaryData },
                pairedItem: { item: 0 },
              });
            }
          } else {
            // Split multi-statement SQL: run DDL/PRAGMA/etc with run(),
            // only the last statement produces output
            const statements = sql
              .split(";")
              .map((s) => s.trim())
              .filter((s) => s.length > 0);
            const lastSql =
              statements.length > 0
                ? `${statements[statements.length - 1]};`
                : sql;

            // Single-statement remote query: use quack_query to avoid
            // stale ATTACH handles (critical for WSL2 networking).
            if (isRemote && statements.length <= 1) {
              const rows = await runRemoteQuery(credentials, lastSql);
              for (const row of rows) {
                returnData.push({
                  json: row as unknown as IDataObject,
                  pairedItem: { item: 0 },
                });
              }
            } else {
              // Multi-statement or local: use ATTACH-based connection
              for (let i = 0; i < statements.length - 1; i++) {
                try {
                  await connection.run(`${statements[i]};`);
                } catch (_s) {
                  this.logger.warn(
                    `Multi-statement SQL: intermediate statement failed (statement ${i + 1}/${statements.length - 1}): ${(statements[i] || "").substring(0, 80)}`,
                    { error: (_s as Error).message },
                  );
                }
              }
              const result = await connection.runAndReadAll(lastSql);
              const rows = result.getRowObjectsJson();
              for (const row of rows) {
                returnData.push({
                  json: row as unknown as IDataObject,
                  pairedItem: { item: 0 },
                });
              }
            }
          }
        } else if (op === "stateless") {
          if (!isRemote) {
            throw new NodeOperationError(
              this.getNode(),
              'Stateless Quack Query requires Connection Mode to be "Remote". Use "Select (Custom SQL)" for local queries.',
              { itemIndex: 0 },
            );
          }
          const host = (credentials.host as string) || "quack:localhost:9494";
          const token = credentials.token as string;
          const disableSsl = credentials.disableSsl as boolean;
          const sql = this.getNodeParameter("sqlQuery", 0) as string;
          const escapedSql = sql.replace(/'/g, "''");

          const tokenArg = token
            ? `, token := '${token.replace(/'/g, "''")}'`
            : "";
          const sslArg = disableSsl ? ", disable_ssl := true" : "";
          const result = await connection.runAndReadAll(
            `FROM quack_query('${host.replace(/'/g, "''")}', '${escapedSql}'${tokenArg}${sslArg});`,
          );
          const rows = result.getRowObjectsJson();
          for (const row of rows) {
            returnData.push({
              json: row as unknown as IDataObject,
              pairedItem: { item: 0 },
            });
          }
        }
      }

      // ========================== SERVER ==========================
      else if (resource === "server") {
        if (!isRemote) {
          throw new NodeOperationError(
            this.getNode(),
            'Server operations require Connection Mode to be "Remote".',
            { itemIndex: 0 },
          );
        }

        const op = this.getNodeParameter("operation", 0) as string;

        if (op === "serverInfo") {
          const rows = await runRemoteQuery(
            credentials,
            "SELECT * FROM whoami();",
          );
          for (const row of rows) {
            // Parse JSON metadata field if present
            const output: Record<string, unknown> = { ...row };
            if (typeof output.meta === "string") {
              try {
                output.meta = JSON.parse(output.meta as string);
              } catch (_e) {
                /* keep as raw string if invalid JSON */
              }
            }
            returnData.push({
              json: output as unknown as IDataObject,
              pairedItem: { item: 0 },
            });
          }
        } else if (op === "listSessions") {
          const rows = await runRemoteQuery(
            credentials,
            "SELECT * FROM quack_active_connections() WHERE state = 'active';",
          );
          for (const row of rows) {
            returnData.push({
              json: row as unknown as IDataObject,
              pairedItem: { item: 0 },
            });
          }
        }
      }
    } catch (error) {
      // Quietly ignore shutdown errors (Ctrl+C tears down DuckDB mid-query)
      const msg = (error as Error).message;
      if (
        msg.includes("database is closed") ||
        msg.includes("Connection closed")
      ) {
        return [returnData];
      }
      if (this.continueOnFail()) {
        returnData.push({
          json: { error: (error as Error).message } as unknown as IDataObject,
          error: error as NodeOperationError,
          pairedItem: { item: 0 },
        });
      } else {
        throw new NodeApiError(this.getNode(), error as unknown as JsonObject, {
          itemIndex: 0,
        });
      }
    } finally {
      try {
        connection.closeSync();
      } catch (_e) {
        /* ignore shutdown errors */
      }
    }

    return [returnData];
  }
}
