"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.DuckDbQuack = void 0;
const n8n_workflow_1 = require("n8n-workflow");
const node_api_1 = require("@duckdb/node-api");
const fs = __importStar(require("fs"));
const os = __importStar(require("os"));
const path = __importStar(require("path"));
const instanceCache = new Map();
async function getOrCreateInstance(key, path) {
    const entry = instanceCache.get(key);
    if (entry)
        return entry;
    const dbPath = path.startsWith('quack_') ? ':memory:' : path;
    const inst = await node_api_1.DuckDBInstance.create(dbPath);
    instanceCache.set(key, inst);
    return inst;
}
const VALID_TABLE_NAME = /^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$/;
function validateTableName(name, node, itemIndex) {
    const trimmed = name.trim();
    if (!trimmed)
        return trimmed;
    if (!VALID_TABLE_NAME.test(trimmed)) {
        throw new n8n_workflow_1.NodeOperationError(node, `Invalid table name "${trimmed}". Use only letters, digits, underscores, and dots (e.g., employees or main.employees).`, { itemIndex });
    }
    return trimmed;
}
const CORE_EXTENSIONS = ['httpfs'];
function parseExtensionSpec(spec) {
    const trimmed = spec.trim();
    const parts = trimmed.split(/\s+/);
    const name = parts[0];
    return { name, install: trimmed };
}
const loadedInstances = new Set();
class DuckDbQuack {
    constructor() {
        this.methods = {
            loadOptions: {
                async getTables() {
                    const run = async () => {
                        const credentials = await this.getCredentials('duckDbQuackApi');
                        const instancePath = credentials.connectionType === 'local'
                            ? (credentials.filePath || ':memory:')
                            : `quack_${credentials.host || 'localhost'}`;
                        const cacheKey = instancePath;
                        const instance = await getOrCreateInstance(cacheKey, instancePath);
                        const connection = await instance.connect();
                        try {
                            if (credentials.connectionType === 'remote') {
                                const host = credentials.host || 'quack:localhost:9494';
                                const token = credentials.token;
                                const disableSsl = credentials.disableSsl;
                                const tokenArg = token
                                    ? `, token := '${token.replace(/'/g, "''")}'`
                                    : '';
                                const sslArg = disableSsl ? ', disable_ssl := true' : '';
                                const result = await connection.runAndReadAll(`FROM quack_query('${host.replace(/'/g, "''")}', 'SHOW ALL TABLES'${tokenArg}${sslArg});`);
                                const rows = result.getRowObjectsJson();
                                return rows.map((row) => ({
                                    name: `[${row.schema}] ${row.name}${row.temporary ? ' (Temp)' : ''}`,
                                    value: `${row.schema}.${row.name}`,
                                }));
                            }
                            const result = await connection.runAndReadAll('SHOW ALL TABLES;');
                            const rows = result.getRowObjectsJson();
                            return rows.map((row) => ({
                                name: `[${row.schema}] ${row.name}${row.temporary ? ' (Temp)' : ''}`,
                                value: `${row.schema}.${row.name}`,
                            }));
                        }
                        catch (error) {
                            return [
                                {
                                    name: `Error fetching metadata: ${error.message}`,
                                    value: 'error',
                                },
                            ];
                        }
                        finally {
                            connection.closeSync();
                        }
                    };
                    const prev = DuckDbQuack._loadLock;
                    let release;
                    DuckDbQuack._loadLock = new Promise((r) => {
                        release = r;
                    });
                    await prev;
                    try {
                        return await run();
                    }
                    finally {
                        release();
                    }
                },
            },
        };
        this.description = {
            displayName: 'DuckDB Client',
            name: 'duckDbQuack',
            icon: { light: 'file:duckdb.svg', dark: 'file:duckdb.svg' },
            group: ['transform'],
            version: 1,
            subtitle: '={{$parameter["operation"] + ": " + $parameter["resource"]}}',
            description: 'Open, Read, Write, and Query data using local DuckDB files or high-speed Quack remote endpoints',
            defaults: { name: 'DuckDB Engine' },
            inputs: [n8n_workflow_1.NodeConnectionTypes.Main],
            outputs: [n8n_workflow_1.NodeConnectionTypes.Main],
            usableAsTool: true,
            credentials: [{ name: 'duckDbQuackApi', required: true }],
            properties: [
                {
                    displayName: 'Resource',
                    name: 'resource',
                    type: 'options',
                    noDataExpression: true,
                    options: [
                        { name: 'Query / Administration', value: 'query' },
                        { name: 'Table', value: 'table' },
                    ],
                    default: 'table',
                },
                {
                    displayName: 'Operation',
                    name: 'operation',
                    type: 'options',
                    displayOptions: { show: { resource: ['table'] } },
                    noDataExpression: true,
                    options: [
                        {
                            name: 'List Columns',
                            value: 'listColumns',
                            action: 'List columns a table',
                            description: 'Inspect structural properties and column definitions',
                        },
                        {
                            name: 'List Tables',
                            value: 'listTables',
                            action: 'List tables a table',
                            description: 'Fetch all tables visible in the current catalog',
                        },
                        {
                            name: 'Read Table',
                            value: 'read',
                            action: 'Read table a table',
                            description: 'Stream records from an active table',
                        },
                        {
                            name: 'Write / Append Rows',
                            value: 'write',
                            action: 'Write append rows a table',
                            description: 'Insert or map incoming data rows into a table',
                        },
                    ],
                    default: 'read',
                },
                {
                    displayName: 'Table Name or ID',
                    name: 'tableName',
                    type: 'options',
                    typeOptions: { loadOptionsMethod: 'getTables' },
                    displayOptions: {
                        show: { resource: ['table'], operation: ['listColumns', 'read'] },
                    },
                    default: '',
                    required: true,
                    description: 'Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>',
                },
                {
                    displayName: 'Table Name',
                    name: 'tableName',
                    type: 'string',
                    displayOptions: {
                        show: { resource: ['table'], operation: ['write'] },
                    },
                    default: '',
                    required: true,
                    placeholder: 'my_table',
                    description: 'Name of the table to write to (e.g., employees). For in-memory databases, use just the table name. The table is auto-created if missing.',
                },
                {
                    displayName: 'Filter (WHERE Clause)',
                    name: 'whereClause',
                    type: 'string',
                    displayOptions: { show: { resource: ['table'], operation: ['read'] } },
                    default: '',
                    required: false,
                    placeholder: "score > 80 AND name LIKE 'A%'",
                    description: 'SQL WHERE conditions applied at the database level before data reaches n8n',
                },
                {
                    displayName: 'Limit',
                    name: 'limit',
                    type: 'number',
                    displayOptions: { show: { resource: ['table'], operation: ['read'] } },
                    default: '',
                    required: false,
                    placeholder: '100',
                    description: 'Maximum number of rows to return. Leave empty for all records.',
                },
                {
                    displayName: 'Output Format',
                    name: 'outputFormat',
                    type: 'options',
                    displayOptions: { show: { resource: ['table'], operation: ['read'] } },
                    options: [
                        { name: 'Parquet File (Binary)', value: 'parquet' },
                        { name: 'Standard JSON Array', value: 'json' },
                    ],
                    default: 'json',
                },
                {
                    displayName: 'Write Mode',
                    name: 'writeMode',
                    type: 'options',
                    displayOptions: { show: { resource: ['table'], operation: ['write'] } },
                    options: [
                        { name: 'Append Rows', value: 'append' },
                        { name: 'Overwrite / Recreate', value: 'overwrite' },
                    ],
                    default: 'append',
                },
                {
                    displayName: 'Column Types',
                    name: 'columnTypes',
                    type: 'options',
                    displayOptions: { show: { resource: ['table'], operation: ['write'], writeMode: ['overwrite'] } },
                    options: [
                        {
                            name: 'VARCHAR (Safe — all types preserved as text)',
                            value: 'varchar',
                        },
                        {
                            name: 'Auto-Detect (Infer INT, DOUBLE, DATE from data)',
                            value: 'auto',
                        },
                    ],
                    default: 'varchar',
                    description: 'VARCHAR preserves everything as text. Auto-detect uses DuckDB type inference on VALUES for proper integers, floats, and dates.',
                },
                {
                    displayName: 'Operation',
                    name: 'operation',
                    type: 'options',
                    displayOptions: { show: { resource: ['query'] } },
                    noDataExpression: true,
                    options: [
                        {
                            name: 'Persist Memory to Disk',
                            value: 'persist',
                            action: 'Persist memory to disk a query',
                            description: 'Snapshot transient :memory: state to a file-backed database',
                        },
                        {
                            name: 'Select (Custom SQL)',
                            value: 'select',
                            action: 'Select custom sql a query',
                            description: 'Execute raw multi-line SQL queries',
                        },
                        {
                            name: 'Stateless Quack Query',
                            value: 'stateless',
                            action: 'Stateless quack query a query',
                            description: 'Single round-trip query bypassing ATTACH (remote only)',
                        },
                    ],
                    default: 'select',
                },
                {
                    displayName: 'SQL Query',
                    name: 'sqlQuery',
                    type: 'string',
                    typeOptions: { alwaysOpenEditWindow: true },
                    displayOptions: {
                        show: { resource: ['query'], operation: ['select', 'stateless'] },
                    },
                    default: 'SELECT * FROM target_db.main.orders LIMIT 100;',
                    required: true,
                },
                {
                    displayName: 'Output Format',
                    name: 'queryOutputFormat',
                    type: 'options',
                    displayOptions: { show: { resource: ['query'], operation: ['select'] } },
                    options: [
                        { name: 'Parquet File (Binary)', value: 'parquet' },
                        { name: 'Standard JSON Array', value: 'json' },
                    ],
                    default: 'json',
                },
                {
                    displayName: 'Target Disk Path',
                    name: 'targetDiskPath',
                    type: 'string',
                    displayOptions: { show: { resource: ['query'], operation: ['persist'] } },
                    default: '',
                    required: true,
                    placeholder: '/home/user/my_data.db',
                    description: 'Path to the .db file to create. All in-memory tables will be copied to this file.',
                },
                {
                    displayName: 'Force Overwrite',
                    name: 'forceOverwrite',
                    type: 'boolean',
                    displayOptions: { show: { resource: ['query'], operation: ['persist'] } },
                    default: false,
                    description: 'Whether to overwrite if the target file already exists',
                },
            ],
        };
    }
    async execute() {
        const items = this.getInputData();
        const returnData = [];
        const resource = this.getNodeParameter('resource', 0);
        const credentials = await this.getCredentials('duckDbQuackApi');
        const instancePath = credentials.connectionType === 'local'
            ? (credentials.filePath || ':memory:')
            : `quack_${credentials.host || 'localhost'}`;
        const cacheKey = instancePath;
        const instance = await getOrCreateInstance(cacheKey, instancePath);
        const connection = await instance.connect();
        try {
            if (!loadedInstances.has(cacheKey)) {
                await connection.run(`SET autoload_known_extensions = true;`);
                await connection.run(`SET autoinstall_known_extensions = true;`);
                for (const ext of CORE_EXTENSIONS) {
                    const spec = parseExtensionSpec(ext);
                    await connection.run(`INSTALL ${spec.install};`);
                    await connection.run(`LOAD ${spec.name};`);
                }
                if (credentials.autoLoadExtensions) {
                    const explicitList = credentials.autoLoadExtensions
                        .split(',')
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
            if (credentials.autoLoadExtensions) {
                const reloadList = credentials.autoLoadExtensions
                    .split(',')
                    .map((e) => e.trim())
                    .filter((e) => e.length > 0);
                for (const ext of reloadList) {
                    try {
                        await connection.run(`LOAD ${parseExtensionSpec(ext).name};`);
                    }
                    catch (_e) {
                        const spec = parseExtensionSpec(ext);
                        await connection.run(`INSTALL ${spec.install};`);
                        await connection.run(`LOAD ${spec.name};`);
                    }
                }
            }
            const isRemote = credentials.connectionType === 'remote';
            const needsAttach = isRemote &&
                (resource === 'query' ||
                    (resource === 'table' &&
                        this.getNodeParameter('operation', 0) === 'write'));
            if (needsAttach) {
                const host = credentials.host || 'quack:localhost:9494';
                const token = credentials.token;
                const disableSsl = credentials.disableSsl;
                await connection.run(`INSTALL quack;`);
                await connection.run(`LOAD quack;`);
                if (token) {
                    await connection.run(`CREATE OR REPLACE SECRET (TYPE quack, TOKEN '${token.replace(/'/g, "''")}', SCOPE '${host.replace(/'/g, "''")}');`);
                }
                const sslFlag = disableSsl ? ', DISABLE_SSL true' : '';
                try {
                    await connection.run(`ATTACH '${host.replace(/'/g, "''")}' AS target_db (TYPE quack${sslFlag});`);
                }
                catch (_e) {
                    try {
                        await connection.run(`DETACH target_db;`);
                    }
                    catch (_e2) {
                    }
                    await connection.run(`ATTACH '${host.replace(/'/g, "''")}' AS target_db (TYPE quack${sslFlag});`);
                }
                await connection.run(`USE target_db;`);
            }
            const exportParquet = async (sql, filename) => {
                const tmpFile = path.join(os.tmpdir(), `n8n_duck_${Date.now()}.parquet`);
                try {
                    await connection.run(`COPY (${sql}) TO '${tmpFile.replace(/'/g, "''")}' (FORMAT 'PARQUET');`);
                    const fileBuffer = fs.readFileSync(tmpFile);
                    const binaryData = await this.helpers.prepareBinaryData(fileBuffer, filename, 'application/vnd.apache.parquet');
                    return binaryData;
                }
                finally {
                    try {
                        fs.unlinkSync(tmpFile);
                    }
                    catch (_e) { }
                }
            };
            const runRemoteQuery = async (creds, sql) => {
                const host = creds.host || 'quack:localhost:9494';
                const token = creds.token;
                const disableSsl = creds.disableSsl;
                const escapedSql = sql.replace(/'/g, "''");
                const tokenArg = token ? `, token := '${token.replace(/'/g, "''")}'` : '';
                const sslArg = disableSsl ? ', disable_ssl := true' : '';
                const result = await connection.runAndReadAll(`FROM quack_query('${host.replace(/'/g, "''")}', '${escapedSql}'${tokenArg}${sslArg});`);
                return result.getRowObjectsJson();
            };
            if (resource === 'table') {
                const op = this.getNodeParameter('operation', 0);
                if (op === 'listColumns') {
                    const rawTable = this.getNodeParameter('tableName', 0);
                    const table = validateTableName(rawTable, this.getNode(), 0);
                    const sql = `DESCRIBE ${table}`;
                    const rows = isRemote
                        ? await runRemoteQuery(credentials, sql)
                        : (await connection.runAndReadAll(`${sql};`)).getRowObjectsJson();
                    for (const row of rows) {
                        returnData.push({
                            json: row,
                            pairedItem: { item: 0 },
                        });
                    }
                }
                else if (op === 'listTables') {
                    const sql = 'SHOW ALL TABLES';
                    const rows = isRemote
                        ? await runRemoteQuery(credentials, sql)
                        : (await connection.runAndReadAll(`${sql};`)).getRowObjectsJson();
                    for (const row of rows) {
                        returnData.push({
                            json: row,
                            pairedItem: { item: 0 },
                        });
                    }
                }
                else if (op === 'read') {
                    const rawTable = this.getNodeParameter('tableName', 0);
                    const table = validateTableName(rawTable, this.getNode(), 0);
                    const format = this.getNodeParameter('outputFormat', 0);
                    const whereClause = this.getNodeParameter('whereClause', 0, '');
                    const limit = this.getNodeParameter('limit', 0, '');
                    let sql = `SELECT * FROM ${table}`;
                    if (whereClause && whereClause.trim()) {
                        sql += ` WHERE ${whereClause.trim()}`;
                    }
                    if (limit && Number(limit) > 0) {
                        sql += ` LIMIT ${Number(limit)}`;
                    }
                    if (format === 'parquet') {
                        const binaryData = await exportParquet(sql, `${table}.parquet`);
                        returnData.push({
                            json: { rowCount: 'Streamed to Parquet file' },
                            binary: { data: binaryData },
                            pairedItem: { item: 0 },
                        });
                    }
                    else {
                        const rows = isRemote
                            ? await runRemoteQuery(credentials, sql)
                            : (await connection.runAndReadAll(`${sql};`)).getRowObjectsJson();
                        for (const row of rows) {
                            returnData.push({
                                json: row,
                                pairedItem: { item: 0 },
                            });
                        }
                    }
                }
                else if (op === 'write') {
                    const rawTable = this.getNodeParameter('tableName', 0);
                    const table = validateTableName(rawTable, this.getNode(), 0);
                    const mode = this.getNodeParameter('writeMode', 0);
                    const columnTypes = this.getNodeParameter('columnTypes', 0, 'varchar');
                    if (items.length === 0) {
                        returnData.push({
                            json: { rows_inserted: 0 },
                            pairedItem: { item: 0 },
                        });
                    }
                    else {
                        if (mode === 'overwrite') {
                            await connection.run(`DROP TABLE IF EXISTS ${table};`);
                        }
                        const sampleRow = items[0].json;
                        const columns = Object.keys(sampleRow);
                        if (columns.length === 0) {
                            throw new n8n_workflow_1.NodeOperationError(this.getNode(), 'No columns found in input data. Provide at least one key-value pair.', { itemIndex: 0 });
                        }
                        if (columnTypes === 'auto' && mode === 'overwrite') {
                            const valueRows = items.map((item) => {
                                const vals = columns.map((col) => {
                                    const val = item.json[col];
                                    if (val === undefined || val === null)
                                        return 'NULL';
                                    if (typeof val === 'boolean')
                                        return val ? 'TRUE' : 'FALSE';
                                    if (typeof val === 'string') {
                                        if (/^\d{4}-\d{2}-\d{2}$/.test(val))
                                            return `DATE '${val}'`;
                                        if (/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$/.test(val))
                                            return `TIMESTAMP '${val}'`;
                                        return `'${val.replace(/'/g, "''")}'`;
                                    }
                                    return String(val);
                                });
                                return `(${vals.join(', ')})`;
                            });
                            const createSql = `CREATE OR REPLACE TABLE ${table} AS SELECT * FROM (VALUES ${valueRows.join(', ')}) t(${columns.join(', ')});`;
                            await connection.run(createSql);
                            returnData.push({
                                json: { rows_inserted: items.length },
                                pairedItem: { item: 0 },
                            });
                        }
                        else {
                            const colDefs = columns.map((c) => `${c} VARCHAR`).join(', ');
                            await connection.run(`CREATE TABLE IF NOT EXISTS ${table} (${colDefs});`);
                            if (isRemote) {
                                const valueRows = [];
                                let inserted = 0;
                                for (let i = 0; i < items.length; i++) {
                                    const vals = columns.map((col) => {
                                        const val = items[i].json[col];
                                        if (val === undefined || val === null)
                                            return 'NULL';
                                        return `'${String(val).replace(/'/g, "''")}'`;
                                    });
                                    valueRows.push(`(${vals.join(', ')})`);
                                    inserted++;
                                }
                                if (valueRows.length > 0) {
                                    await connection.run(`INSERT INTO ${table} (${columns.join(', ')}) VALUES ${valueRows.join(', ')};`);
                                }
                                returnData.push({
                                    json: { rows_inserted: inserted },
                                    pairedItem: { item: 0 },
                                });
                            }
                            else {
                                const appender = await connection.createAppender(table);
                                let inserted = 0;
                                for (let i = 0; i < items.length; i++) {
                                    const row = items[i].json;
                                    try {
                                        for (const col of columns) {
                                            const val = row[col];
                                            if (val === undefined || val === null) {
                                                appender.appendNull();
                                            }
                                            else {
                                                appender.appendVarchar(String(val));
                                            }
                                        }
                                        appender.endRow();
                                        inserted++;
                                    }
                                    catch (itemError) {
                                        if (this.continueOnFail()) {
                                            returnData.push({
                                                json: {
                                                    error: itemError.message,
                                                },
                                                error: itemError,
                                                pairedItem: { item: i },
                                            });
                                            continue;
                                        }
                                        throw new n8n_workflow_1.NodeApiError(this.getNode(), itemError, { itemIndex: i });
                                    }
                                }
                                appender.closeSync();
                                returnData.push({
                                    json: { rows_inserted: inserted },
                                    pairedItem: { item: 0 },
                                });
                            }
                        }
                    }
                }
            }
            else if (resource === 'query') {
                const op = this.getNodeParameter('operation', 0);
                if (op === 'persist') {
                    if (isRemote || credentials.filePath !== ':memory:') {
                        throw new n8n_workflow_1.NodeOperationError(this.getNode(), 'Persist Memory to Disk requires Connection Mode to be "Local" and File Path set to ":memory:".', { itemIndex: 0 });
                    }
                    const dest = this.getNodeParameter('targetDiskPath', 0);
                    const overwrite = this.getNodeParameter('forceOverwrite', 0);
                    if (fs.existsSync(dest)) {
                        if (overwrite) {
                            fs.unlinkSync(dest);
                            instanceCache.delete(dest);
                        }
                        else {
                            throw new n8n_workflow_1.NodeOperationError(this.getNode(), `Target file already exists: ${dest}. Enable "Force Overwrite" to replace it.`, { itemIndex: 0 });
                        }
                    }
                    await connection.run(`ATTACH '${dest.replace(/'/g, "''")}' AS disk_db;`);
                    const tablesResult = await connection.runAndReadAll("SELECT table_name FROM information_schema.tables WHERE table_schema='main';");
                    const tables = tablesResult.getRowObjectsJson();
                    let copied = 0;
                    for (const t of tables) {
                        const name = t.table_name;
                        await connection.run(`CREATE TABLE IF NOT EXISTS disk_db.main.${name} AS SELECT * FROM main.${name};`);
                        copied++;
                    }
                    await connection.run(`DETACH disk_db;`);
                    returnData.push({
                        json: {
                            success: true,
                            message: `Saved ${copied} tables to ${dest}`,
                        },
                        pairedItem: { item: 0 },
                    });
                }
                else if (op === 'select') {
                    const sql = this.getNodeParameter('sqlQuery', 0);
                    const format = this.getNodeParameter('queryOutputFormat', 0);
                    if (format === 'parquet') {
                        const binaryData = await exportParquet(sql, 'query_output.parquet');
                        returnData.push({
                            json: {
                                rowCount: 'Query output saved to Parquet format',
                            },
                            binary: { data: binaryData },
                            pairedItem: { item: 0 },
                        });
                    }
                    else {
                        const statements = sql
                            .split(';')
                            .map((s) => s.trim())
                            .filter((s) => s.length > 0);
                        for (let i = 0; i < statements.length - 1; i++) {
                            try {
                                await connection.run(`${statements[i]};`);
                            }
                            catch (_s) {
                                this.logger.warn(`Multi-statement SQL: intermediate statement failed (statement ${i + 1}/${statements.length - 1}): ${(statements[i] || '').substring(0, 80)}`, { error: _s.message });
                            }
                        }
                        const lastSql = statements.length > 0
                            ? `${statements[statements.length - 1]};`
                            : sql;
                        const result = await connection.runAndReadAll(lastSql);
                        const rows = result.getRowObjectsJson();
                        for (const row of rows) {
                            returnData.push({
                                json: row,
                                pairedItem: { item: 0 },
                            });
                        }
                    }
                }
                else if (op === 'stateless') {
                    if (!isRemote) {
                        throw new n8n_workflow_1.NodeOperationError(this.getNode(), 'Stateless Quack Query requires Connection Mode to be "Remote". Use "Select (Custom SQL)" for local queries.', { itemIndex: 0 });
                    }
                    const host = credentials.host || 'quack:localhost:9494';
                    const token = credentials.token;
                    const disableSsl = credentials.disableSsl;
                    const sql = this.getNodeParameter('sqlQuery', 0);
                    const escapedSql = sql.replace(/'/g, "''");
                    const tokenArg = token ? `, token := '${token.replace(/'/g, "''")}'` : '';
                    const sslArg = disableSsl ? ', disable_ssl := true' : '';
                    const result = await connection.runAndReadAll(`FROM quack_query('${host.replace(/'/g, "''")}', '${escapedSql}'${tokenArg}${sslArg});`);
                    const rows = result.getRowObjectsJson();
                    for (const row of rows) {
                        returnData.push({
                            json: row,
                            pairedItem: { item: 0 },
                        });
                    }
                }
            }
        }
        catch (error) {
            const msg = error.message;
            if (msg.includes('database is closed') || msg.includes('Connection closed')) {
                return [returnData];
            }
            if (this.continueOnFail()) {
                returnData.push({
                    json: { error: error.message },
                    error: error,
                    pairedItem: { item: 0 },
                });
            }
            else {
                throw new n8n_workflow_1.NodeApiError(this.getNode(), error, {
                    itemIndex: 0,
                });
            }
        }
        finally {
            try {
                connection.closeSync();
            }
            catch (_e) {
            }
        }
        return [returnData];
    }
}
exports.DuckDbQuack = DuckDbQuack;
DuckDbQuack._loadLock = Promise.resolve();
//# sourceMappingURL=DuckDbQuack.node.js.map