import type { Icon } from "n8n-workflow";
import { ICredentialType, INodeProperties } from "n8n-workflow";

export class DuckDbQuackApi implements ICredentialType {
  name = "duckDbQuackApi";
  displayName = "DuckDB / Quack Engine Connection API";
  documentationUrl = "https://duckdb.org/docs/current/quack/overview";
  icon = { light: "file:duckdb.svg", dark: "file:duckdb.svg" } as Icon;
  properties: INodeProperties[] = [
    {
      displayName: "Connection Mode",
      name: "connectionType",
      type: "options",
      options: [
        { name: "Local File / In-Memory RAM", value: "local" },
        { name: "Remote Quack Server (HTTP/2 Protocol)", value: "remote" },
      ],
      default: "local",
    },
    {
      displayName: "Local File Path",
      name: "filePath",
      type: "string",
      displayOptions: { show: { connectionType: ["local"] } },
      default: ":memory:",
      description:
        'Use ":memory:" for volatile scratchpads, or provide an absolute path (e.g., /data/analytics.db).',
    },
    {
      displayName: "Remote Server URI",
      name: "host",
      type: "string",
      displayOptions: { show: { connectionType: ["remote"] } },
      default: "quack:localhost:9494",
      placeholder: "quack:hostname:port",
    },
    {
      displayName: "Authentication Token",
      name: "token",
      type: "string",
      typeOptions: { password: true },
      displayOptions: { show: { connectionType: ["remote"] } },
      default: "",
    },
    {
      displayName: "Disable SSL Encryption",
      name: "disableSsl",
      type: "boolean",
      displayOptions: { show: { connectionType: ["remote"] } },
      default: false,
      description:
        "Check this option ONLY if connecting over a trusted local private subnet without an HTTPS proxy layer.",
    },
    {
      displayName: "Auto-Install Additional Extensions",
      name: "autoLoadExtensions",
      type: "string",
      default: "",
      placeholder: "spatial, postgres, fts",
      description:
        'Optional: comma-separated list of extensions to load. Supports community syntax (e.g., "spatial, gsheets FROM community"). httpfs is auto-loaded. Leave empty to skip. DuckDB autoloads known extensions on demand.',
    },
  ];
}
