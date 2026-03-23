#!/usr/bin/env bun
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import {
  getCloudConfig,
  getConnectionString,
  createDatabase,
} from "../config.js";
import { syncPush, syncPull, listSqliteTables } from "../sync.js";
import { sendFeedback } from "../feedback.js";
import { getDbPath } from "../dotfile.js";
import { SqliteAdapter, PgAdapter } from "../adapter.js";

// ---------------------------------------------------------------------------
// Server setup
// ---------------------------------------------------------------------------

const server = new McpServer({
  name: "cloud",
  version: "0.1.0",
});

// ---------------------------------------------------------------------------
// cloud_status
// ---------------------------------------------------------------------------

server.tool("cloud_status", "Show cloud configuration and connection health", {}, async () => {
  const config = getCloudConfig();
  const lines: string[] = [
    `Mode: ${config.mode}`,
    `RDS Host: ${config.rds.host || "(not configured)"}`,
    `RDS Port: ${config.rds.port}`,
    `RDS Username: ${config.rds.username || "(not configured)"}`,
    `SSL: ${config.rds.ssl}`,
    `Auto-sync: ${config.auto_sync_interval_minutes ? `${config.auto_sync_interval_minutes} min` : "disabled"}`,
  ];

  // Check PG connection if configured
  if (config.rds.host && config.rds.username) {
    try {
      const connStr = getConnectionString("postgres");
      const pg = new PgAdapter(connStr);
      pg.get("SELECT 1 as ok");
      lines.push("PostgreSQL: connected");
      pg.close();
    } catch (err: any) {
      lines.push(`PostgreSQL: connection failed — ${err?.message}`);
    }
  }

  return { content: [{ type: "text", text: lines.join("\n") }] };
});

// ---------------------------------------------------------------------------
// sync_push
// ---------------------------------------------------------------------------

server.tool(
  "sync_push",
  "Push local SQLite data to cloud PostgreSQL",
  {
    service: z.string().describe("Service name"),
    tables: z
      .string()
      .optional()
      .describe("Comma-separated table names (default: all)"),
  },
  async ({ service, tables: tablesStr }) => {
    const config = getCloudConfig();
    if (config.mode === "local") {
      return {
        content: [
          {
            type: "text",
            text: "Error: mode is 'local'. Configure cloud mode first via `cloud setup`.",
          },
        ],
        isError: true,
      };
    }

    const dbPath = getDbPath(service);
    const local = new SqliteAdapter(dbPath);
    const connStr = getConnectionString(service);
    const cloud = new PgAdapter(connStr);

    let tableList: string[];
    if (tablesStr) {
      tableList = tablesStr.split(",").map((t) => t.trim());
    } else {
      tableList = listSqliteTables(local);
    }

    const results = syncPush(local, cloud, { tables: tableList });

    local.close();
    cloud.close();

    const totalWritten = results.reduce((s, r) => s + r.rowsWritten, 0);
    const totalErrors = results.reduce((s, r) => s + r.errors.length, 0);
    const lines = [
      `Pushed ${tableList.length} table(s): ${totalWritten} rows, ${totalErrors} errors.`,
    ];
    for (const r of results) {
      lines.push(
        `  ${r.table}: ${r.rowsWritten} written, ${r.rowsSkipped} skipped`
      );
      for (const e of r.errors) {
        lines.push(`    ERROR: ${e}`);
      }
    }

    return { content: [{ type: "text", text: lines.join("\n") }] };
  }
);

// ---------------------------------------------------------------------------
// sync_pull
// ---------------------------------------------------------------------------

server.tool(
  "sync_pull",
  "Pull cloud PostgreSQL data to local SQLite",
  {
    service: z.string().describe("Service name"),
    tables: z
      .string()
      .optional()
      .describe("Comma-separated table names (default: all)"),
  },
  async ({ service, tables: tablesStr }) => {
    const config = getCloudConfig();
    if (config.mode === "local") {
      return {
        content: [
          {
            type: "text",
            text: "Error: mode is 'local'. Configure cloud mode first via `cloud setup`.",
          },
        ],
        isError: true,
      };
    }

    const dbPath = getDbPath(service);
    const local = new SqliteAdapter(dbPath);
    const connStr = getConnectionString(service);
    const cloud = new PgAdapter(connStr);

    let tableList: string[];
    if (tablesStr) {
      tableList = tablesStr.split(",").map((t) => t.trim());
    } else {
      try {
        const rows = cloud.all(
          `SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename`
        );
        tableList = rows.map((r: any) => r.tablename);
      } catch {
        local.close();
        cloud.close();
        return {
          content: [
            { type: "text", text: "Error: failed to list tables from cloud." },
          ],
          isError: true,
        };
      }
    }

    const results = syncPull(local, cloud, { tables: tableList });

    local.close();
    cloud.close();

    const totalWritten = results.reduce((s, r) => s + r.rowsWritten, 0);
    const totalErrors = results.reduce((s, r) => s + r.errors.length, 0);
    const lines = [
      `Pulled ${tableList.length} table(s): ${totalWritten} rows, ${totalErrors} errors.`,
    ];
    for (const r of results) {
      lines.push(
        `  ${r.table}: ${r.rowsWritten} written, ${r.rowsSkipped} skipped`
      );
      for (const e of r.errors) {
        lines.push(`    ERROR: ${e}`);
      }
    }

    return { content: [{ type: "text", text: lines.join("\n") }] };
  }
);

// ---------------------------------------------------------------------------
// send_feedback
// ---------------------------------------------------------------------------

server.tool(
  "send_feedback",
  "Send feedback for a service",
  {
    service: z.string().describe("Service name"),
    message: z.string().describe("Feedback message"),
    email: z.string().optional().describe("Contact email"),
    version: z.string().optional().describe("Service version"),
  },
  async ({ service, message, email, version }) => {
    const db = createDatabase({ service: "cloud" });
    const result = await sendFeedback(
      { service, message, email, version },
      db
    );
    db.close();

    if (result.sent) {
      return {
        content: [
          {
            type: "text",
            text: `Feedback sent successfully (id: ${result.id})`,
          },
        ],
      };
    }
    return {
      content: [
        {
          type: "text",
          text: `Feedback saved locally (id: ${result.id}). Remote send failed: ${result.error}`,
        },
      ],
    };
  }
);

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((err) => {
  console.error("cloud-mcp failed to start:", err);
  process.exit(1);
});
