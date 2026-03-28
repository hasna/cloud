import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import {
  getCloudConfig,
  getConnectionString,
  createDatabase,
} from "./config.js";
import { syncPush, syncPull, listSqliteTables, listPgTables } from "./sync.js";
import { sendFeedback } from "./feedback.js";
import { getDbPath } from "./dotfile.js";
import { SqliteAdapter, PgAdapterAsync } from "./adapter.js";

/**
 * Register cloud-related MCP tools onto an existing MCP server.
 * Services call this to embed cloud sync/feedback tools into their own MCP server.
 *
 * @param migrations - Optional list of SQL statements to run against PG before pushing.
 *   Use this to ensure the cloud schema exists (CREATE TABLE IF NOT EXISTS ...).
 *
 * @example
 * ```ts
 * import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
 * import { registerCloudTools } from "@hasna/cloud";
 *
 * const server = new McpServer({ name: "my-service", version: "0.1.0" });
 * registerCloudTools(server, "my-service", { migrations: PG_MIGRATIONS });
 * ```
 */
export function registerCloudTools(
  server: McpServer,
  serviceName: string,
  opts: { migrations?: string[] } = {}
): void {
  // --- cloud_status ---
  server.tool(
    `${serviceName}_cloud_status`,
    "Show cloud configuration and connection health",
    {},
    async () => {
      const config = getCloudConfig();
      const lines = [
        `Mode: ${config.mode}`,
        `Service: ${serviceName}`,
        `RDS Host: ${config.rds.host || "(not configured)"}`,
      ];

      if (config.rds.host && config.rds.username) {
        try {
          const pg = new PgAdapterAsync(getConnectionString("postgres"));
          await pg.get("SELECT 1 as ok");
          lines.push("PostgreSQL: connected");
          await pg.close();
        } catch (err: any) {
          lines.push(`PostgreSQL: failed — ${err?.message}`);
        }
      }

      return { content: [{ type: "text", text: lines.join("\n") }] };
    }
  );

  // --- sync_push ---
  server.tool(
    `${serviceName}_cloud_push`,
    "Push local data to cloud PostgreSQL",
    {
      tables: z
        .string()
        .optional()
        .describe("Comma-separated table names (default: all)"),
    },
    async ({ tables: tablesStr }) => {
      const config = getCloudConfig();
      if (config.mode === "local") {
        return {
          content: [
            { type: "text", text: "Error: cloud mode not configured." },
          ],
          isError: true,
        };
      }

      const local = new SqliteAdapter(getDbPath(serviceName));
      const cloud = new PgAdapterAsync(getConnectionString(serviceName));

      if (opts.migrations?.length) {
        for (const sql of opts.migrations) {
          await cloud.run(sql);
        }
      }

      const tableList = tablesStr
        ? tablesStr.split(",").map((t) => t.trim())
        : listSqliteTables(local);

      const results = await syncPush(local, cloud, { tables: tableList });
      local.close();
      await cloud.close();

      const total = results.reduce((s, r) => s + r.rowsWritten, 0);
      return {
        content: [{ type: "text", text: `Pushed ${total} rows across ${tableList.length} table(s).` }],
      };
    }
  );

  // --- sync_pull ---
  server.tool(
    `${serviceName}_cloud_pull`,
    "Pull cloud PostgreSQL data to local",
    {
      tables: z
        .string()
        .optional()
        .describe("Comma-separated table names (default: all)"),
    },
    async ({ tables: tablesStr }) => {
      const config = getCloudConfig();
      if (config.mode === "local") {
        return {
          content: [
            { type: "text", text: "Error: cloud mode not configured." },
          ],
          isError: true,
        };
      }

      const local = new SqliteAdapter(getDbPath(serviceName));
      const cloud = new PgAdapterAsync(getConnectionString(serviceName));

      let tableList: string[];
      if (tablesStr) {
        tableList = tablesStr.split(",").map((t) => t.trim());
      } else {
        try {
          tableList = await listPgTables(cloud);
        } catch {
          local.close();
          await cloud.close();
          return {
            content: [
              { type: "text", text: "Error: failed to list cloud tables." },
            ],
            isError: true,
          };
        }
      }

      const results = await syncPull(cloud, local, { tables: tableList });
      local.close();
      await cloud.close();

      const total = results.reduce((s, r) => s + r.rowsWritten, 0);
      return {
        content: [{ type: "text", text: `Pulled ${total} rows across ${tableList.length} table(s).` }],
      };
    }
  );

  // --- send_feedback ---
  server.tool(
    `${serviceName}_cloud_feedback`,
    "Send feedback for this service",
    {
      message: z.string().describe("Feedback message"),
      email: z.string().optional().describe("Contact email"),
    },
    async ({ message, email }) => {
      const db = createDatabase({ service: "cloud" });
      const result = await sendFeedback(
        { service: serviceName, message, email },
        db
      );
      db.close();

      return {
        content: [
          {
            type: "text",
            text: result.sent
              ? `Feedback sent (id: ${result.id})`
              : `Saved locally (id: ${result.id}): ${result.error}`,
          },
        ],
      };
    }
  );
}
