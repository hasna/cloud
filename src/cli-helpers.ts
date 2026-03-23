import type { Command } from "commander";
import {
  getCloudConfig,
  getConnectionString,
  createDatabase,
} from "./config.js";
import { syncPush, syncPull, listSqliteTables } from "./sync.js";
import { sendFeedback } from "./feedback.js";
import { getDbPath } from "./dotfile.js";
import { SqliteAdapter, PgAdapter } from "./adapter.js";

/**
 * Register cloud-related subcommands onto an existing Commander program.
 * Services call this to embed cloud sync/feedback commands into their own CLI.
 *
 * @example
 * ```ts
 * import { Command } from "commander";
 * import { registerCloudCommands } from "@hasna/cloud";
 *
 * const program = new Command();
 * registerCloudCommands(program, "my-service");
 * program.parse();
 * ```
 */
export function registerCloudCommands(
  program: Command,
  serviceName: string
): void {
  const cloudCmd = program
    .command("cloud")
    .description("Cloud sync and feedback commands");

  cloudCmd
    .command("status")
    .description("Show cloud config and connection health")
    .action(async () => {
      const config = getCloudConfig();
      console.log("Mode:", config.mode);
      console.log("RDS Host:", config.rds.host || "(not configured)");
      console.log("Service:", serviceName);

      if (config.rds.host && config.rds.username) {
        try {
          const connStr = getConnectionString("postgres");
          const pg = new PgAdapter(connStr);
          pg.get("SELECT 1 as ok");
          console.log("PostgreSQL: connected");
          pg.close();
        } catch (err: any) {
          console.log("PostgreSQL: connection failed —", err?.message);
        }
      }
    });

  cloudCmd
    .command("push")
    .description("Push local data to cloud")
    .option("--tables <tables>", "Comma-separated table names")
    .action((opts) => {
      const config = getCloudConfig();
      if (config.mode === "local") {
        console.error("Error: mode is 'local'. Run `cloud setup` first.");
        process.exit(1);
      }

      const local = new SqliteAdapter(getDbPath(serviceName));
      const cloud = new PgAdapter(getConnectionString(serviceName));

      const tables = opts.tables
        ? opts.tables.split(",").map((t: string) => t.trim())
        : listSqliteTables(local);

      const results = syncPush(local, cloud, {
        tables,
        onProgress: (p) => {
          if (p.phase === "done") {
            console.log(`  ${p.table}: ${p.rowsWritten} rows pushed`);
          }
        },
      });

      local.close();
      cloud.close();

      const total = results.reduce((s, r) => s + r.rowsWritten, 0);
      console.log(`Done. ${total} rows pushed.`);
    });

  cloudCmd
    .command("pull")
    .description("Pull cloud data to local")
    .option("--tables <tables>", "Comma-separated table names")
    .action((opts) => {
      const config = getCloudConfig();
      if (config.mode === "local") {
        console.error("Error: mode is 'local'. Run `cloud setup` first.");
        process.exit(1);
      }

      const local = new SqliteAdapter(getDbPath(serviceName));
      const cloud = new PgAdapter(getConnectionString(serviceName));

      let tables: string[];
      if (opts.tables) {
        tables = opts.tables.split(",").map((t: string) => t.trim());
      } else {
        const rows = cloud.all(
          `SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename`
        );
        tables = rows.map((r: any) => r.tablename);
      }

      const results = syncPull(local, cloud, {
        tables,
        onProgress: (p) => {
          if (p.phase === "done") {
            console.log(`  ${p.table}: ${p.rowsWritten} rows pulled`);
          }
        },
      });

      local.close();
      cloud.close();

      const total = results.reduce((s, r) => s + r.rowsWritten, 0);
      console.log(`Done. ${total} rows pulled.`);
    });

  cloudCmd
    .command("feedback")
    .description("Send feedback")
    .requiredOption("--message <msg>", "Feedback message")
    .option("--email <email>", "Contact email")
    .action(async (opts) => {
      const db = createDatabase({ service: "cloud" });
      const result = await sendFeedback(
        { service: serviceName, message: opts.message, email: opts.email },
        db
      );
      db.close();

      if (result.sent) {
        console.log(`Feedback sent (id: ${result.id})`);
      } else {
        console.log(`Feedback saved locally (id: ${result.id}): ${result.error}`);
      }
    });
}
