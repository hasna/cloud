#!/usr/bin/env bun
import { Command } from "commander";
import {
  getCloudConfig,
  saveCloudConfig,
  getConnectionString,
  createDatabase,
  type CloudConfig,
} from "../config.js";
import { syncPush, syncPull, listSqliteTables, listPgTables } from "../sync.js";
import { saveFeedback, sendFeedback } from "../feedback.js";
import { migrateDotfile, getDataDir, getDbPath } from "../dotfile.js";
import { SqliteAdapter, PgAdapterAsync } from "../adapter.js";
import {
  registerSyncSchedule,
  removeSyncSchedule,
  getSyncScheduleStatus,
  parseInterval,
} from "../sync-schedule.js";
import {
  runScheduledSync,
  discoverSyncableServices,
} from "../scheduled-sync.js";
import { discoverServices, isSyncExcludedTable } from "../discover.js";
import { migrateService, migrateAllServices, ensurePgDatabase, ensureAllPgDatabases } from "../pg-migrate.js";
import { readFileSync } from "fs";
import { join, dirname } from "path";

const program = new Command();

program
  .name("cloud")
  .description(
    "Shared cloud infrastructure — database adapter, sync engine, feedback, dotfile migration"
  )
  .version("0.1.8");

// ---------------------------------------------------------------------------
// cloud setup
// ---------------------------------------------------------------------------

program
  .command("setup")
  .description("Configure cloud settings")
  .option("--host <host>", "RDS hostname")
  .option("--port <port>", "RDS port", "5432")
  .option("--username <user>", "RDS username")
  .option("--password-env <env>", "Env var for RDS password", "HASNA_RDS_PASSWORD")
  .option("--ssl", "Enable SSL", true)
  .option("--no-ssl", "Disable SSL")
  .option("--mode <mode>", "Mode: local, cloud, or hybrid", "local")
  .option("--sync-interval <minutes>", "Auto-sync interval in minutes", "0")
  .action((opts) => {
    const config = getCloudConfig();

    if (opts.host) config.rds.host = opts.host;
    if (opts.port) config.rds.port = parseInt(opts.port, 10);
    if (opts.username) config.rds.username = opts.username;
    if (opts.passwordEnv) config.rds.password_env = opts.passwordEnv;
    config.rds.ssl = opts.ssl;
    if (opts.mode) config.mode = opts.mode as CloudConfig["mode"];
    if (opts.syncInterval)
      config.auto_sync_interval_minutes = parseInt(opts.syncInterval, 10);

    saveCloudConfig(config);
    console.log("Cloud configuration saved.");
    console.log(JSON.stringify(config, null, 2));
  });

// ---------------------------------------------------------------------------
// cloud status
// ---------------------------------------------------------------------------

program
  .command("status")
  .description("Show current cloud configuration and connection health")
  .action(async () => {
    const config = getCloudConfig();
    console.log("Mode:", config.mode);
    console.log("RDS Host:", config.rds.host || "(not configured)");
    console.log("RDS Port:", config.rds.port);
    console.log("RDS Username:", config.rds.username || "(not configured)");
    console.log("SSL:", config.rds.ssl);
    console.log(
      "Auto-sync interval:",
      config.auto_sync_interval_minutes
        ? `${config.auto_sync_interval_minutes} minutes`
        : "disabled"
    );

    // Check PG connection if configured
    if (config.rds.host && config.rds.username) {
      console.log("\nChecking PostgreSQL connection...");
      try {
        const connStr = getConnectionString("postgres");
        const pg = new PgAdapterAsync(connStr);
        const row = await pg.get("SELECT 1 as ok");
        if (row?.ok === 1) {
          console.log("PostgreSQL: connected");
        }
        await pg.close();
      } catch (err: any) {
        console.log("PostgreSQL: connection failed —", err?.message);
      }
    }
  });

// ---------------------------------------------------------------------------
// cloud sync push
// ---------------------------------------------------------------------------

const syncCmd = program.command("sync").description("Sync data between local and cloud");

syncCmd
  .command("push")
  .description("Push local data to cloud")
  .option("--service <name>", "Service name")
  .option("--all", "Push all discovered services")
  .option("--tables <tables>", "Comma-separated table names (default: all)")
  .action(async (opts) => {
    const config = getCloudConfig();
    if (config.mode === "local") {
      console.error(
        "Error: mode is 'local'. Run `cloud setup --mode hybrid` or `--mode cloud` first."
      );
      process.exit(1);
    }

    if (!opts.service && !opts.all) {
      console.error("Error: specify --service <name> or --all");
      process.exit(1);
    }

    const services = opts.all ? discoverServices() : [opts.service];
    let grandTotalWritten = 0;
    let grandTotalErrors = 0;

    for (const service of services) {
      const dbPath = getDbPath(service);
      let local: SqliteAdapter;
      try {
        local = new SqliteAdapter(dbPath);
      } catch {
        if (opts.all) continue; // skip services without a local DB
        console.error(`No local database found for service "${service}"`);
        process.exit(1);
        return;
      }

      let tables: string[];
      if (opts.tables) {
        tables = opts.tables.split(",").map((t: string) => t.trim());
      } else {
        tables = listSqliteTables(local).filter((t) => !isSyncExcludedTable(t));
      }

      if (tables.length === 0) {
        local.close();
        continue;
      }

      console.log(`[${service}] Pushing ${tables.length} table(s) to cloud...`);

      let connStr: string;
      try {
        connStr = getConnectionString(service);
      } catch (err: any) {
        console.error(`  [${service}] ${err?.message ?? String(err)}`);
        local.close();
        grandTotalErrors++;
        continue;
      }
      const cloud = new PgAdapterAsync(connStr);

      const results = await syncPush(local, cloud, {
        tables,
        onProgress: (p) => {
          if (p.phase === "done" && !opts.all) {
            console.log(
              `  [${p.currentTableIndex + 1}/${p.totalTables}] ${p.table}: ${p.rowsWritten} rows synced`
            );
          }
        },
      });

      local.close();
      await cloud.close();

      const totalWritten = results.reduce((s, r) => s + r.rowsWritten, 0);
      const totalErrors = results.reduce((s, r) => s + r.errors.length, 0);
      grandTotalWritten += totalWritten;
      grandTotalErrors += totalErrors;

      if (opts.all) {
        console.log(`  ${service}: ${totalWritten} rows pushed${totalErrors > 0 ? `, ${totalErrors} errors` : ""}`);
      } else {
        console.log(`\nDone. ${totalWritten} rows pushed, ${totalErrors} errors.`);
        if (totalErrors > 0) {
          for (const r of results) {
            for (const e of r.errors) {
              console.error(`  ${r.table}: ${e}`);
            }
          }
        }
      }
    }

    if (opts.all) {
      console.log(`\nDone. ${services.length} services, ${grandTotalWritten} rows pushed, ${grandTotalErrors} errors.`);
    }
  });

// ---------------------------------------------------------------------------
// cloud sync pull
// ---------------------------------------------------------------------------

syncCmd
  .command("pull")
  .description("Pull cloud data to local")
  .option("--service <name>", "Service name")
  .option("--all", "Pull all discovered services")
  .option("--tables <tables>", "Comma-separated table names (default: all)")
  .action(async (opts) => {
    const config = getCloudConfig();
    if (config.mode === "local") {
      console.error(
        "Error: mode is 'local'. Run `cloud setup --mode hybrid` or `--mode cloud` first."
      );
      process.exit(1);
    }

    if (!opts.service && !opts.all) {
      console.error("Error: specify --service <name> or --all");
      process.exit(1);
    }

    const services = opts.all ? discoverServices() : [opts.service];
    let grandTotalWritten = 0;
    let grandTotalErrors = 0;

    for (const service of services) {
      const dbPath = getDbPath(service);
      let local: SqliteAdapter;
      try {
        local = new SqliteAdapter(dbPath);
      } catch {
        if (opts.all) continue;
        console.error(`No local database found for service "${service}"`);
        process.exit(1);
        return;
      }

      let connStr: string;
      try {
        connStr = getConnectionString(service);
      } catch (err: any) {
        console.error(`  [${service}] ${err?.message ?? String(err)}`);
        local.close();
        grandTotalErrors++;
        continue;
      }
      const cloud = new PgAdapterAsync(connStr);

      let tables: string[];
      if (opts.tables) {
        tables = opts.tables.split(",").map((t: string) => t.trim());
      } else {
        try {
          tables = (await listPgTables(cloud)).filter((t) => !isSyncExcludedTable(t));
        } catch {
          if (!opts.all) console.error(`Failed to list tables from cloud for "${service}".`);
          local.close();
          await cloud.close();
          if (!opts.all) { process.exit(1); return; }
          grandTotalErrors++;
          continue;
        }
      }

      if (tables.length === 0) {
        local.close();
        await cloud.close();
        continue;
      }

      if (!opts.all) console.log(`Pulling ${tables.length} table(s) from cloud...`);

      const results = await syncPull(cloud, local, {
        tables,
        onProgress: (p) => {
          if (p.phase === "done" && !opts.all) {
            console.log(
              `  [${p.currentTableIndex + 1}/${p.totalTables}] ${p.table}: ${p.rowsWritten} rows synced`
            );
          }
        },
      });

      local.close();
      await cloud.close();

      const totalWritten = results.reduce((s, r) => s + r.rowsWritten, 0);
      const totalErrors = results.reduce((s, r) => s + r.errors.length, 0);
      grandTotalWritten += totalWritten;
      grandTotalErrors += totalErrors;

      if (opts.all) {
        if (totalWritten > 0 || totalErrors > 0) {
          console.log(`  ${service}: ${totalWritten} rows pulled${totalErrors > 0 ? `, ${totalErrors} errors` : ""}`);
        }
      } else {
        console.log(`\nDone. ${totalWritten} rows pulled, ${totalErrors} errors.`);
        if (totalErrors > 0) {
          for (const r of results) {
            for (const e of r.errors) {
              console.error(`  ${r.table}: ${e}`);
            }
          }
        }
      }
    }

    if (opts.all) {
      console.log(`\nDone. ${services.length} services, ${grandTotalWritten} rows pulled, ${grandTotalErrors} errors.`);
    }
  });

// ---------------------------------------------------------------------------
// cloud migrate-pg
// ---------------------------------------------------------------------------

program
  .command("migrate-pg")
  .description("Apply PG migrations for services")
  .option("--service <name>", "Service name")
  .option("--all", "Migrate all discovered services")
  .option("--create-db", "Create PG databases if they don't exist (default: true)", true)
  .action(async (opts) => {
    if (!opts.service && !opts.all) {
      console.error("Error: specify --service <name> or --all");
      process.exit(1);
    }

    if (opts.all) {
      if (opts.createDb) {
        console.log("Ensuring PG databases exist...");
        const dbResults = await ensureAllPgDatabases();
        for (const r of dbResults) {
          if (r.created) console.log(`  Created database: ${r.service}`);
          if (r.error) console.error(`  ${r.service}: ${r.error}`);
        }
      }

      console.log("\nRunning PG migrations...");
      const results = await migrateAllServices();
      let totalApplied = 0;
      let totalErrors = 0;

      for (const r of results) {
        totalApplied += r.applied.length;
        totalErrors += r.errors.length;
        if (r.applied.length > 0 || r.errors.length > 0) {
          console.log(`  ${r.service}: ${r.applied.length} applied, ${r.alreadyApplied.length} existing${r.errors.length > 0 ? `, ${r.errors.length} errors` : ""}`);
          for (const e of r.errors) {
            console.error(`    ${e}`);
          }
        }
      }

      console.log(`\nDone. ${results.length} services, ${totalApplied} migrations applied, ${totalErrors} errors.`);
    } else {
      if (opts.createDb) {
        try {
          const created = await ensurePgDatabase(opts.service);
          if (created) console.log(`Created database: ${opts.service}`);
        } catch (err: any) {
          console.error(`Failed to create database: ${err?.message ?? String(err)}`);
        }
      }

      const result = await migrateService(opts.service);
      console.log(`${result.service}: ${result.applied.length} applied, ${result.alreadyApplied.length} existing`);
      for (const e of result.errors) {
        console.error(`  ${e}`);
      }
    }
  });

// ---------------------------------------------------------------------------
// cloud sync schedule
// ---------------------------------------------------------------------------

syncCmd
  .command("schedule")
  .description("Manage scheduled background sync")
  .option("--every <interval>", "Set sync interval (e.g. 5m, 10m, 1h)")
  .option("--off", "Disable scheduled sync")
  .option("--now", "Run a one-off sync immediately")
  .action(async (opts) => {
    // --off: remove schedule
    if (opts.off) {
      try {
        await removeSyncSchedule();
        console.log("Scheduled sync disabled.");
      } catch (err: any) {
        console.error("Failed to remove schedule:", err?.message);
        process.exit(1);
      }
      return;
    }

    // --now: run sync immediately (one-shot)
    if (opts.now) {
      const config = getCloudConfig();
      if (config.mode === "local") {
        console.error(
          "Error: mode is 'local'. Run `cloud setup --mode hybrid` or `--mode cloud` first."
        );
        process.exit(1);
      }

      console.log("Running sync now...");
      const services = discoverSyncableServices();
      console.log(`Discovered ${services.length} service(s): ${services.join(", ") || "(none)"}`);

      const results = await runScheduledSync();
      for (const r of results) {
        const status = r.errors.length === 0 ? "ok" : "errors";
        console.log(
          `  ${r.service}: ${r.tables_synced} table(s), ${r.total_rows_synced} row(s) [${status}]`
        );
        for (const e of r.errors) {
          console.error(`    ${e}`);
        }
      }

      if (results.length === 0) {
        console.log("No services synced (mode may be local or no databases found).");
      } else {
        const totalRows = results.reduce((s, r) => s + r.total_rows_synced, 0);
        const totalErrors = results.reduce((s, r) => s + r.errors.length, 0);
        console.log(`\nDone. ${totalRows} rows synced, ${totalErrors} errors.`);
      }
      return;
    }

    // --every: register schedule
    if (opts.every) {
      try {
        const minutes = parseInterval(opts.every);
        await registerSyncSchedule(minutes);
        console.log(
          `Scheduled sync registered: every ${minutes} minute(s).`
        );
      } catch (err: any) {
        console.error("Failed to register schedule:", err?.message);
        process.exit(1);
      }
      return;
    }

    // No flags: show current status
    const status = getSyncScheduleStatus();
    if (status.registered) {
      console.log("Scheduled sync: enabled");
      console.log(`  Interval: ${status.schedule_minutes} minute(s)`);
      console.log(`  Cron expression: ${status.cron_expression}`);
    } else {
      console.log("Scheduled sync: disabled");
      console.log(
        "\nTo enable, run: cloud sync schedule --every 5m"
      );
    }

    // Also show discoverable services
    const services = discoverSyncableServices();
    if (services.length > 0) {
      console.log(`\nSyncable services (${services.length}):`);
      for (const s of services) {
        console.log(`  - ${s}`);
      }
    } else {
      console.log("\nNo syncable services found (no .db files in ~/.hasna/).");
    }
  });

// ---------------------------------------------------------------------------
// cloud feedback send
// ---------------------------------------------------------------------------

program
  .command("feedback")
  .description("Send feedback")
  .requiredOption("--service <name>", "Service name")
  .requiredOption("--message <msg>", "Feedback message")
  .option("--email <email>", "Contact email")
  .option("--version <ver>", "Service version")
  .action(async (opts) => {
    const db = createDatabase({ service: "cloud" });

    const result = await sendFeedback(
      {
        service: opts.service,
        version: opts.version,
        message: opts.message,
        email: opts.email,
      },
      db
    );

    if (result.sent) {
      console.log(`Feedback sent successfully (id: ${result.id})`);
    } else {
      console.log(
        `Feedback saved locally (id: ${result.id}). Remote send failed: ${result.error}`
      );
    }

    db.close();
  });

// ---------------------------------------------------------------------------
// cloud migrate
// ---------------------------------------------------------------------------

program
  .command("migrate")
  .description("Migrate legacy dotfiles to ~/.hasna/")
  .argument("<service>", "Service name to migrate")
  .action((service) => {
    const migrated = migrateDotfile(service);
    if (migrated.length === 0) {
      console.log(
        `No migration needed for "${service}" — either no legacy dir or already migrated.`
      );
    } else {
      console.log(`Migrated ${migrated.length} file(s) from ~/.${service}/ to ~/.hasna/${service}/:`);
      for (const f of migrated) {
        console.log(`  ${f}`);
      }
    }
  });

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

program.parse();
