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
import { readFileSync, existsSync, statSync } from "fs";
import { join, dirname } from "path";
import { homedir } from "os";

const program = new Command();

// Sync log helper — append to ~/.hasna/cloud/sync.log
function logSync(direction: string, service: string, rows: number, errors: number): void {
  try {
    const logDir = join(homedir(), ".hasna", "cloud");
    const logPath = join(logDir, "sync.log");
    const { mkdirSync, appendFileSync } = require("fs");
    mkdirSync(logDir, { recursive: true });
    const ts = new Date().toISOString();
    appendFileSync(logPath, `${ts} ${direction.padEnd(4)} ${service.padEnd(20)} ${rows} rows, ${errors} errors\n`);
  } catch {}
}

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
  .description("Configure cloud settings — interactive wizard or flags")
  .option("--host <host>", "RDS hostname")
  .option("--port <port>", "RDS port", "5432")
  .option("--username <user>", "RDS username")
  .option("--password-env <env>", "Env var for RDS password", "HASNA_RDS_PASSWORD")
  .option("--ssl", "Enable SSL", true)
  .option("--no-ssl", "Disable SSL")
  .option("--mode <mode>", "Mode: local, cloud, or hybrid")
  .option("--schedule <interval>", "Sync schedule (e.g. 30m, 1h)")
  .option("--migrate", "Run PG migrations after setup")
  .option("--pull", "Pull data from cloud after setup")
  .action(async (opts) => {
    const config = getCloudConfig();

    // Auto-detect from existing secrets if no flags provided
    const isAutoDetect = !opts.host && !opts.username;
    if (isAutoDetect) {
      // Try to detect from environment
      const envHost = process.env.HASNA_RDS_HOST;
      const envUser = process.env.HASNA_RDS_USERNAME;
      if (envHost && !config.rds.host) {
        config.rds.host = envHost;
        console.log(`Auto-detected RDS host: ${envHost}`);
      }
      if (envUser && !config.rds.username) {
        config.rds.username = envUser;
        console.log(`Auto-detected RDS username: ${envUser}`);
      }
    }

    if (opts.host) config.rds.host = opts.host;
    if (opts.port) config.rds.port = parseInt(opts.port, 10);
    if (opts.username) config.rds.username = opts.username;
    if (opts.passwordEnv) config.rds.password_env = opts.passwordEnv;
    config.rds.ssl = opts.ssl;
    if (opts.mode) {
      config.mode = opts.mode as CloudConfig["mode"];
    } else if (config.mode === "local" && config.rds.host) {
      // Auto-upgrade to hybrid if host is configured
      config.mode = "hybrid";
      console.log("Mode set to: hybrid (auto-upgraded from local)");
    }

    saveCloudConfig(config);
    console.log("\n✓ Configuration saved\n");

    // Validate connection
    const password = process.env[config.rds.password_env];
    if (!password) {
      console.error(`✗ ${config.rds.password_env} not set in environment`);
      console.error(`  Add it to ~/.secrets/hasna/rds/live.env and source it`);
      return;
    }

    if (config.rds.host) {
      process.stdout.write("Testing PG connection... ");
      try {
        const connStr = getConnectionString("postgres");
        const pg = new PgAdapterAsync(connStr);
        await pg.all("SELECT 1");
        await pg.close();
        console.log("✓ Connected\n");
      } catch (err: any) {
        console.log(`✗ Failed: ${err?.message ?? String(err)}`);
        return;
      }

      // Create databases + run migrations
      if (opts.migrate !== false) {
        console.log("Creating databases & running migrations...");
        const dbResults = await ensureAllPgDatabases();
        const created = dbResults.filter(r => r.created);
        if (created.length > 0) {
          console.log(`  Created ${created.length} database(s): ${created.map(r => r.service).join(", ")}`);
        }

        const migResults = await migrateAllServices();
        const applied = migResults.filter(r => r.applied.length > 0);
        const totalApplied = migResults.reduce((s, r) => s + r.applied.length, 0);
        if (totalApplied > 0) {
          console.log(`  Applied ${totalApplied} migration(s) across ${applied.length} service(s)`);
        } else {
          console.log("  All migrations up to date");
        }
        console.log("");
      }

      // Set up sync schedule
      if (opts.schedule) {
        try {
          const minutes = parseInterval(opts.schedule);
          await registerSyncSchedule(minutes);
          console.log(`✓ Sync scheduled every ${minutes}m\n`);
        } catch (err: any) {
          console.error(`✗ Schedule failed: ${err?.message}`);
        }
      }

      // Pull data
      if (opts.pull) {
        console.log("Pulling data from cloud...");
        const services = discoverServices();
        for (const service of services) {
          try {
            const dbPath = getDbPath(service);
            const local = new SqliteAdapter(dbPath);
            const connStr = getConnectionString(service);
            const cloud = new PgAdapterAsync(connStr);
            const tables = (await listPgTables(cloud)).filter((t) => !isSyncExcludedTable(t));
            if (tables.length > 0) {
              const results = await syncPull(cloud, local, { tables });
              const written = results.reduce((s, r) => s + r.rowsWritten, 0);
              if (written > 0) console.log(`  ${service}: ${written} rows`);
            }
            local.close();
            await cloud.close();
          } catch {}
        }
        console.log("");
      }
    }

    console.log("Setup complete. Run `cloud doctor` to verify everything.");
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
  .option("--dry-run", "Preview what would be synced without executing")
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

      if (opts.dryRun) {
        const rowCounts = tables.map((t) => {
          try { const r = local.get(`SELECT COUNT(*) as cnt FROM "${t}"`); return `${t}: ${r?.cnt ?? 0} rows`; } catch { return `${t}: ?`; }
        });
        console.log(`[${service}] Would push ${tables.length} table(s): ${rowCounts.join(", ")}`);
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
      logSync("push", service, totalWritten, totalErrors);

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
  .option("--dry-run", "Preview what would be synced without executing")
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

      if (opts.dryRun) {
        console.log(`[${service}] Would pull ${tables.length} table(s): ${tables.join(", ")}`);
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
      logSync("pull", service, totalWritten, totalErrors);

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
// cloud sync status
// ---------------------------------------------------------------------------

syncCmd
  .command("status")
  .description("Show sync status for all discovered services")
  .option("--service <name>", "Show status for a single service")
  .option("--json", "Output as JSON")
  .action(async (opts) => {
    const services = opts.service ? [opts.service] : discoverServices();
    const statuses: Array<{ service: string; localDb: string | null; localSize: string; tables: number; pgReachable: boolean }> = [];

    for (const service of services) {
      const dbPath = getDbPath(service);
      const localExists = existsSync(dbPath);
      let localSize = "—";
      let tableCount = 0;

      if (localExists) {
        try {
          const stat = statSync(dbPath);
          localSize = stat.size > 1024 * 1024
            ? `${(stat.size / 1024 / 1024).toFixed(1)}MB`
            : `${(stat.size / 1024).toFixed(0)}KB`;
        } catch {}

        try {
          const local = new SqliteAdapter(dbPath);
          const tables = listSqliteTables(local);
          tableCount = tables.length;
          local.close();
        } catch {}
      }

      let pgReachable = false;
      try {
        const connStr = getConnectionString(service);
        const pg = new PgAdapterAsync(connStr);
        await pg.all("SELECT 1");
        pgReachable = true;
        await pg.close();
      } catch {}

      statuses.push({
        service,
        localDb: localExists ? dbPath : null,
        localSize,
        tables: tableCount,
        pgReachable,
      });
    }

    if (opts.json) {
      console.log(JSON.stringify(statuses, null, 2));
    } else {
      const config = getCloudConfig();
      console.log(`Mode: ${config.mode}`);
      console.log(`Services: ${statuses.length}\n`);

      for (const s of statuses) {
        if (!s.localDb && !s.pgReachable) continue; // skip empty entries
        const pgIcon = s.pgReachable ? "✓" : "✗";
        console.log(`  ${s.service.padEnd(20)} ${s.localSize.padStart(8)}  ${String(s.tables).padStart(3)} tables  PG: ${pgIcon}`);
      }

      const withData = statuses.filter(s => s.localDb);
      const pgOk = statuses.filter(s => s.pgReachable);
      console.log(`\n${withData.length} with local data, ${pgOk.length} with PG connection`);
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
// cloud doctor
// ---------------------------------------------------------------------------

program
  .command("doctor")
  .description("Comprehensive health check for cloud sync setup")
  .action(async () => {
    const checks: Array<{ name: string; status: "pass" | "fail" | "warn"; detail: string }> = [];

    // 1. Config file
    const configPath = join(homedir(), ".hasna", "cloud", "config.json");
    if (existsSync(configPath)) {
      checks.push({ name: "Config file", status: "pass", detail: configPath });
    } else {
      checks.push({ name: "Config file", status: "fail", detail: "Missing. Run `cloud setup`." });
    }

    // 2. Mode
    const config = getCloudConfig();
    if (config.mode === "hybrid" || config.mode === "cloud") {
      checks.push({ name: "Sync mode", status: "pass", detail: config.mode });
    } else {
      checks.push({ name: "Sync mode", status: "fail", detail: `"${config.mode}" — sync disabled. Run \`cloud setup --mode hybrid\`.` });
    }

    // 3. RDS host configured
    if (config.rds.host) {
      checks.push({ name: "RDS host", status: "pass", detail: config.rds.host });
    } else {
      checks.push({ name: "RDS host", status: "fail", detail: "Not configured. Run `cloud setup`." });
    }

    // 4. RDS password
    const password = process.env[config.rds.password_env];
    if (password) {
      checks.push({ name: "RDS password", status: "pass", detail: `${config.rds.password_env} is set` });
    } else {
      checks.push({ name: "RDS password", status: "fail", detail: `${config.rds.password_env} not in environment. Add to ~/.secrets/hasna/rds/live.env` });
    }

    // 5. PG connection
    if (config.rds.host && password) {
      try {
        const connStr = getConnectionString("postgres");
        const pg = new PgAdapterAsync(connStr);
        await pg.all("SELECT 1");
        await pg.close();
        checks.push({ name: "PG connection", status: "pass", detail: "Connected" });
      } catch (err: any) {
        checks.push({ name: "PG connection", status: "fail", detail: err?.message ?? String(err) });
      }
    } else {
      checks.push({ name: "PG connection", status: "fail", detail: "Skipped — missing host or password" });
    }

    // 6. SSL CA cert
    const caPath = process.env.NODE_EXTRA_CA_CERTS;
    if (caPath && existsSync(caPath)) {
      checks.push({ name: "SSL CA cert", status: "pass", detail: caPath });
    } else if (caPath) {
      checks.push({ name: "SSL CA cert", status: "warn", detail: `NODE_EXTRA_CA_CERTS set but file missing: ${caPath}` });
    } else {
      checks.push({ name: "SSL CA cert", status: "warn", detail: "NODE_EXTRA_CA_CERTS not set. May cause SSL errors on some systems." });
    }

    // 7. Services with local data
    const services = discoverServices();
    checks.push({ name: "Local services", status: services.length > 0 ? "pass" : "warn", detail: `${services.length} found in ~/.hasna/` });

    // 8. Sync schedule
    const schedule = getSyncScheduleStatus();
    if (schedule.registered) {
      checks.push({ name: "Sync schedule", status: "pass", detail: `Every ${schedule.schedule_minutes}m (${schedule.mechanism})` });
    } else {
      checks.push({ name: "Sync schedule", status: "warn", detail: "Not configured. Run `cloud sync schedule --every 30m`." });
    }

    // Print results
    console.log("Cloud Doctor\n");
    for (const c of checks) {
      const icon = c.status === "pass" ? "✓" : c.status === "fail" ? "✗" : "⚠";
      console.log(`  ${icon} ${c.name.padEnd(20)} ${c.detail}`);
    }

    const fails = checks.filter(c => c.status === "fail").length;
    const warns = checks.filter(c => c.status === "warn").length;
    console.log(`\n${checks.length} checks: ${checks.length - fails - warns} passed, ${warns} warnings, ${fails} failed`);

    if (fails > 0) process.exit(1);
  });

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

program.parse();
