import { existsSync, readdirSync, statSync } from "fs";
import { join } from "path";
import { getCloudConfig, getConnectionString } from "./config.js";
import { SqliteAdapter, PgAdapterAsync } from "./adapter.js";
import { incrementalSyncPush } from "./sync-incremental.js";
import { getDataDir, getHasnaDir } from "./dotfile.js";
import { listSqliteTables } from "./sync.js";

// ---------------------------------------------------------------------------
// Service discovery
// ---------------------------------------------------------------------------

/**
 * Discover services under `~/.hasna/` that have a `<service>.db` SQLite file.
 * Returns an array of service names.
 */
export function discoverSyncableServices(): string[] {
  const hasnaDir = getHasnaDir();
  const services: string[] = [];

  try {
    const entries = readdirSync(hasnaDir, { withFileTypes: true });
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      const dbPath = join(hasnaDir, entry.name, `${entry.name}.db`);
      if (existsSync(dbPath)) {
        services.push(entry.name);
      }
    }
  } catch {
    // ~/.hasna/ may not exist yet
  }

  return services;
}

// ---------------------------------------------------------------------------
// Sync runner
// ---------------------------------------------------------------------------

export interface ScheduledSyncResult {
  service: string;
  tables_synced: number;
  total_rows_synced: number;
  errors: string[];
}

/**
 * Run a scheduled sync push for all discovered services.
 *
 * - Skips if mode is `local`.
 * - Opens each service's SQLite DB, discovers tables, and pushes to PG.
 * - Returns per-service results.
 */
export async function runScheduledSync(): Promise<ScheduledSyncResult[]> {
  const config = getCloudConfig();
  if (config.mode === "local") return [];

  const services = discoverSyncableServices();
  const results: ScheduledSyncResult[] = [];

  let remote: PgAdapterAsync | null = null;

  for (const service of services) {
    const result: ScheduledSyncResult = {
      service,
      tables_synced: 0,
      total_rows_synced: 0,
      errors: [],
    };

    try {
      const dbPath = join(getDataDir(service), `${service}.db`);
      if (!existsSync(dbPath)) {
        continue;
      }

      const local = new SqliteAdapter(dbPath);

      const tables = listSqliteTables(local).filter(
        (t) => !t.startsWith("_") && !t.startsWith("sqlite_")
      );

      if (tables.length === 0) {
        local.close();
        continue;
      }

      // Connect to the service's PG database
      try {
        const connStr = getConnectionString(service);
        remote = new PgAdapterAsync(connStr);
      } catch (err: any) {
        result.errors.push(`Connection failed: ${err?.message ?? String(err)}`);
        local.close();
        results.push(result);
        continue;
      }

      const stats = incrementalSyncPush(local, remote as any, tables);

      for (const s of stats) {
        if (s.errors.length === 0) {
          result.tables_synced++;
        }
        result.total_rows_synced += s.synced_rows;
        result.errors.push(...s.errors);
      }

      local.close();
      await remote.close();
      remote = null;
    } catch (err: any) {
      result.errors.push(err?.message ?? String(err));
    }

    results.push(result);
  }

  // Clean up remote if still open
  if (remote) {
    try {
      await remote.close();
    } catch {
      // best-effort
    }
  }

  return results;
}

// ---------------------------------------------------------------------------
// Bun.cron worker entry point
//
// When Bun.cron registers this file, Bun executes it on schedule and looks
// for a default export with a `scheduled()` handler (Cloudflare Workers
// Cron Triggers API convention).
// ---------------------------------------------------------------------------

export default {
  async scheduled() {
    await runScheduledSync();
  },
};
