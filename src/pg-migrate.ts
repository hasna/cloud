/**
 * Generic PostgreSQL migration runner.
 *
 * Applies a flat array of SQL migration strings to a PG database.
 * Tracks applied versions in a `_pg_migrations` table.
 * Forward-only — no rollbacks. Migrations must be idempotent.
 *
 * Usage:
 *   import { applyPgMigrations } from "@hasna/cloud";
 *   const result = await applyPgMigrations(connectionString, migrations);
 *
 * Or for service discovery:
 *   import { migrateService, migrateAllServices } from "@hasna/cloud";
 *   await migrateService("todos", connectionString);
 *   await migrateAllServices();  // discovers all installed @hasna/* services
 */

import { PgAdapterAsync } from "./adapter.js";
import { getConnectionString } from "./config.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface PgMigrationResult {
  service: string;
  applied: number[];
  alreadyApplied: number[];
  errors: string[];
  totalMigrations: number;
}

// ---------------------------------------------------------------------------
// Core runner
// ---------------------------------------------------------------------------

/**
 * Apply an array of SQL migrations to a PostgreSQL database.
 *
 * @param connectionString - PG connection string (postgres://...)
 * @param migrations - Ordered array of SQL strings. Index = version number.
 * @param service - Service name for result reporting (default: "unknown")
 */
export async function applyPgMigrations(
  connectionString: string,
  migrations: string[],
  service = "unknown"
): Promise<PgMigrationResult> {
  const pg = new PgAdapterAsync(connectionString);

  const result: PgMigrationResult = {
    service,
    applied: [],
    alreadyApplied: [],
    errors: [],
    totalMigrations: migrations.length,
  };

  try {
    // Create tracking table if it doesn't exist
    await pg.run(
      `CREATE TABLE IF NOT EXISTS _pg_migrations (
        id SERIAL PRIMARY KEY,
        version INT UNIQUE NOT NULL,
        applied_at TIMESTAMPTZ DEFAULT NOW()
      )`
    );

    // Check which migrations are already applied
    const applied = await pg.all(
      "SELECT version FROM _pg_migrations ORDER BY version"
    );
    const appliedSet = new Set(
      applied.map((r: { version: number }) => r.version)
    );

    // Apply new ones in order
    for (let i = 0; i < migrations.length; i++) {
      if (appliedSet.has(i)) {
        result.alreadyApplied.push(i);
        continue;
      }

      try {
        await pg.exec(migrations[i]!);
        await pg.run(
          "INSERT INTO _pg_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING",
          i
        );
        result.applied.push(i);
      } catch (err: any) {
        result.errors.push(
          `Migration ${i}: ${err?.message ?? String(err)}`
        );
        // Stop on first error — don't apply later migrations on broken schema
        break;
      }
    }
  } finally {
    await pg.close();
  }

  return result;
}

// ---------------------------------------------------------------------------
// Service-level helpers
// ---------------------------------------------------------------------------

/**
 * Known service → npm package mapping for PG migration discovery.
 * Each package must export `PG_MIGRATIONS: string[]` from its pg-migrations module.
 */
function getServicePackage(service: string): string {
  return `@hasna/${service}`;
}

/**
 * Dynamically import a service's PG_MIGRATIONS array.
 * Tries multiple import paths:
 *   1. @hasna/<service>/pg-migrations (package.json exports)
 *   2. @hasna/<service>/dist/db/pg-migrations.js (direct dist path)
 */
async function loadServiceMigrations(service: string): Promise<string[] | null> {
  const pkg = getServicePackage(service);

  // Try various export paths
  const paths = [
    `${pkg}/pg-migrations`,
    `${pkg}/dist/db/pg-migrations.js`,
    `${pkg}/dist/db/pg-migrations`,
  ];

  for (const path of paths) {
    try {
      const mod = await import(path);
      if (Array.isArray(mod.PG_MIGRATIONS)) {
        return mod.PG_MIGRATIONS;
      }
      if (mod.default && Array.isArray(mod.default.PG_MIGRATIONS)) {
        return mod.default.PG_MIGRATIONS;
      }
    } catch {
      // Try next path
    }
  }

  return null;
}

/**
 * Migrate a single service's PG database.
 *
 * @param service - Service name (e.g., "todos", "conversations")
 * @param connectionString - Optional override. Default: auto-detected from cloud config.
 */
export async function migrateService(
  service: string,
  connectionString?: string
): Promise<PgMigrationResult> {
  const connStr = connectionString ?? getConnectionString(service);
  const migrations = await loadServiceMigrations(service);

  if (!migrations) {
    return {
      service,
      applied: [],
      alreadyApplied: [],
      errors: [`No PG migrations found for service "${service}"`],
      totalMigrations: 0,
    };
  }

  return applyPgMigrations(connStr, migrations, service);
}

/**
 * Discover all installed @hasna/* services and migrate their PG databases.
 *
 * Discovery: scans ~/.hasna/ for service directories that have local DBs.
 */
export async function migrateAllServices(): Promise<PgMigrationResult[]> {
  const { discoverServices } = await import("./discover.js");
  const services = discoverServices();
  const results: PgMigrationResult[] = [];

  for (const service of services) {
    try {
      const result = await migrateService(service);
      results.push(result);
    } catch (err: any) {
      results.push({
        service,
        applied: [],
        alreadyApplied: [],
        errors: [err?.message ?? String(err)],
        totalMigrations: 0,
      });
    }
  }

  return results;
}

// ---------------------------------------------------------------------------
// PG database creation helper
// ---------------------------------------------------------------------------

/**
 * Ensure a PostgreSQL database exists for a service.
 * Connects to the `postgres` default database and runs CREATE DATABASE.
 */
export async function ensurePgDatabase(service: string): Promise<boolean> {
  const config = (await import("./config.js")).getCloudConfig();
  const { host, port, username, password_env, ssl } = config.rds;

  if (!host || !username) return false;

  const password = process.env[password_env] ?? "";
  const sslParam = ssl ? "?sslmode=require" : "";
  const adminConnStr = `postgres://${username}:${encodeURIComponent(password)}@${host}:${port}/postgres${sslParam}`;

  const pg = new PgAdapterAsync(adminConnStr);
  try {
    // Check if database exists
    const existing = await pg.all(
      `SELECT 1 FROM pg_database WHERE datname = $1`,
      service
    );
    if (existing.length === 0) {
      // CREATE DATABASE can't run inside a transaction
      await pg.exec(`CREATE DATABASE "${service}"`);
      return true; // created
    }
    return false; // already existed
  } finally {
    await pg.close();
  }
}

/**
 * Ensure PG databases exist for all discovered services.
 */
export async function ensureAllPgDatabases(): Promise<Array<{ service: string; created: boolean; error?: string }>> {
  const { discoverServices } = await import("./discover.js");
  const services = discoverServices();
  const results: Array<{ service: string; created: boolean; error?: string }> = [];

  for (const service of services) {
    try {
      const created = await ensurePgDatabase(service);
      results.push({ service, created });
    } catch (err: any) {
      results.push({ service, created: false, error: err?.message ?? String(err) });
    }
  }

  return results;
}
