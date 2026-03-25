/**
 * Service discovery — find all @hasna/* services that have local data.
 *
 * Scans ~/.hasna/ for directories containing .db files.
 * Also maintains a known-services list for migration discovery.
 */

import { readdirSync, existsSync, statSync } from "fs";
import { join } from "path";
import { homedir } from "os";

// ---------------------------------------------------------------------------
// Known services with PG migration support
// ---------------------------------------------------------------------------

/**
 * Services known to have pg-migrations.ts files.
 * Updated as services are onboarded to cloud sync.
 */
export const KNOWN_PG_SERVICES: string[] = [
  "assistants",
  "attachments",
  "brains",
  "configs",
  "connectors",
  "contacts",
  "context",
  "conversations",
  "crawl",
  "deployment",
  "economy",
  "emails",
  "files",
  "hooks",
  "implementations",
  "logs",
  "mcps",
  "mementos",
  "microservices",
  "predictor",
  "prompts",
  "recordings",
  "researcher",
  "sandboxes",
  "search",
  "secrets",
  "sessions",
  "signatures",
  "skills",
  "telephony",
  "terminal",
  "testers",
  "tickets",
  "todos",
  "wallets",
];

/**
 * Tables to exclude from sync — internal SQLite tables, FTS virtual tables,
 * migration tracking, and sync metadata.
 */
export const SYNC_EXCLUDED_TABLE_PATTERNS = [
  /^sqlite_/,
  /_fts$/,
  /_fts_/,
  /^_sync_/,
  /^_pg_migrations$/,
];

/**
 * Check if a table name should be excluded from sync.
 */
export function isSyncExcludedTable(table: string): boolean {
  return SYNC_EXCLUDED_TABLE_PATTERNS.some((p) => p.test(table));
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

/**
 * Discover all services that have local data directories under ~/.hasna/.
 * Returns service names (directory names), sorted alphabetically.
 */
export function discoverServices(): string[] {
  const dataDir = join(homedir(), ".hasna");
  if (!existsSync(dataDir)) return [];

  try {
    const entries = readdirSync(dataDir, { withFileTypes: true });
    return entries
      .filter((e) => {
        if (!e.isDirectory()) return false;
        // Skip cloud config dir and other non-service dirs
        if (e.name === "cloud" || e.name.startsWith(".")) return false;
        return true;
      })
      .map((e) => e.name)
      .sort();
  } catch {
    return [];
  }
}

/**
 * Discover services that have both local data AND known PG migrations.
 */
export function discoverSyncableServices(): string[] {
  const local = discoverServices();
  const pgSet = new Set(KNOWN_PG_SERVICES);
  return local.filter((s) => pgSet.has(s));
}

/**
 * Get the local SQLite database path for a service.
 * Convention: ~/.hasna/<service>/<service>.db
 */
export function getServiceDbPath(service: string): string | null {
  const dataDir = join(homedir(), ".hasna", service);
  if (!existsSync(dataDir)) return null;

  // Try common naming conventions
  const candidates = [
    join(dataDir, `${service}.db`),
    join(dataDir, "data.db"),
    join(dataDir, "database.db"),
  ];

  // Also scan for any .db file
  try {
    const files = readdirSync(dataDir);
    for (const f of files) {
      if (f.endsWith(".db") && !f.endsWith("-wal") && !f.endsWith("-shm")) {
        candidates.push(join(dataDir, f));
      }
    }
  } catch {}

  for (const p of candidates) {
    if (existsSync(p)) return p;
  }

  return null;
}
