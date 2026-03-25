import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import { z } from "zod";

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

export const CloudConfigSchema = z.object({
  rds: z
    .object({
      host: z.string().default(""),
      port: z.number().default(5432),
      username: z.string().default(""),
      password_env: z.string().default("HASNA_RDS_PASSWORD"),
      ssl: z.boolean().default(true),
    })
    .default({}),
  mode: z.enum(["local", "cloud", "hybrid"]).default("local"),
  auto_sync_interval_minutes: z.number().default(0),
  feedback_endpoint: z
    .string()
    .default("https://feedback.hasna.com/api/v1/feedback"),
  sync: z
    .object({
      schedule_minutes: z.number().default(0),
    })
    .default({}),
});

export type CloudConfig = z.infer<typeof CloudConfigSchema>;

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

const CONFIG_DIR = join(homedir(), ".hasna", "cloud");
const CONFIG_PATH = join(CONFIG_DIR, "config.json");

export function getConfigDir(): string {
  return CONFIG_DIR;
}

export function getConfigPath(): string {
  return CONFIG_PATH;
}

// ---------------------------------------------------------------------------
// Read / Write
// ---------------------------------------------------------------------------

export function getCloudConfig(): CloudConfig {
  if (!existsSync(CONFIG_PATH)) {
    return CloudConfigSchema.parse({});
  }
  try {
    const raw = readFileSync(CONFIG_PATH, "utf-8");
    return CloudConfigSchema.parse(JSON.parse(raw));
  } catch {
    return CloudConfigSchema.parse({});
  }
}

export function saveCloudConfig(config: CloudConfig): void {
  mkdirSync(CONFIG_DIR, { recursive: true });
  writeFileSync(CONFIG_PATH, JSON.stringify(config, null, 2) + "\n", "utf-8");
}

// ---------------------------------------------------------------------------
// Connection String
// ---------------------------------------------------------------------------

export function getConnectionString(dbName: string): string {
  const config = getCloudConfig();
  const { host, port, username, password_env, ssl } = config.rds;

  if (!host || !username) {
    throw new Error(
      "Cloud RDS not configured. Run `cloud setup` to configure."
    );
  }

  const password = process.env[password_env] ?? "";
  const sslParam = ssl ? "?sslmode=require" : "";
  return `postgres://${username}:${encodeURIComponent(password)}@${host}:${port}/${dbName}${sslParam}`;
}

// ---------------------------------------------------------------------------
// Database Factory — THE main entry point
// ---------------------------------------------------------------------------

import { SqliteAdapter, PgAdapter, type DbAdapter } from "./adapter.js";
import { getDbPath } from "./dotfile.js";

export interface CreateDatabaseOptions {
  /** Service name — used to locate the SQLite file and PG database. */
  service: string;
  /** Override mode from config. */
  mode?: "local" | "cloud" | "hybrid";
  /** Override the SQLite file path. */
  sqlitePath?: string;
  /** Override the PG connection string. */
  pgConnectionString?: string;
}

/**
 * Create a database adapter based on the current configuration.
 *
 * - `local` mode → SqliteAdapter
 * - `cloud` mode → PgAdapter
 * - `hybrid` mode → SqliteAdapter (PG is used only for sync)
 */
export function createDatabase(options: CreateDatabaseOptions): DbAdapter {
  const config = getCloudConfig();
  const mode = options.mode ?? config.mode;

  if (mode === "cloud") {
    const connStr =
      options.pgConnectionString ?? getConnectionString(options.service);
    return new PgAdapter(connStr);
  }

  // local or hybrid — use SQLite
  const dbPath = options.sqlitePath ?? getDbPath(options.service);
  return new SqliteAdapter(dbPath);
}
