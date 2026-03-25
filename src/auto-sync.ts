import { existsSync, readFileSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import type { DbAdapter } from "./adapter.js";
import { PgAdapterAsync } from "./adapter.js";
import { getCloudConfig, getConnectionString } from "./config.js";
import { syncPush, syncPull, listSqliteTables, listPgTables } from "./sync.js";
import { isSyncExcludedTable } from "./discover.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface AutoSyncConfig {
  auto_sync_on_start: boolean;
  auto_sync_on_stop: boolean;
}

export interface AutoSyncContext {
  serviceName: string;
  local: DbAdapter;
  tables: string[];
  config: AutoSyncConfig;
}

export interface AutoSyncResult {
  event: "start" | "stop";
  direction: "pull" | "push";
  success: boolean;
  tables_synced: number;
  total_rows_synced: number;
  errors: string[];
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const AUTO_SYNC_CONFIG_PATH = join(homedir(), ".hasna", "cloud", "config.json");

const DEFAULT_AUTO_SYNC_CONFIG: AutoSyncConfig = {
  auto_sync_on_start: true,
  auto_sync_on_stop: true,
};

export function getAutoSyncConfig(): AutoSyncConfig {
  try {
    if (!existsSync(AUTO_SYNC_CONFIG_PATH)) {
      return { ...DEFAULT_AUTO_SYNC_CONFIG };
    }
    const raw = JSON.parse(readFileSync(AUTO_SYNC_CONFIG_PATH, "utf-8"));
    return {
      auto_sync_on_start:
        typeof raw.auto_sync_on_start === "boolean"
          ? raw.auto_sync_on_start
          : DEFAULT_AUTO_SYNC_CONFIG.auto_sync_on_start,
      auto_sync_on_stop:
        typeof raw.auto_sync_on_stop === "boolean"
          ? raw.auto_sync_on_stop
          : DEFAULT_AUTO_SYNC_CONFIG.auto_sync_on_stop,
    };
  } catch {
    return { ...DEFAULT_AUTO_SYNC_CONFIG };
  }
}

// ---------------------------------------------------------------------------
// Auto-sync execution — async using PgAdapterAsync
// ---------------------------------------------------------------------------

async function executeAutoSync(
  event: "start" | "stop",
  serviceName: string,
  local: DbAdapter,
  tables: string[]
): Promise<AutoSyncResult> {
  const direction = event === "start" ? "pull" : "push";
  const result: AutoSyncResult = {
    event,
    direction,
    success: false,
    tables_synced: 0,
    total_rows_synced: 0,
    errors: [],
  };

  let remote: PgAdapterAsync | null = null;
  try {
    const connStr = getConnectionString(serviceName);
    remote = new PgAdapterAsync(connStr);

    const syncTables = tables.length > 0
      ? tables.filter((t) => !isSyncExcludedTable(t))
      : direction === "push"
        ? listSqliteTables(local).filter((t) => !isSyncExcludedTable(t))
        : (await listPgTables(remote)).filter((t) => !isSyncExcludedTable(t));

    if (syncTables.length === 0) {
      result.success = true;
      return result;
    }

    const results = direction === "pull"
      ? await syncPull(remote, local, { tables: syncTables })
      : await syncPush(local, remote, { tables: syncTables });

    for (const r of results) {
      if (r.errors.length === 0) result.tables_synced++;
      result.total_rows_synced += r.rowsWritten;
      result.errors.push(...r.errors);
    }

    result.success = result.errors.length === 0;
  } catch (err: any) {
    result.errors.push(err?.message ?? String(err));
  } finally {
    if (remote) {
      try { await remote.close(); } catch {}
    }
  }

  return result;
}

// ---------------------------------------------------------------------------
// Signal handling for auto-sync on stop
// ---------------------------------------------------------------------------

type CleanupFn = () => Promise<void> | void;
const cleanupHandlers: CleanupFn[] = [];
let signalHandlersInstalled = false;

function installSignalHandlers(): void {
  if (signalHandlersInstalled) return;
  signalHandlersInstalled = true;

  const handleExit = async () => {
    for (const fn of cleanupHandlers) {
      try { await fn(); } catch {}
    }
  };

  process.on("SIGTERM", async () => {
    await handleExit();
    process.exit(0);
  });

  process.on("SIGINT", async () => {
    await handleExit();
    process.exit(0);
  });

  process.on("beforeExit", async () => {
    await handleExit();
  });
}

// ---------------------------------------------------------------------------
// setupAutoSync — hooks into MCP server lifecycle
// ---------------------------------------------------------------------------

export function setupAutoSync(
  serviceName: string,
  server: any,
  local: DbAdapter,
  remote: DbAdapter | PgAdapterAsync,
  tables: string[]
): {
  syncOnStart: () => Promise<AutoSyncResult | null>;
  syncOnStop: () => Promise<AutoSyncResult | null>;
  config: AutoSyncConfig;
} {
  const config = getAutoSyncConfig();
  const cloudConfig = getCloudConfig();
  const isSyncEnabled =
    cloudConfig.mode === "hybrid" || cloudConfig.mode === "cloud";

  const syncOnStart = async (): Promise<AutoSyncResult | null> => {
    if (!config.auto_sync_on_start || !isSyncEnabled) return null;
    return executeAutoSync("start", serviceName, local, tables);
  };

  const syncOnStop = async (): Promise<AutoSyncResult | null> => {
    if (!config.auto_sync_on_stop || !isSyncEnabled) return null;
    return executeAutoSync("stop", serviceName, local, tables);
  };

  // Hook into MCP server events
  if (server && typeof server.onconnect === "function") {
    const origOnConnect = server.onconnect;
    server.onconnect = async (...args: any[]) => {
      await syncOnStart();
      return origOnConnect.apply(server, args);
    };
  } else if (server && typeof server.on === "function") {
    server.on("connect", () => syncOnStart());
  }

  if (server && typeof server.ondisconnect === "function") {
    const origOnDisconnect = server.ondisconnect;
    server.ondisconnect = async (...args: any[]) => {
      await syncOnStop();
      return origOnDisconnect.apply(server, args);
    };
  } else if (server && typeof server.on === "function") {
    server.on("disconnect", () => syncOnStop());
  }

  // Signal handlers for graceful shutdown
  installSignalHandlers();
  cleanupHandlers.push(async () => { await syncOnStop(); });

  return { syncOnStart, syncOnStop, config };
}

// ---------------------------------------------------------------------------
// enableAutoSync — simplified helper
// ---------------------------------------------------------------------------

export function enableAutoSync(
  serviceName: string,
  mcpServer: any,
  local: DbAdapter,
  remote: DbAdapter | PgAdapterAsync,
  tables: string[]
): void {
  setupAutoSync(serviceName, mcpServer, local, remote, tables);
}
