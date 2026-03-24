import { existsSync, readFileSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import type { DbAdapter } from "./adapter.js";
import { getCloudConfig } from "./config.js";
import { incrementalSyncPush, incrementalSyncPull } from "./sync-incremental.js";

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
  remote: DbAdapter;
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

/**
 * Read auto-sync configuration from `~/.hasna/cloud/config.json`.
 * Falls back to defaults if the file does not exist or is malformed.
 */
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
// Auto-sync execution
// ---------------------------------------------------------------------------

/**
 * Execute an auto-sync pull (for start) or push (for stop).
 */
function executeAutoSync(
  event: "start" | "stop",
  local: DbAdapter,
  remote: DbAdapter,
  tables: string[]
): AutoSyncResult {
  const direction = event === "start" ? "pull" : "push";
  const result: AutoSyncResult = {
    event,
    direction,
    success: false,
    tables_synced: 0,
    total_rows_synced: 0,
    errors: [],
  };

  try {
    const stats =
      direction === "pull"
        ? incrementalSyncPull(remote, local, tables)
        : incrementalSyncPush(local, remote, tables);

    for (const s of stats) {
      if (s.errors.length === 0) {
        result.tables_synced++;
      }
      result.total_rows_synced += s.synced_rows;
      result.errors.push(...s.errors);
    }

    result.success = result.errors.length === 0;
  } catch (err: any) {
    result.errors.push(err?.message ?? String(err));
  }

  return result;
}

// ---------------------------------------------------------------------------
// Signal handling for auto-sync on stop
// ---------------------------------------------------------------------------

type CleanupFn = () => void;
const cleanupHandlers: CleanupFn[] = [];
let signalHandlersInstalled = false;

function installSignalHandlers(): void {
  if (signalHandlersInstalled) return;
  signalHandlersInstalled = true;

  const handleExit = () => {
    for (const fn of cleanupHandlers) {
      try {
        fn();
      } catch {
        // Best-effort on shutdown
      }
    }
  };

  process.on("SIGTERM", () => {
    handleExit();
    process.exit(0);
  });

  process.on("SIGINT", () => {
    handleExit();
    process.exit(0);
  });

  process.on("beforeExit", () => {
    handleExit();
  });
}

// ---------------------------------------------------------------------------
// setupAutoSync — hooks into MCP server lifecycle
// ---------------------------------------------------------------------------

/**
 * Set up auto-sync hooks for a service's MCP server.
 *
 * - On connect: if `auto_sync_on_start` and mode is `hybrid` or `cloud`,
 *   pull from cloud to local.
 * - On disconnect/SIGTERM: if `auto_sync_on_stop` and mode is `hybrid` or `cloud`,
 *   push from local to cloud.
 *
 * @param serviceName - The service identifier.
 * @param server - The MCP server instance (any object with `onconnect`/`ondisconnect` events).
 * @param local - The local database adapter.
 * @param remote - The remote database adapter.
 * @param tables - Tables to sync.
 * @returns An object with methods to manually trigger start/stop syncs.
 */
export function setupAutoSync(
  serviceName: string,
  server: any,
  local: DbAdapter,
  remote: DbAdapter,
  tables: string[]
): {
  syncOnStart: () => AutoSyncResult | null;
  syncOnStop: () => AutoSyncResult | null;
  config: AutoSyncConfig;
} {
  const config = getAutoSyncConfig();
  const cloudConfig = getCloudConfig();
  const isSyncEnabled =
    cloudConfig.mode === "hybrid" || cloudConfig.mode === "cloud";

  const syncOnStart = (): AutoSyncResult | null => {
    if (!config.auto_sync_on_start || !isSyncEnabled) return null;
    return executeAutoSync("start", local, remote, tables);
  };

  const syncOnStop = (): AutoSyncResult | null => {
    if (!config.auto_sync_on_stop || !isSyncEnabled) return null;
    return executeAutoSync("stop", local, remote, tables);
  };

  // Hook into MCP server events if the server supports them
  if (server && typeof server.onconnect === "function") {
    const origOnConnect = server.onconnect;
    server.onconnect = (...args: any[]) => {
      syncOnStart();
      return origOnConnect.apply(server, args);
    };
  } else if (server && typeof server.on === "function") {
    // EventEmitter-style server
    server.on("connect", () => {
      syncOnStart();
    });
  }

  if (server && typeof server.ondisconnect === "function") {
    const origOnDisconnect = server.ondisconnect;
    server.ondisconnect = (...args: any[]) => {
      syncOnStop();
      return origOnDisconnect.apply(server, args);
    };
  } else if (server && typeof server.on === "function") {
    server.on("disconnect", () => {
      syncOnStop();
    });
  }

  // Also hook into process signals for graceful shutdown sync
  installSignalHandlers();
  cleanupHandlers.push(() => {
    syncOnStop();
  });

  return { syncOnStart, syncOnStop, config };
}

// ---------------------------------------------------------------------------
// enableAutoSync — simplified helper for services
// ---------------------------------------------------------------------------

/**
 * Enable auto-sync for a service. Simplified entry point that services
 * can call with minimal configuration.
 *
 * @param serviceName - The service name (used for logging context).
 * @param mcpServer - The MCP server instance.
 * @param local - The local database adapter.
 * @param remote - The remote database adapter.
 * @param tables - Tables to sync on start/stop.
 */
export function enableAutoSync(
  serviceName: string,
  mcpServer: any,
  local: DbAdapter,
  remote: DbAdapter,
  tables: string[]
): void {
  setupAutoSync(serviceName, mcpServer, local, remote, tables);
}
