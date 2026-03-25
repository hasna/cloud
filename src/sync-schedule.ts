import { join, dirname } from "path";
import { getCloudConfig, saveCloudConfig } from "./config.js";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const CRON_TITLE = "hasna-cloud-sync";

/**
 * Resolve the path to the scheduled-sync worker script.
 * In the built package this lives at `dist/scheduled-sync.js`;
 * during development it's `src/scheduled-sync.ts`.
 */
function getWorkerPath(): string {
  // import.meta.dir gives the directory of the *current* file at runtime.
  // The scheduled-sync module lives in the same directory.
  const dir = typeof import.meta.dir === "string" ? import.meta.dir : dirname(import.meta.url.replace("file://", ""));
  // Prefer .ts (dev) if it exists, otherwise .js (dist)
  const tsPath = join(dir, "scheduled-sync.ts");
  const jsPath = join(dir, "scheduled-sync.js");
  try {
    const { existsSync } = require("fs");
    if (existsSync(tsPath)) return tsPath;
  } catch {
    // noop
  }
  return jsPath;
}

// ---------------------------------------------------------------------------
// Interval parsing
// ---------------------------------------------------------------------------

/**
 * Parse a human-friendly interval string into minutes.
 *
 * Supported formats:
 * - `5m`, `10m`, `30m` - minutes
 * - `1h`, `2h` - hours (converted to minutes)
 * - `5` - plain number treated as minutes
 */
export function parseInterval(input: string): number {
  const trimmed = input.trim().toLowerCase();

  const hourMatch = trimmed.match(/^(\d+)\s*h$/);
  if (hourMatch) {
    const hours = parseInt(hourMatch[1], 10);
    if (hours <= 0) {
      throw new Error(
        `Invalid interval "${input}". Value must be greater than 0.`
      );
    }
    return hours * 60;
  }

  const minMatch = trimmed.match(/^(\d+)\s*m$/);
  if (minMatch) {
    const mins = parseInt(minMatch[1], 10);
    if (mins <= 0) {
      throw new Error(
        `Invalid interval "${input}". Value must be greater than 0.`
      );
    }
    return mins;
  }

  const plain = parseInt(trimmed, 10);
  if (!isNaN(plain) && plain > 0) {
    return plain;
  }

  throw new Error(
    `Invalid interval "${input}". Use formats like: 5m, 10m, 1h, or a plain number of minutes.`
  );
}

/**
 * Convert minutes to a cron expression.
 *
 * - For intervals that divide evenly into 60: `*\/<n> * * * *`
 * - For hourly multiples: `0 *\/<h> * * *`
 * - Otherwise: `*\/<n> * * * *` (best-effort)
 */
export function minutesToCron(minutes: number): string {
  if (minutes <= 0) {
    throw new Error("Interval must be greater than 0 minutes.");
  }

  if (minutes < 60) {
    return `*/${minutes} * * * *`;
  }

  const hours = Math.floor(minutes / 60);
  const remainderMins = minutes % 60;

  if (remainderMins === 0 && hours <= 24) {
    return `0 */${hours} * * *`;
  }

  // Fallback for odd intervals: use minute-level
  return `*/${minutes} * * * *`;
}

// ---------------------------------------------------------------------------
// Schedule management
// ---------------------------------------------------------------------------

export interface SyncScheduleStatus {
  registered: boolean;
  schedule_minutes: number;
  cron_expression: string | null;
}

/**
 * Register a Bun.cron job that runs the scheduled sync worker on a fixed
 * interval.
 *
 * - Persists `schedule_minutes` in `~/.hasna/cloud/config.json`.
 * - Calls `Bun.cron()` to register an OS-level cron job.
 */
export async function registerSyncSchedule(
  intervalMinutes: number
): Promise<void> {
  if (intervalMinutes <= 0) {
    throw new Error("Interval must be a positive number of minutes.");
  }

  const cronExpr = minutesToCron(intervalMinutes);
  const workerPath = getWorkerPath();

  // Register with Bun.cron (OS-level cron/launchd/schtasks)
  await Bun.cron(workerPath, cronExpr, CRON_TITLE);

  // Persist to config
  const config = getCloudConfig();
  config.sync.schedule_minutes = intervalMinutes;
  saveCloudConfig(config);
}

/**
 * Remove the registered sync cron job.
 *
 * - Calls `Bun.cron.remove()` to unregister the OS-level job.
 * - Sets `schedule_minutes` to 0 in config.
 */
export async function removeSyncSchedule(): Promise<void> {
  await Bun.cron.remove(CRON_TITLE);

  // Update config
  const config = getCloudConfig();
  config.sync.schedule_minutes = 0;
  saveCloudConfig(config);
}

/**
 * Get the current sync schedule status from config.
 */
export function getSyncScheduleStatus(): SyncScheduleStatus {
  const config = getCloudConfig();
  const minutes = config.sync.schedule_minutes;
  const registered = minutes > 0;

  return {
    registered,
    schedule_minutes: minutes,
    cron_expression: registered ? minutesToCron(minutes) : null,
  };
}
