import { join, dirname } from "path";
import { existsSync, writeFileSync, unlinkSync, readFileSync, mkdirSync } from "fs";
import { homedir, platform } from "os";
import { getCloudConfig, saveCloudConfig } from "./config.js";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SERVICE_NAME = "hasna-cloud-sync";
const CONFIG_DIR = join(homedir(), ".hasna", "cloud");

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

  return `*/${minutes} * * * *`;
}

// ---------------------------------------------------------------------------
// Worker path
// ---------------------------------------------------------------------------

function getWorkerPath(): string {
  const dir = typeof import.meta.dir === "string" ? import.meta.dir : dirname(import.meta.url.replace("file://", ""));
  const tsPath = join(dir, "scheduled-sync.ts");
  const jsPath = join(dir, "scheduled-sync.js");
  try {
    if (existsSync(tsPath)) return tsPath;
  } catch {}
  return jsPath;
}

function getBunPath(): string {
  // Try common bun locations
  const candidates = [
    join(homedir(), ".bun", "bin", "bun"),
    "/usr/local/bin/bun",
    "/usr/bin/bun",
  ];
  for (const p of candidates) {
    if (existsSync(p)) return p;
  }
  return "bun"; // fallback to PATH
}

// ---------------------------------------------------------------------------
// macOS: launchd plist
// ---------------------------------------------------------------------------

function getLaunchdPlistPath(): string {
  return join(homedir(), "Library", "LaunchAgents", `com.hasna.cloud-sync.plist`);
}

function createLaunchdPlist(intervalMinutes: number): string {
  const workerPath = getWorkerPath();
  const bunPath = getBunPath();
  const logPath = join(CONFIG_DIR, "sync.log");
  const errorLogPath = join(CONFIG_DIR, "sync-error.log");

  return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.hasna.cloud-sync</string>
  <key>ProgramArguments</key>
  <array>
    <string>${bunPath}</string>
    <string>run</string>
    <string>${workerPath}</string>
  </array>
  <key>StartInterval</key>
  <integer>${intervalMinutes * 60}</integer>
  <key>RunAtLoad</key>
  <true/>
  <key>StandardOutPath</key>
  <string>${logPath}</string>
  <key>StandardErrorPath</key>
  <string>${errorLogPath}</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>${process.env.PATH || "/usr/local/bin:/usr/bin:/bin"}</string>
    <key>HOME</key>
    <string>${homedir()}</string>
  </dict>
</dict>
</plist>`;
}

async function registerLaunchd(intervalMinutes: number): Promise<void> {
  const plistPath = getLaunchdPlistPath();
  const plistDir = dirname(plistPath);
  mkdirSync(plistDir, { recursive: true });

  // Unload existing if present
  try {
    await Bun.spawn(["launchctl", "unload", plistPath]).exited;
  } catch {}

  writeFileSync(plistPath, createLaunchdPlist(intervalMinutes));
  await Bun.spawn(["launchctl", "load", plistPath]).exited;
}

async function removeLaunchd(): Promise<void> {
  const plistPath = getLaunchdPlistPath();
  try {
    await Bun.spawn(["launchctl", "unload", plistPath]).exited;
  } catch {}
  try {
    unlinkSync(plistPath);
  } catch {}
}

// ---------------------------------------------------------------------------
// Linux: systemd user timer
// ---------------------------------------------------------------------------

function getSystemdDir(): string {
  return join(homedir(), ".config", "systemd", "user");
}

function createSystemdService(): string {
  const workerPath = getWorkerPath();
  const bunPath = getBunPath();

  return `[Unit]
Description=Hasna Cloud Sync
After=network.target

[Service]
Type=oneshot
ExecStart=${bunPath} run ${workerPath}
Environment=HOME=${homedir()}
Environment=PATH=${process.env.PATH || "/usr/local/bin:/usr/bin:/bin"}

[Install]
WantedBy=default.target
`;
}

function createSystemdTimer(intervalMinutes: number): string {
  return `[Unit]
Description=Hasna Cloud Sync Timer

[Timer]
OnBootSec=${intervalMinutes}min
OnUnitActiveSec=${intervalMinutes}min
Persistent=true

[Install]
WantedBy=timers.target
`;
}

async function registerSystemd(intervalMinutes: number): Promise<void> {
  const dir = getSystemdDir();
  mkdirSync(dir, { recursive: true });

  writeFileSync(join(dir, `${SERVICE_NAME}.service`), createSystemdService());
  writeFileSync(join(dir, `${SERVICE_NAME}.timer`), createSystemdTimer(intervalMinutes));

  await Bun.spawn(["systemctl", "--user", "daemon-reload"]).exited;
  await Bun.spawn(["systemctl", "--user", "enable", "--now", `${SERVICE_NAME}.timer`]).exited;
}

async function removeSystemd(): Promise<void> {
  try {
    await Bun.spawn(["systemctl", "--user", "disable", "--now", `${SERVICE_NAME}.timer`]).exited;
  } catch {}
  const dir = getSystemdDir();
  try { unlinkSync(join(dir, `${SERVICE_NAME}.service`)); } catch {}
  try { unlinkSync(join(dir, `${SERVICE_NAME}.timer`)); } catch {}
  try {
    await Bun.spawn(["systemctl", "--user", "daemon-reload"]).exited;
  } catch {}
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export interface SyncScheduleStatus {
  registered: boolean;
  schedule_minutes: number;
  cron_expression: string | null;
  mechanism: "launchd" | "systemd" | "none";
}

/**
 * Register a system-level scheduled sync.
 *
 * - macOS: creates a launchd plist in ~/Library/LaunchAgents/
 * - Linux: creates a systemd user timer in ~/.config/systemd/user/
 * - Persists interval in ~/.hasna/cloud/config.json
 */
export async function registerSyncSchedule(
  intervalMinutes: number
): Promise<void> {
  if (intervalMinutes <= 0) {
    throw new Error("Interval must be a positive number of minutes.");
  }

  mkdirSync(CONFIG_DIR, { recursive: true });

  if (platform() === "darwin") {
    await registerLaunchd(intervalMinutes);
  } else {
    await registerSystemd(intervalMinutes);
  }

  // Persist to config
  const config = getCloudConfig();
  config.sync.schedule_minutes = intervalMinutes;
  saveCloudConfig(config);
}

/**
 * Remove the registered sync schedule.
 */
export async function removeSyncSchedule(): Promise<void> {
  if (platform() === "darwin") {
    await removeLaunchd();
  } else {
    await removeSystemd();
  }

  const config = getCloudConfig();
  config.sync.schedule_minutes = 0;
  saveCloudConfig(config);
}

/**
 * Get the current sync schedule status.
 */
export function getSyncScheduleStatus(): SyncScheduleStatus {
  const config = getCloudConfig();
  const minutes = config.sync.schedule_minutes;
  const registered = minutes > 0;

  let mechanism: "launchd" | "systemd" | "none" = "none";
  if (registered) {
    if (platform() === "darwin") {
      mechanism = existsSync(getLaunchdPlistPath()) ? "launchd" : "none";
    } else {
      mechanism = existsSync(join(getSystemdDir(), `${SERVICE_NAME}.timer`)) ? "systemd" : "none";
    }
  }

  return {
    registered,
    schedule_minutes: minutes,
    cron_expression: registered ? minutesToCron(minutes) : null,
    mechanism,
  };
}
