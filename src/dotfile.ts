import {
  existsSync,
  mkdirSync,
  readdirSync,
  copyFileSync,
  statSync,
} from "fs";
import { homedir } from "os";
import { join, relative } from "path";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const HASNA_DIR = join(homedir(), ".hasna");

// ---------------------------------------------------------------------------
// Data Directory
// ---------------------------------------------------------------------------

/**
 * Returns `~/.hasna/<serviceName>/`, creating it if needed.
 */
export function getDataDir(serviceName: string): string {
  const dir = join(HASNA_DIR, serviceName);
  mkdirSync(dir, { recursive: true });
  return dir;
}

/**
 * Returns the path for the service's SQLite database:
 * `~/.hasna/<serviceName>/<serviceName>.db`
 */
export function getDbPath(serviceName: string): string {
  const dir = getDataDir(serviceName);
  return join(dir, `${serviceName}.db`);
}

// ---------------------------------------------------------------------------
// Dotfile Migration
// ---------------------------------------------------------------------------

/**
 * Migrate from legacy `~/.<serviceName>/` to `~/.hasna/<serviceName>/`.
 *
 * - If `~/.<serviceName>/` exists and `~/.hasna/<serviceName>/` does NOT,
 *   copies all contents over.
 * - Returns a list of migrated file paths (relative to the source dir),
 *   or an empty array if no migration was needed.
 */
export function migrateDotfile(serviceName: string): string[] {
  const legacyDir = join(homedir(), `.${serviceName}`);
  const newDir = join(HASNA_DIR, serviceName);

  // Nothing to migrate
  if (!existsSync(legacyDir)) return [];

  // Already migrated
  if (existsSync(newDir)) return [];

  mkdirSync(newDir, { recursive: true });

  const migrated: string[] = [];
  copyDirRecursive(legacyDir, newDir, legacyDir, migrated);
  return migrated;
}

function copyDirRecursive(
  src: string,
  dest: string,
  root: string,
  migrated: string[]
): void {
  const entries = readdirSync(src, { withFileTypes: true });
  for (const entry of entries) {
    const srcPath = join(src, entry.name);
    const destPath = join(dest, entry.name);
    if (entry.isDirectory()) {
      mkdirSync(destPath, { recursive: true });
      copyDirRecursive(srcPath, destPath, root, migrated);
    } else {
      copyFileSync(srcPath, destPath);
      migrated.push(relative(root, srcPath));
    }
  }
}

/**
 * Check if a legacy dotfile directory exists for the given service.
 */
export function hasLegacyDotfile(serviceName: string): boolean {
  return existsSync(join(homedir(), `.${serviceName}`));
}

/**
 * Get the `.hasna` base directory.
 */
export function getHasnaDir(): string {
  mkdirSync(HASNA_DIR, { recursive: true });
  return HASNA_DIR;
}
