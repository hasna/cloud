import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { getDataDir, getDbPath, migrateDotfile } from "../src/dotfile";
import {
  existsSync,
  mkdirSync,
  writeFileSync,
  rmSync,
  readFileSync,
} from "fs";
import { homedir } from "os";
import { join } from "path";

const TEST_SERVICE = `cloud-test-svc-${Date.now()}`;
const LEGACY_DIR = join(homedir(), `.${TEST_SERVICE}`);
const NEW_DIR = join(homedir(), ".hasna", TEST_SERVICE);

describe("dotfile", () => {
  afterEach(() => {
    // Cleanup
    try {
      rmSync(LEGACY_DIR, { recursive: true, force: true });
    } catch {}
    try {
      rmSync(NEW_DIR, { recursive: true, force: true });
    } catch {}
  });

  test("getDataDir creates ~/.hasna/<service>/", () => {
    const dir = getDataDir(TEST_SERVICE);
    expect(dir).toBe(NEW_DIR);
    expect(existsSync(dir)).toBe(true);
  });

  test("getDbPath returns correct path", () => {
    const dbPath = getDbPath(TEST_SERVICE);
    expect(dbPath).toBe(join(NEW_DIR, `${TEST_SERVICE}.db`));
    // Should have created the dir
    expect(existsSync(NEW_DIR)).toBe(true);
  });

  test("migrateDotfile copies legacy dir to new location", () => {
    // Setup legacy dir with files
    mkdirSync(LEGACY_DIR, { recursive: true });
    writeFileSync(join(LEGACY_DIR, "config.json"), '{"key": "value"}');
    mkdirSync(join(LEGACY_DIR, "sub"), { recursive: true });
    writeFileSync(join(LEGACY_DIR, "sub", "data.txt"), "hello");

    const migrated = migrateDotfile(TEST_SERVICE);
    expect(migrated).toHaveLength(2);
    expect(migrated).toContain("config.json");
    expect(migrated).toContain(join("sub", "data.txt"));

    // Verify files were copied
    expect(existsSync(join(NEW_DIR, "config.json"))).toBe(true);
    expect(readFileSync(join(NEW_DIR, "config.json"), "utf-8")).toBe(
      '{"key": "value"}'
    );
    expect(existsSync(join(NEW_DIR, "sub", "data.txt"))).toBe(true);
  });

  test("migrateDotfile returns empty if no legacy dir", () => {
    const migrated = migrateDotfile(TEST_SERVICE);
    expect(migrated).toHaveLength(0);
  });

  test("migrateDotfile returns empty if already migrated", () => {
    // Both dirs exist
    mkdirSync(LEGACY_DIR, { recursive: true });
    writeFileSync(join(LEGACY_DIR, "data.txt"), "hello");
    mkdirSync(NEW_DIR, { recursive: true });

    const migrated = migrateDotfile(TEST_SERVICE);
    expect(migrated).toHaveLength(0);
  });
});
