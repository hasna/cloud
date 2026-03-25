import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { SqliteAdapter } from "../src/adapter";
import {
  discoverSyncableServices,
  runScheduledSync,
} from "../src/scheduled-sync";
import { existsSync, unlinkSync, mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { tmpdir, homedir } from "os";

describe("scheduled-sync", () => {
  // -----------------------------------------------------------------------
  // discoverSyncableServices
  // -----------------------------------------------------------------------

  test("discoverSyncableServices returns an array", () => {
    const services = discoverSyncableServices();
    expect(Array.isArray(services)).toBe(true);
  });

  test("discoverSyncableServices finds services with .db files", () => {
    const services = discoverSyncableServices();
    // If ~/.hasna/ exists and has service dirs with .db files, should find them
    // At minimum this should not throw
    for (const s of services) {
      expect(typeof s).toBe("string");
      expect(s.length).toBeGreaterThan(0);
    }
  });

  // -----------------------------------------------------------------------
  // runScheduledSync
  // -----------------------------------------------------------------------

  test("runScheduledSync returns empty array in local mode", async () => {
    // Default mode is "local", so sync should be a no-op
    const results = await runScheduledSync();
    expect(Array.isArray(results)).toBe(true);
    // In local mode, it returns empty
    expect(results.length).toBe(0);
  });

  // -----------------------------------------------------------------------
  // Scheduled sync worker default export
  // -----------------------------------------------------------------------

  test("default export has a scheduled() method", async () => {
    const mod = await import("../src/scheduled-sync");
    expect(typeof mod.default).toBe("object");
    expect(typeof mod.default.scheduled).toBe("function");
  });
});
