import { describe, test, expect } from "bun:test";
import { discoverServices, isSyncExcludedTable, getServiceDbPath, KNOWN_PG_SERVICES } from "../src/discover";

describe("discoverServices", () => {
  test("returns array of service names", () => {
    const services = discoverServices();
    expect(Array.isArray(services)).toBe(true);
    // Should find at least some services on a dev machine
    if (services.length > 0) {
      expect(typeof services[0]).toBe("string");
      // Should not include 'cloud' (config dir, not a service)
      expect(services).not.toContain("cloud");
    }
  });

  test("returns sorted results", () => {
    const services = discoverServices();
    const sorted = [...services].sort();
    expect(services).toEqual(sorted);
  });
});

describe("isSyncExcludedTable", () => {
  test("excludes sqlite internal tables", () => {
    expect(isSyncExcludedTable("sqlite_sequence")).toBe(true);
    expect(isSyncExcludedTable("sqlite_stat1")).toBe(true);
  });

  test("excludes FTS tables", () => {
    expect(isSyncExcludedTable("tasks_fts")).toBe(true);
    expect(isSyncExcludedTable("tasks_fts_content")).toBe(true);
    expect(isSyncExcludedTable("tasks_fts_data")).toBe(true);
    expect(isSyncExcludedTable("tasks_fts_docsize")).toBe(true);
    expect(isSyncExcludedTable("tasks_fts_idx")).toBe(true);
    expect(isSyncExcludedTable("tasks_fts_config")).toBe(true);
    expect(isSyncExcludedTable("messages_fts")).toBe(true);
  });

  test("excludes sync metadata tables", () => {
    expect(isSyncExcludedTable("_sync_meta")).toBe(true);
    expect(isSyncExcludedTable("_sync_resume")).toBe(true);
    expect(isSyncExcludedTable("_pg_migrations")).toBe(true);
  });

  test("allows regular tables", () => {
    expect(isSyncExcludedTable("tasks")).toBe(false);
    expect(isSyncExcludedTable("projects")).toBe(false);
    expect(isSyncExcludedTable("agents")).toBe(false);
    expect(isSyncExcludedTable("task_tags")).toBe(false);
    expect(isSyncExcludedTable("_migrations")).toBe(false);
  });
});

describe("getServiceDbPath", () => {
  test("returns null for nonexistent service", () => {
    expect(getServiceDbPath("nonexistent-service-xyz")).toBeNull();
  });

  test("finds todos db if it exists", () => {
    const path = getServiceDbPath("todos");
    if (path) {
      expect(path).toContain("todos");
      expect(path).toEndWith(".db");
    }
  });
});

describe("KNOWN_PG_SERVICES", () => {
  test("contains expected services", () => {
    expect(KNOWN_PG_SERVICES).toContain("todos");
    expect(KNOWN_PG_SERVICES).toContain("conversations");
    expect(KNOWN_PG_SERVICES).toContain("mementos");
  });

  test("is sorted", () => {
    const sorted = [...KNOWN_PG_SERVICES].sort();
    expect(KNOWN_PG_SERVICES).toEqual(sorted);
  });
});
