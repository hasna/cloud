import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { SqliteAdapter } from "../src/adapter";
import {
  detectConflicts,
  resolveConflicts,
  getWinningData,
  ensureConflictsTable,
  storeConflicts,
  listConflicts,
  resolveConflict,
  getConflict,
  purgeResolvedConflicts,
  type SyncConflict,
} from "../src/sync-conflicts";
import { unlinkSync, existsSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";

const DB_PATH = join(tmpdir(), `cloud-test-sync-conflicts-${Date.now()}.db`);

describe("detectConflicts", () => {
  test("detects rows with different updated_at in both datasets", () => {
    const local = [
      { id: "r1", name: "Alice", updated_at: "2025-01-01T00:00:00Z" },
      { id: "r2", name: "Bob", updated_at: "2025-02-01T00:00:00Z" },
    ];
    const remote = [
      { id: "r1", name: "Alice Updated", updated_at: "2025-06-01T00:00:00Z" },
      { id: "r2", name: "Bob", updated_at: "2025-02-01T00:00:00Z" },
    ];

    const conflicts = detectConflicts(local, remote, "users");
    expect(conflicts).toHaveLength(1);
    expect(conflicts[0].row_id).toBe("r1");
    expect(conflicts[0].local_updated_at).toBe("2025-01-01T00:00:00Z");
    expect(conflicts[0].remote_updated_at).toBe("2025-06-01T00:00:00Z");
    expect(conflicts[0].resolved).toBe(false);
  });

  test("returns empty array when no conflicts exist", () => {
    const local = [
      { id: "r1", name: "Alice", updated_at: "2025-01-01T00:00:00Z" },
    ];
    const remote = [
      { id: "r1", name: "Alice", updated_at: "2025-01-01T00:00:00Z" },
    ];

    const conflicts = detectConflicts(local, remote, "users");
    expect(conflicts).toHaveLength(0);
  });

  test("ignores rows that exist only locally", () => {
    const local = [
      { id: "r1", name: "Local Only", updated_at: "2025-01-01T00:00:00Z" },
    ];
    const remote: Record<string, any>[] = [];

    const conflicts = detectConflicts(local, remote, "users");
    expect(conflicts).toHaveLength(0);
  });

  test("ignores rows that exist only remotely", () => {
    const local: Record<string, any>[] = [];
    const remote = [
      { id: "r1", name: "Remote Only", updated_at: "2025-01-01T00:00:00Z" },
    ];

    const conflicts = detectConflicts(local, remote, "users");
    expect(conflicts).toHaveLength(0);
  });

  test("uses custom primary key and conflict column", () => {
    const local = [
      { uid: "a", value: 10, modified: "2025-01-01" },
    ];
    const remote = [
      { uid: "a", value: 20, modified: "2025-06-01" },
    ];

    const conflicts = detectConflicts(local, remote, "data", "uid", "modified");
    expect(conflicts).toHaveLength(1);
    expect(conflicts[0].row_id).toBe("a");
  });

  test("detects multiple conflicts across many rows", () => {
    const local = [
      { id: "1", updated_at: "2025-01-01" },
      { id: "2", updated_at: "2025-01-01" },
      { id: "3", updated_at: "2025-01-01" },
    ];
    const remote = [
      { id: "1", updated_at: "2025-06-01" },
      { id: "2", updated_at: "2025-06-01" },
      { id: "3", updated_at: "2025-01-01" }, // same — no conflict
    ];

    const conflicts = detectConflicts(local, remote, "items");
    expect(conflicts).toHaveLength(2);
  });

  test("preserves full row data in conflict objects", () => {
    const local = [
      { id: "r1", name: "Alice", score: 100, updated_at: "2025-01-01" },
    ];
    const remote = [
      { id: "r1", name: "Alice V2", score: 200, updated_at: "2025-06-01" },
    ];

    const conflicts = detectConflicts(local, remote, "scores");
    expect(conflicts[0].local_data.score).toBe(100);
    expect(conflicts[0].remote_data.score).toBe(200);
    expect(conflicts[0].table).toBe("scores");
  });
});

describe("resolveConflicts", () => {
  const makeConflict = (
    localTs: string,
    remoteTs: string
  ): SyncConflict => ({
    table: "items",
    row_id: "r1",
    local_updated_at: localTs,
    remote_updated_at: remoteTs,
    local_data: { id: "r1", value: "local" },
    remote_data: { id: "r1", value: "remote" },
    resolved: false,
  });

  test("newest-wins resolves to the newer timestamp", () => {
    const conflicts = [makeConflict("2025-01-01", "2025-06-01")];
    const resolved = resolveConflicts(conflicts, "newest-wins");

    expect(resolved).toHaveLength(1);
    expect(resolved[0].resolved).toBe(true);
    expect(resolved[0].resolution).toBe("newest-wins");
  });

  test("local-wins always picks local", () => {
    const conflicts = [makeConflict("2025-01-01", "2025-06-01")];
    const resolved = resolveConflicts(conflicts, "local-wins");

    expect(resolved[0].resolved).toBe(true);
    expect(resolved[0].resolution).toBe("local-wins");
  });

  test("remote-wins always picks remote", () => {
    const conflicts = [makeConflict("2025-06-01", "2025-01-01")];
    const resolved = resolveConflicts(conflicts, "remote-wins");

    expect(resolved[0].resolved).toBe(true);
    expect(resolved[0].resolution).toBe("remote-wins");
  });

  test("defaults to newest-wins strategy", () => {
    const conflicts = [makeConflict("2025-01-01", "2025-06-01")];
    const resolved = resolveConflicts(conflicts);

    expect(resolved[0].resolution).toBe("newest-wins");
  });

  test("resolves multiple conflicts at once", () => {
    const conflicts = [
      makeConflict("2025-01-01", "2025-06-01"),
      makeConflict("2025-03-01", "2025-02-01"),
    ];
    const resolved = resolveConflicts(conflicts, "newest-wins");

    expect(resolved).toHaveLength(2);
    expect(resolved.every((c) => c.resolved)).toBe(true);
  });
});

describe("getWinningData", () => {
  test("returns remote data for newest-wins when remote is newer", () => {
    const conflict: SyncConflict = {
      table: "items",
      row_id: "r1",
      local_updated_at: "2025-01-01",
      remote_updated_at: "2025-06-01",
      local_data: { id: "r1", value: "local" },
      remote_data: { id: "r1", value: "remote" },
      resolved: true,
      resolution: "newest-wins",
    };

    const winner = getWinningData(conflict);
    expect(winner.value).toBe("remote");
  });

  test("returns local data for newest-wins when local is newer", () => {
    const conflict: SyncConflict = {
      table: "items",
      row_id: "r1",
      local_updated_at: "2025-06-01",
      remote_updated_at: "2025-01-01",
      local_data: { id: "r1", value: "local" },
      remote_data: { id: "r1", value: "remote" },
      resolved: true,
      resolution: "newest-wins",
    };

    const winner = getWinningData(conflict);
    expect(winner.value).toBe("local");
  });

  test("returns local data for local-wins", () => {
    const conflict: SyncConflict = {
      table: "items",
      row_id: "r1",
      local_updated_at: "2025-01-01",
      remote_updated_at: "2025-06-01",
      local_data: { id: "r1", value: "local" },
      remote_data: { id: "r1", value: "remote" },
      resolved: true,
      resolution: "local-wins",
    };

    const winner = getWinningData(conflict);
    expect(winner.value).toBe("local");
  });

  test("returns remote data for remote-wins", () => {
    const conflict: SyncConflict = {
      table: "items",
      row_id: "r1",
      local_updated_at: "2025-06-01",
      remote_updated_at: "2025-01-01",
      local_data: { id: "r1", value: "local" },
      remote_data: { id: "r1", value: "remote" },
      resolved: true,
      resolution: "remote-wins",
    };

    const winner = getWinningData(conflict);
    expect(winner.value).toBe("remote");
  });

  test("throws for unresolved conflict", () => {
    const conflict: SyncConflict = {
      table: "items",
      row_id: "r1",
      local_updated_at: "2025-01-01",
      remote_updated_at: "2025-06-01",
      local_data: { id: "r1", value: "local" },
      remote_data: { id: "r1", value: "remote" },
      resolved: false,
    };

    expect(() => getWinningData(conflict)).toThrow("not resolved");
  });
});

describe("conflict storage", () => {
  let db: SqliteAdapter;

  beforeEach(() => {
    db = new SqliteAdapter(DB_PATH);
  });

  afterEach(() => {
    db.close();
    for (const suffix of ["", "-wal", "-shm"]) {
      try {
        if (existsSync(DB_PATH + suffix)) unlinkSync(DB_PATH + suffix);
      } catch {}
    }
  });

  test("ensureConflictsTable creates the table", () => {
    ensureConflictsTable(db);

    const tables = db.all(
      `SELECT name FROM sqlite_master WHERE type='table' AND name='_sync_conflicts'`
    );
    expect(tables).toHaveLength(1);
  });

  test("storeConflicts stores unresolved conflicts", () => {
    const conflicts: SyncConflict[] = [
      {
        table: "items",
        row_id: "r1",
        local_updated_at: "2025-01-01",
        remote_updated_at: "2025-06-01",
        local_data: { id: "r1", value: "local" },
        remote_data: { id: "r1", value: "remote" },
        resolved: false,
      },
    ];

    storeConflicts(db, conflicts);

    const stored = listConflicts(db);
    expect(stored).toHaveLength(1);
    expect(stored[0].table_name).toBe("items");
    expect(stored[0].row_id).toBe("r1");
    expect(stored[0].resolution).toBeNull();
    expect(stored[0].resolved_at).toBeNull();

    // Verify JSON serialization
    const parsed = JSON.parse(stored[0].local_data);
    expect(parsed.value).toBe("local");
  });

  test("storeConflicts stores resolved conflicts with resolution", () => {
    const conflicts: SyncConflict[] = [
      {
        table: "items",
        row_id: "r1",
        local_updated_at: "2025-01-01",
        remote_updated_at: "2025-06-01",
        local_data: { id: "r1", value: "local" },
        remote_data: { id: "r1", value: "remote" },
        resolved: true,
        resolution: "newest-wins",
      },
    ];

    storeConflicts(db, conflicts);

    const stored = listConflicts(db);
    expect(stored).toHaveLength(1);
    expect(stored[0].resolution).toBe("newest-wins");
    expect(stored[0].resolved_at).not.toBeNull();
  });

  test("listConflicts filters by resolved status", () => {
    const unresolved: SyncConflict = {
      table: "items",
      row_id: "r1",
      local_updated_at: "2025-01-01",
      remote_updated_at: "2025-06-01",
      local_data: { id: "r1" },
      remote_data: { id: "r1" },
      resolved: false,
    };
    const resolved: SyncConflict = {
      table: "items",
      row_id: "r2",
      local_updated_at: "2025-01-01",
      remote_updated_at: "2025-06-01",
      local_data: { id: "r2" },
      remote_data: { id: "r2" },
      resolved: true,
      resolution: "local-wins",
    };

    storeConflicts(db, [unresolved, resolved]);

    const unresolvedList = listConflicts(db, { resolved: false });
    expect(unresolvedList).toHaveLength(1);
    expect(unresolvedList[0].row_id).toBe("r1");

    const resolvedList = listConflicts(db, { resolved: true });
    expect(resolvedList).toHaveLength(1);
    expect(resolvedList[0].row_id).toBe("r2");
  });

  test("listConflicts filters by table name", () => {
    storeConflicts(db, [
      {
        table: "items",
        row_id: "r1",
        local_updated_at: "2025-01-01",
        remote_updated_at: "2025-06-01",
        local_data: {},
        remote_data: {},
        resolved: false,
      },
      {
        table: "users",
        row_id: "r2",
        local_updated_at: "2025-01-01",
        remote_updated_at: "2025-06-01",
        local_data: {},
        remote_data: {},
        resolved: false,
      },
    ]);

    const itemConflicts = listConflicts(db, { table: "items" });
    expect(itemConflicts).toHaveLength(1);
    expect(itemConflicts[0].table_name).toBe("items");
  });

  test("resolveConflict updates a stored conflict", () => {
    storeConflicts(db, [
      {
        table: "items",
        row_id: "r1",
        local_updated_at: "2025-01-01",
        remote_updated_at: "2025-06-01",
        local_data: { id: "r1" },
        remote_data: { id: "r1" },
        resolved: false,
      },
    ]);

    const conflicts = listConflicts(db);
    const id = conflicts[0].id;

    const updated = resolveConflict(db, id, "remote-wins");
    expect(updated).not.toBeNull();
    expect(updated!.resolution).toBe("remote-wins");
    expect(updated!.resolved_at).not.toBeNull();
  });

  test("resolveConflict returns null for unknown ID", () => {
    ensureConflictsTable(db);
    const result = resolveConflict(db, "nonexistent", "local-wins");
    expect(result).toBeNull();
  });

  test("getConflict retrieves a single conflict by ID", () => {
    storeConflicts(db, [
      {
        table: "items",
        row_id: "r1",
        local_updated_at: "2025-01-01",
        remote_updated_at: "2025-06-01",
        local_data: { id: "r1", name: "test" },
        remote_data: { id: "r1", name: "test2" },
        resolved: false,
      },
    ]);

    const conflicts = listConflicts(db);
    const id = conflicts[0].id;

    const found = getConflict(db, id);
    expect(found).not.toBeNull();
    expect(found!.row_id).toBe("r1");
  });

  test("getConflict returns null for unknown ID", () => {
    ensureConflictsTable(db);
    const found = getConflict(db, "nonexistent");
    expect(found).toBeNull();
  });

  test("purgeResolvedConflicts removes only resolved entries", () => {
    storeConflicts(db, [
      {
        table: "items",
        row_id: "r1",
        local_updated_at: "2025-01-01",
        remote_updated_at: "2025-06-01",
        local_data: {},
        remote_data: {},
        resolved: false,
      },
      {
        table: "items",
        row_id: "r2",
        local_updated_at: "2025-01-01",
        remote_updated_at: "2025-06-01",
        local_data: {},
        remote_data: {},
        resolved: true,
        resolution: "local-wins",
      },
    ]);

    const purged = purgeResolvedConflicts(db);
    expect(purged).toBe(1);

    const remaining = listConflicts(db);
    expect(remaining).toHaveLength(1);
    expect(remaining[0].row_id).toBe("r1");
  });
});
