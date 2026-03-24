import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { SqliteAdapter } from "../src/adapter";
import {
  incrementalSyncPush,
  incrementalSyncPull,
  ensureSyncMetaTable,
  getSyncMetaAll,
  getSyncMetaForTable,
  resetSyncMeta,
  resetAllSyncMeta,
} from "../src/sync-incremental";
import { unlinkSync, existsSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";

const DB_LOCAL = join(tmpdir(), `cloud-test-incr-local-${Date.now()}.db`);
const DB_REMOTE = join(tmpdir(), `cloud-test-incr-remote-${Date.now()}.db`);

function cleanDb(path: string) {
  for (const suffix of ["", "-wal", "-shm"]) {
    try {
      if (existsSync(path + suffix)) unlinkSync(path + suffix);
    } catch {}
  }
}

describe("sync-incremental", () => {
  let local: SqliteAdapter;
  let remote: SqliteAdapter;

  beforeEach(() => {
    local = new SqliteAdapter(DB_LOCAL);
    remote = new SqliteAdapter(DB_REMOTE);

    const schema = `
      CREATE TABLE IF NOT EXISTS items (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        value INTEGER DEFAULT 0,
        updated_at TEXT DEFAULT (datetime('now'))
      )
    `;
    local.exec(schema);
    remote.exec(schema);
  });

  afterEach(() => {
    local.close();
    remote.close();
    cleanDb(DB_LOCAL);
    cleanDb(DB_REMOTE);
  });

  // -----------------------------------------------------------------------
  // ensureSyncMetaTable
  // -----------------------------------------------------------------------

  test("ensureSyncMetaTable creates _sync_meta table", () => {
    ensureSyncMetaTable(local);
    const tables = local.all(
      "SELECT name FROM sqlite_master WHERE type='table' AND name='_sync_meta'"
    );
    expect(tables).toHaveLength(1);
  });

  // -----------------------------------------------------------------------
  // incrementalSyncPush — first sync (full)
  // -----------------------------------------------------------------------

  test("incrementalSyncPush does a full push on first sync", () => {
    local.run(
      "INSERT INTO items (id, name, value, updated_at) VALUES (?, ?, ?, ?)",
      "i1", "Alpha", 10, "2025-01-01T00:00:00Z"
    );
    local.run(
      "INSERT INTO items (id, name, value, updated_at) VALUES (?, ?, ?, ?)",
      "i2", "Beta", 20, "2025-01-02T00:00:00Z"
    );

    const results = incrementalSyncPush(local, remote, ["items"]);

    expect(results).toHaveLength(1);
    expect(results[0].table).toBe("items");
    expect(results[0].first_sync).toBe(true);
    expect(results[0].synced_rows).toBe(2);
    expect(results[0].total_rows).toBe(2);
    expect(results[0].errors).toHaveLength(0);

    // Verify rows exist in remote
    const rows = remote.all("SELECT * FROM items ORDER BY id");
    expect(rows).toHaveLength(2);
    expect(rows[0].name).toBe("Alpha");
    expect(rows[1].name).toBe("Beta");
  });

  // -----------------------------------------------------------------------
  // incrementalSyncPush — incremental (only changed rows)
  // -----------------------------------------------------------------------

  test("incrementalSyncPush only syncs changed rows on subsequent syncs", () => {
    // First sync
    local.run(
      "INSERT INTO items (id, name, value, updated_at) VALUES (?, ?, ?, ?)",
      "i1", "Alpha", 10, "2025-01-01T00:00:00Z"
    );
    local.run(
      "INSERT INTO items (id, name, value, updated_at) VALUES (?, ?, ?, ?)",
      "i2", "Beta", 20, "2025-01-02T00:00:00Z"
    );

    incrementalSyncPush(local, remote, ["items"]);

    // Now add a new row and update an existing one with a future timestamp
    local.run(
      "INSERT INTO items (id, name, value, updated_at) VALUES (?, ?, ?, ?)",
      "i3", "Gamma", 30, "2099-01-01T00:00:00Z"
    );
    local.run(
      "UPDATE items SET name = ?, value = ?, updated_at = ? WHERE id = ?",
      "AlphaUpdated", 99, "2099-01-01T00:00:00Z", "i1"
    );

    // Second (incremental) sync
    const results = incrementalSyncPush(local, remote, ["items"]);

    expect(results[0].first_sync).toBe(false);
    expect(results[0].synced_rows).toBe(2); // i1 updated + i3 new
    expect(results[0].errors).toHaveLength(0);

    // Verify remote has the updated data
    const i1 = remote.get("SELECT * FROM items WHERE id = ?", "i1");
    expect(i1.name).toBe("AlphaUpdated");
    expect(i1.value).toBe(99);

    const i3 = remote.get("SELECT * FROM items WHERE id = ?", "i3");
    expect(i3.name).toBe("Gamma");
  });

  // -----------------------------------------------------------------------
  // incrementalSyncPush — no changes since last sync
  // -----------------------------------------------------------------------

  test("incrementalSyncPush reports 0 synced rows when nothing changed", () => {
    local.run(
      "INSERT INTO items (id, name, value, updated_at) VALUES (?, ?, ?, ?)",
      "i1", "Alpha", 10, "2025-01-01T00:00:00Z"
    );

    // First sync
    incrementalSyncPush(local, remote, ["items"]);

    // Second sync — nothing changed (all rows have old timestamps)
    const results = incrementalSyncPush(local, remote, ["items"]);

    expect(results[0].synced_rows).toBe(0);
    expect(results[0].first_sync).toBe(false);
  });

  // -----------------------------------------------------------------------
  // incrementalSyncPull — first sync (full)
  // -----------------------------------------------------------------------

  test("incrementalSyncPull does a full pull on first sync", () => {
    remote.run(
      "INSERT INTO items (id, name, value, updated_at) VALUES (?, ?, ?, ?)",
      "i1", "CloudAlpha", 100, "2025-06-01T00:00:00Z"
    );

    const results = incrementalSyncPull(remote, local, ["items"]);

    expect(results).toHaveLength(1);
    expect(results[0].first_sync).toBe(true);
    expect(results[0].synced_rows).toBe(1);

    const row = local.get("SELECT * FROM items WHERE id = ?", "i1");
    expect(row.name).toBe("CloudAlpha");
    expect(row.value).toBe(100);
  });

  // -----------------------------------------------------------------------
  // incrementalSyncPull — incremental
  // -----------------------------------------------------------------------

  test("incrementalSyncPull only pulls changed rows on subsequent syncs", () => {
    remote.run(
      "INSERT INTO items (id, name, value, updated_at) VALUES (?, ?, ?, ?)",
      "i1", "CloudA", 10, "2025-01-01T00:00:00Z"
    );

    // First pull
    incrementalSyncPull(remote, local, ["items"]);

    // Add new data to remote with future timestamp
    remote.run(
      "INSERT INTO items (id, name, value, updated_at) VALUES (?, ?, ?, ?)",
      "i2", "CloudB", 20, "2099-06-01T00:00:00Z"
    );

    // Second (incremental) pull
    const results = incrementalSyncPull(remote, local, ["items"]);

    expect(results[0].first_sync).toBe(false);
    expect(results[0].synced_rows).toBe(1); // only i2
    expect(results[0].errors).toHaveLength(0);

    const row = local.get("SELECT * FROM items WHERE id = ?", "i2");
    expect(row.name).toBe("CloudB");
  });

  // -----------------------------------------------------------------------
  // Sync meta helpers
  // -----------------------------------------------------------------------

  test("getSyncMetaAll returns all tracked tables", () => {
    local.run(
      "INSERT INTO items (id, name, value) VALUES (?, ?, ?)",
      "i1", "Alpha", 10
    );

    // Create another table
    local.exec(
      "CREATE TABLE IF NOT EXISTS notes (id TEXT PRIMARY KEY, content TEXT, updated_at TEXT DEFAULT (datetime('now')))"
    );
    local.run(
      "INSERT INTO notes (id, content) VALUES (?, ?)",
      "n1", "Hello"
    );
    remote.exec(
      "CREATE TABLE IF NOT EXISTS notes (id TEXT PRIMARY KEY, content TEXT, updated_at TEXT DEFAULT (datetime('now')))"
    );

    incrementalSyncPush(local, remote, ["items", "notes"]);

    const metas = getSyncMetaAll(local);
    expect(metas).toHaveLength(2);
    expect(metas.map((m) => m.table_name).sort()).toEqual(["items", "notes"]);
    expect(metas[0].direction).toBe("push");
  });

  test("getSyncMetaForTable returns meta for specific table", () => {
    local.run(
      "INSERT INTO items (id, name, value) VALUES (?, ?, ?)",
      "i1", "Alpha", 10
    );

    incrementalSyncPush(local, remote, ["items"]);

    const meta = getSyncMetaForTable(local, "items");
    expect(meta).not.toBeNull();
    expect(meta!.table_name).toBe("items");
    expect(meta!.direction).toBe("push");
    expect(meta!.last_synced_at).toBeTruthy();
  });

  test("getSyncMetaForTable returns null for untracked table", () => {
    ensureSyncMetaTable(local);
    const meta = getSyncMetaForTable(local, "nonexistent");
    expect(meta).toBeNull();
  });

  // -----------------------------------------------------------------------
  // Reset sync meta
  // -----------------------------------------------------------------------

  test("resetSyncMeta removes meta for a specific table", () => {
    local.run(
      "INSERT INTO items (id, name, value) VALUES (?, ?, ?)",
      "i1", "Alpha", 10
    );
    incrementalSyncPush(local, remote, ["items"]);

    expect(getSyncMetaForTable(local, "items")).not.toBeNull();

    resetSyncMeta(local, "items");

    expect(getSyncMetaForTable(local, "items")).toBeNull();
  });

  test("resetAllSyncMeta clears all sync tracking", () => {
    local.run(
      "INSERT INTO items (id, name, value) VALUES (?, ?, ?)",
      "i1", "Alpha", 10
    );
    incrementalSyncPush(local, remote, ["items"]);

    expect(getSyncMetaAll(local).length).toBeGreaterThan(0);

    resetAllSyncMeta(local);

    expect(getSyncMetaAll(local)).toHaveLength(0);
  });

  test("after resetSyncMeta, next sync is a full sync", () => {
    local.run(
      "INSERT INTO items (id, name, value, updated_at) VALUES (?, ?, ?, ?)",
      "i1", "Alpha", 10, "2025-01-01T00:00:00Z"
    );

    // First sync
    incrementalSyncPush(local, remote, ["items"]);

    // Reset
    resetSyncMeta(local, "items");

    // Next sync should be full
    const results = incrementalSyncPush(local, remote, ["items"]);
    expect(results[0].first_sync).toBe(true);
  });

  // -----------------------------------------------------------------------
  // Error handling
  // -----------------------------------------------------------------------

  test("incrementalSyncPush handles table without primary key", () => {
    local.exec("CREATE TABLE IF NOT EXISTS nopk (data TEXT)");
    local.run("INSERT INTO nopk (data) VALUES (?)", "hello");
    remote.exec("CREATE TABLE IF NOT EXISTS nopk (data TEXT)");

    const results = incrementalSyncPush(local, remote, ["nopk"]);
    expect(results[0].errors.length).toBeGreaterThan(0);
    expect(results[0].errors[0]).toContain('no "id" column');
  });

  test("incrementalSyncPush handles empty table", () => {
    const results = incrementalSyncPush(local, remote, ["items"]);
    expect(results[0].total_rows).toBe(0);
    expect(results[0].synced_rows).toBe(0);
    expect(results[0].first_sync).toBe(true);
  });

  // -----------------------------------------------------------------------
  // Conflict resolution (newer wins)
  // -----------------------------------------------------------------------

  test("incrementalSyncPush skips rows when remote is newer", () => {
    // Put newer data in remote
    remote.run(
      "INSERT INTO items (id, name, value, updated_at) VALUES (?, ?, ?, ?)",
      "i1", "RemoteNewer", 999, "2099-12-31T23:59:59Z"
    );

    // Put older data in local
    local.run(
      "INSERT INTO items (id, name, value, updated_at) VALUES (?, ?, ?, ?)",
      "i1", "LocalOlder", 1, "2020-01-01T00:00:00Z"
    );

    const results = incrementalSyncPush(local, remote, ["items"]);
    expect(results[0].skipped_rows).toBe(1);
    expect(results[0].synced_rows).toBe(0);

    // Remote should still have its value
    const row = remote.get("SELECT * FROM items WHERE id = ?", "i1");
    expect(row.name).toBe("RemoteNewer");
  });
});
