import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { SqliteAdapter } from "../src/adapter";
import { syncPush, syncPull, listSqliteTables } from "../src/sync";
import { unlinkSync, existsSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";

const DB_SOURCE = join(tmpdir(), `cloud-test-sync-src-${Date.now()}.db`);
const DB_TARGET = join(tmpdir(), `cloud-test-sync-tgt-${Date.now()}.db`);

describe("sync (SQLite-to-SQLite)", () => {
  let source: SqliteAdapter;
  let target: SqliteAdapter;

  beforeEach(() => {
    source = new SqliteAdapter(DB_SOURCE);
    target = new SqliteAdapter(DB_TARGET);

    // Create same schema in both
    const schema = `
      CREATE TABLE IF NOT EXISTS items (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        value INTEGER DEFAULT 0,
        updated_at TEXT DEFAULT (datetime('now'))
      )
    `;
    source.exec(schema);
    target.exec(schema);
  });

  afterEach(() => {
    source.close();
    target.close();
    for (const f of [DB_SOURCE, DB_TARGET]) {
      try {
        if (existsSync(f)) unlinkSync(f);
        if (existsSync(f + "-wal")) unlinkSync(f + "-wal");
        if (existsSync(f + "-shm")) unlinkSync(f + "-shm");
      } catch {}
    }
  });

  test("syncPush copies rows from source to target", () => {
    source.run(
      "INSERT INTO items (id, name, value) VALUES (?, ?, ?)",
      "i1",
      "Alpha",
      10
    );
    source.run(
      "INSERT INTO items (id, name, value) VALUES (?, ?, ?)",
      "i2",
      "Beta",
      20
    );

    const results = syncPush(source, target, { tables: ["items"] });
    expect(results).toHaveLength(1);
    expect(results[0].rowsWritten).toBe(2);
    expect(results[0].errors).toHaveLength(0);

    const rows = target.all("SELECT * FROM items ORDER BY id");
    expect(rows).toHaveLength(2);
    expect(rows[0].name).toBe("Alpha");
    expect(rows[1].name).toBe("Beta");
  });

  test("syncPush updates existing rows (newer wins)", () => {
    // Insert old data in target
    target.run(
      "INSERT INTO items (id, name, value, updated_at) VALUES (?, ?, ?, ?)",
      "i1",
      "Old",
      1,
      "2024-01-01T00:00:00Z"
    );

    // Insert newer data in source
    source.run(
      "INSERT INTO items (id, name, value, updated_at) VALUES (?, ?, ?, ?)",
      "i1",
      "New",
      99,
      "2025-06-01T00:00:00Z"
    );

    const results = syncPush(source, target, { tables: ["items"] });
    expect(results[0].rowsWritten).toBe(1);

    const row = target.get("SELECT * FROM items WHERE id = ?", "i1");
    expect(row.name).toBe("New");
    expect(row.value).toBe(99);
  });

  test("syncPush skips rows when target is newer", () => {
    // Insert newer data in target
    target.run(
      "INSERT INTO items (id, name, value, updated_at) VALUES (?, ?, ?, ?)",
      "i1",
      "TargetNewer",
      50,
      "2025-12-01T00:00:00Z"
    );

    // Insert older data in source
    source.run(
      "INSERT INTO items (id, name, value, updated_at) VALUES (?, ?, ?, ?)",
      "i1",
      "SourceOlder",
      10,
      "2024-01-01T00:00:00Z"
    );

    const results = syncPush(source, target, { tables: ["items"] });
    expect(results[0].rowsSkipped).toBe(1);
    expect(results[0].rowsWritten).toBe(0);

    const row = target.get("SELECT * FROM items WHERE id = ?", "i1");
    expect(row.name).toBe("TargetNewer");
  });

  test("syncPull copies rows from target to source (reverse)", () => {
    target.run(
      "INSERT INTO items (id, name, value) VALUES (?, ?, ?)",
      "i1",
      "CloudItem",
      42
    );

    const results = syncPull(source, target, { tables: ["items"] });
    expect(results[0].rowsWritten).toBe(1);

    const row = source.get("SELECT * FROM items WHERE id = ?", "i1");
    expect(row.name).toBe("CloudItem");
    expect(row.value).toBe(42);
  });

  test("syncPush reports error for table without primary key column", () => {
    source.exec(
      "CREATE TABLE IF NOT EXISTS nopk (data TEXT)"
    );
    source.run("INSERT INTO nopk (data) VALUES (?)", "hello");
    target.exec("CREATE TABLE IF NOT EXISTS nopk (data TEXT)");

    const results = syncPush(source, target, { tables: ["nopk"] });
    expect(results[0].errors.length).toBeGreaterThan(0);
    expect(results[0].errors[0]).toContain('no "id" column');
  });

  test("syncPush handles empty table", () => {
    const results = syncPush(source, target, { tables: ["items"] });
    expect(results[0].rowsRead).toBe(0);
    expect(results[0].rowsWritten).toBe(0);
  });

  test("progress callback is invoked", () => {
    source.run(
      "INSERT INTO items (id, name, value) VALUES (?, ?, ?)",
      "i1",
      "Alpha",
      10
    );

    const calls: string[] = [];
    syncPush(source, target, {
      tables: ["items"],
      onProgress: (p) => calls.push(p.phase),
    });

    expect(calls).toContain("reading");
    expect(calls).toContain("writing");
    expect(calls).toContain("done");
  });
});

describe("listSqliteTables", () => {
  test("lists user tables", () => {
    const db = new SqliteAdapter(
      join(tmpdir(), `cloud-test-listtables-${Date.now()}.db`)
    );
    db.exec("CREATE TABLE users (id TEXT PRIMARY KEY)");
    db.exec("CREATE TABLE items (id TEXT PRIMARY KEY)");

    const tables = listSqliteTables(db);
    expect(tables).toContain("users");
    expect(tables).toContain("items");
    expect(tables).not.toContain("sqlite_sequence"); // internal

    db.close();
  });
});
