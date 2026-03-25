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

  test("syncPush copies rows from source to target", async () => {
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

    // For SQLite-to-SQLite tests, we pass target as PgAdapterAsync-compatible
    // but syncPush expects (local: DbAdapter, remote: PgAdapterAsync, ...)
    // Since both are SQLite here, we use syncPush with source as local
    // and a mock async adapter wrapping the target.
    // Actually, the sync engine handles both sync and async adapters internally.
    const results = await syncPush(source, target as any, { tables: ["items"] });
    expect(results).toHaveLength(1);
    expect(results[0].rowsWritten).toBe(2);
    expect(results[0].errors).toHaveLength(0);

    const rows = target.all("SELECT * FROM items ORDER BY id");
    expect(rows).toHaveLength(2);
    expect(rows[0].name).toBe("Alpha");
    expect(rows[1].name).toBe("Beta");
  });

  test("syncPush upserts existing rows", async () => {
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

    const results = await syncPush(source, target as any, { tables: ["items"] });
    expect(results[0].rowsWritten).toBe(1);

    const row = target.get("SELECT * FROM items WHERE id = ?", "i1");
    expect(row.name).toBe("New");
    expect(row.value).toBe(99);
  });

  test("syncPull copies rows from cloud to local (reverse)", async () => {
    target.run(
      "INSERT INTO items (id, name, value) VALUES (?, ?, ?)",
      "i1",
      "CloudItem",
      42
    );

    // syncPull(remote, local, options) — target acts as "remote", source as "local"
    const results = await syncPull(target as any, source, { tables: ["items"] });
    expect(results[0].rowsWritten).toBe(1);

    const row = source.get("SELECT * FROM items WHERE id = ?", "i1");
    expect(row.name).toBe("CloudItem");
    expect(row.value).toBe(42);
  });

  test("syncPush inserts rows for table without primary key (with warning)", async () => {
    source.exec(
      "CREATE TABLE IF NOT EXISTS nopk (data TEXT)"
    );
    source.run("INSERT INTO nopk (data) VALUES (?)", "hello");
    target.exec("CREATE TABLE IF NOT EXISTS nopk (data TEXT)");

    const results = await syncPush(source, target as any, { tables: ["nopk"] });
    expect(results[0].errors.length).toBeGreaterThan(0);
    expect(results[0].errors[0]).toContain("no primary key");
    // Rows should still be inserted (without conflict handling)
    expect(results[0].rowsWritten).toBe(1);
    const rows = target.all("SELECT * FROM nopk");
    expect(rows).toHaveLength(1);
    expect(rows[0].data).toBe("hello");
  });

  test("syncPush handles empty table", async () => {
    const results = await syncPush(source, target as any, { tables: ["items"] });
    expect(results[0].rowsRead).toBe(0);
    expect(results[0].rowsWritten).toBe(0);
  });

  test("progress callback is invoked", async () => {
    source.run(
      "INSERT INTO items (id, name, value) VALUES (?, ?, ?)",
      "i1",
      "Alpha",
      10
    );

    const calls: string[] = [];
    await syncPush(source, target as any, {
      tables: ["items"],
      onProgress: (p) => calls.push(p.phase),
    });

    expect(calls).toContain("reading");
    expect(calls).toContain("writing");
    expect(calls).toContain("done");
  });

  test("syncPush handles composite primary keys", async () => {
    // Create a table with composite PK (like task_tags)
    const schema = `
      CREATE TABLE IF NOT EXISTS task_tags (
        task_id TEXT NOT NULL,
        tag_id TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (task_id, tag_id)
      )
    `;
    source.exec(schema);
    target.exec(schema);

    source.run(
      "INSERT INTO task_tags (task_id, tag_id) VALUES (?, ?)",
      "t1",
      "tag-a"
    );
    source.run(
      "INSERT INTO task_tags (task_id, tag_id) VALUES (?, ?)",
      "t1",
      "tag-b"
    );
    source.run(
      "INSERT INTO task_tags (task_id, tag_id) VALUES (?, ?)",
      "t2",
      "tag-a"
    );

    const results = await syncPush(source, target as any, {
      tables: ["task_tags"],
    });
    expect(results).toHaveLength(1);
    expect(results[0].rowsWritten).toBe(3);
    expect(results[0].errors).toHaveLength(0);

    const rows = target.all("SELECT * FROM task_tags ORDER BY task_id, tag_id");
    expect(rows).toHaveLength(3);
    expect(rows[0].task_id).toBe("t1");
    expect(rows[0].tag_id).toBe("tag-a");
    expect(rows[2].task_id).toBe("t2");
    expect(rows[2].tag_id).toBe("tag-a");
  });

  test("syncPush upserts with composite primary keys (no duplicates)", async () => {
    const schema = `
      CREATE TABLE IF NOT EXISTS task_deps (
        task_id TEXT NOT NULL,
        depends_on TEXT NOT NULL,
        dep_type TEXT DEFAULT 'blocks',
        PRIMARY KEY (task_id, depends_on)
      )
    `;
    source.exec(schema);
    target.exec(schema);

    // Pre-existing row in target
    target.run(
      "INSERT INTO task_deps (task_id, depends_on, dep_type) VALUES (?, ?, ?)",
      "t1",
      "t2",
      "old-type"
    );

    // Source has updated version of same row + a new row
    source.run(
      "INSERT INTO task_deps (task_id, depends_on, dep_type) VALUES (?, ?, ?)",
      "t1",
      "t2",
      "blocks"
    );
    source.run(
      "INSERT INTO task_deps (task_id, depends_on, dep_type) VALUES (?, ?, ?)",
      "t1",
      "t3",
      "requires"
    );

    const results = await syncPush(source, target as any, {
      tables: ["task_deps"],
    });
    expect(results[0].rowsWritten).toBe(2);
    expect(results[0].errors).toHaveLength(0);

    const rows = target.all(
      "SELECT * FROM task_deps ORDER BY task_id, depends_on"
    );
    expect(rows).toHaveLength(2);
    // The upsert should have updated the existing row
    expect(rows[0].dep_type).toBe("blocks");
    expect(rows[1].dep_type).toBe("requires");
  });

  test("syncPull handles composite primary keys", async () => {
    const schema = `
      CREATE TABLE IF NOT EXISTS task_tags (
        task_id TEXT NOT NULL,
        tag_id TEXT NOT NULL,
        PRIMARY KEY (task_id, tag_id)
      )
    `;
    source.exec(schema);
    target.exec(schema);

    // target acts as "remote" for pull
    target.run(
      "INSERT INTO task_tags (task_id, tag_id) VALUES (?, ?)",
      "t1",
      "tag-x"
    );

    const results = await syncPull(target as any, source, {
      tables: ["task_tags"],
    });
    expect(results[0].rowsWritten).toBe(1);
    expect(results[0].errors).toHaveLength(0);

    const rows = source.all("SELECT * FROM task_tags");
    expect(rows).toHaveLength(1);
    expect(rows[0].task_id).toBe("t1");
    expect(rows[0].tag_id).toBe("tag-x");
  });

  test("batch upsert handles multiple rows efficiently", async () => {
    // Insert 250 rows to test batching (default batch size = 100)
    for (let i = 0; i < 250; i++) {
      source.run(
        "INSERT INTO items (id, name, value) VALUES (?, ?, ?)",
        `item-${i}`,
        `Item ${i}`,
        i
      );
    }

    const results = await syncPush(source, target as any, { tables: ["items"] });
    expect(results[0].rowsRead).toBe(250);
    expect(results[0].rowsWritten).toBe(250);
    expect(results[0].errors).toHaveLength(0);

    const count = target.get("SELECT COUNT(*) as cnt FROM items");
    expect(count.cnt).toBe(250);
  });
});

describe("sync — FK constraint handling", () => {
  let source: SqliteAdapter;
  let target: SqliteAdapter;

  const DB_SRC_FK = join(tmpdir(), `cloud-test-fk-src-${Date.now()}.db`);
  const DB_TGT_FK = join(tmpdir(), `cloud-test-fk-tgt-${Date.now()}.db`);

  beforeEach(() => {
    source = new SqliteAdapter(DB_SRC_FK);
    target = new SqliteAdapter(DB_TGT_FK);

    // Create parent + child tables with FK constraint in both
    source.exec(`
      CREATE TABLE IF NOT EXISTS projects (id TEXT PRIMARY KEY, name TEXT);
      CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY,
        project_id TEXT REFERENCES projects(id),
        title TEXT
      );
    `);
    target.exec(`
      CREATE TABLE IF NOT EXISTS projects (id TEXT PRIMARY KEY, name TEXT);
      CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY,
        project_id TEXT REFERENCES projects(id),
        title TEXT
      );
    `);
  });

  afterEach(() => {
    source.close();
    target.close();
    for (const f of [DB_SRC_FK, DB_TGT_FK]) {
      try { if (existsSync(f)) unlinkSync(f); } catch {}
      try { if (existsSync(f + "-wal")) unlinkSync(f + "-wal"); } catch {}
      try { if (existsSync(f + "-shm")) unlinkSync(f + "-shm"); } catch {}
    }
  });

  test("syncPull succeeds with FK-dependent tables (FK disabled during sync)", async () => {
    // Insert data in source with FK relationship
    source.run("INSERT INTO projects (id, name) VALUES (?, ?)", "p1", "Project1");
    source.run("INSERT INTO tasks (id, project_id, title) VALUES (?, ?, ?)", "t1", "p1", "Task1");

    // Pull: tasks may arrive before projects if table ordering fails.
    // The sync should disable FK checks so this doesn't error.
    const results = await syncPull(source as any, target, {
      tables: ["tasks", "projects"], // intentionally wrong order
    });

    const totalWritten = results.reduce((s, r) => s + r.rowsWritten, 0);
    const totalErrors = results.reduce((s, r) => s + r.errors.length, 0);
    expect(totalWritten).toBe(2);
    expect(totalErrors).toBe(0);

    const task = target.get("SELECT * FROM tasks WHERE id = ?", "t1");
    expect(task.project_id).toBe("p1");
  });

  test("FK constraints are re-enabled after sync completes", async () => {
    source.run("INSERT INTO projects (id, name) VALUES (?, ?)", "p1", "Project1");

    await syncPull(source as any, target, { tables: ["projects"] });

    // After sync, FK should be back on. Inserting a task with invalid FK should fail.
    expect(() => {
      target.run("INSERT INTO tasks (id, project_id, title) VALUES (?, ?, ?)", "t1", "nonexistent", "Bad");
    }).toThrow();
  });
});

describe("sync — column filtering (schema drift)", () => {
  let source: SqliteAdapter;
  let target: SqliteAdapter;

  const DB_SRC_COL = join(tmpdir(), `cloud-test-col-src-${Date.now()}.db`);
  const DB_TGT_COL = join(tmpdir(), `cloud-test-col-tgt-${Date.now()}.db`);

  beforeEach(() => {
    source = new SqliteAdapter(DB_SRC_COL);
    target = new SqliteAdapter(DB_TGT_COL);
  });

  afterEach(() => {
    source.close();
    target.close();
    for (const f of [DB_SRC_COL, DB_TGT_COL]) {
      try { if (existsSync(f)) unlinkSync(f); } catch {}
      try { if (existsSync(f + "-wal")) unlinkSync(f + "-wal"); } catch {}
      try { if (existsSync(f + "-shm")) unlinkSync(f + "-shm"); } catch {}
    }
  });

  test("syncPull drops columns that exist in source but not target", async () => {
    // Source has extra columns that target doesn't
    source.exec(`
      CREATE TABLE items (
        id TEXT PRIMARY KEY,
        name TEXT,
        search_vector TEXT,
        extra_col TEXT
      )
    `);
    target.exec(`
      CREATE TABLE items (
        id TEXT PRIMARY KEY,
        name TEXT
      )
    `);

    source.run("INSERT INTO items (id, name, search_vector, extra_col) VALUES (?, ?, ?, ?)",
      "i1", "Alpha", "tsvector_data", "extra");

    const results = await syncPull(source as any, target, { tables: ["items"] });
    expect(results[0].rowsWritten).toBe(1);
    expect(results[0].errors).toHaveLength(0);

    const row = target.get("SELECT * FROM items WHERE id = ?", "i1");
    expect(row.name).toBe("Alpha");
    expect(row.search_vector).toBeUndefined();
  });

  test("syncPull works when source and target have identical schemas", async () => {
    source.exec("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT)");
    target.exec("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT)");

    source.run("INSERT INTO items (id, name) VALUES (?, ?)", "i1", "Same");

    const results = await syncPull(source as any, target, { tables: ["items"] });
    expect(results[0].rowsWritten).toBe(1);
    expect(results[0].errors).toHaveLength(0);
  });
});

describe("sync — PG type coercion for SQLite", () => {
  let source: SqliteAdapter;
  let target: SqliteAdapter;

  const DB_SRC_COERCE = join(tmpdir(), `cloud-test-coerce-src-${Date.now()}.db`);
  const DB_TGT_COERCE = join(tmpdir(), `cloud-test-coerce-tgt-${Date.now()}.db`);

  beforeEach(() => {
    source = new SqliteAdapter(DB_SRC_COERCE);
    target = new SqliteAdapter(DB_TGT_COERCE);

    const schema = `
      CREATE TABLE items (
        id TEXT PRIMARY KEY,
        data TEXT,
        count INTEGER,
        updated_at TEXT
      )
    `;
    source.exec(schema);
    target.exec(schema);
  });

  afterEach(() => {
    source.close();
    target.close();
    for (const f of [DB_SRC_COERCE, DB_TGT_COERCE]) {
      try { if (existsSync(f)) unlinkSync(f); } catch {}
      try { if (existsSync(f + "-wal")) unlinkSync(f + "-wal"); } catch {}
      try { if (existsSync(f + "-shm")) unlinkSync(f + "-shm"); } catch {}
    }
  });

  test("syncPull handles JSON objects stored as strings", async () => {
    // Simulate PG returning a JSON string for a TEXT column
    source.run("INSERT INTO items (id, data, count) VALUES (?, ?, ?)",
      "i1", JSON.stringify({ key: "value" }), 42);

    const results = await syncPull(source as any, target, { tables: ["items"] });
    expect(results[0].rowsWritten).toBe(1);
    expect(results[0].errors).toHaveLength(0);

    const row = target.get("SELECT * FROM items WHERE id = ?", "i1");
    expect(JSON.parse(row.data)).toEqual({ key: "value" });
  });

  test("syncPull handles null values", async () => {
    source.run("INSERT INTO items (id, data, count, updated_at) VALUES (?, ?, ?, ?)",
      "i1", null, null, null);

    const results = await syncPull(source as any, target, { tables: ["items"] });
    expect(results[0].rowsWritten).toBe(1);
    expect(results[0].errors).toHaveLength(0);

    const row = target.get("SELECT * FROM items WHERE id = ?", "i1");
    expect(row.data).toBeNull();
    expect(row.count).toBeNull();
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
