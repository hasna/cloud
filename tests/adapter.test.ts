import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { SqliteAdapter } from "../src/adapter";
import { unlinkSync, existsSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";

const TEST_DB = join(tmpdir(), `cloud-test-adapter-${Date.now()}.db`);

describe("SqliteAdapter", () => {
  let db: SqliteAdapter;

  beforeEach(() => {
    db = new SqliteAdapter(TEST_DB);
    db.exec(`
      CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `);
  });

  afterEach(() => {
    db.close();
    try {
      if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
      if (existsSync(TEST_DB + "-wal")) unlinkSync(TEST_DB + "-wal");
      if (existsSync(TEST_DB + "-shm")) unlinkSync(TEST_DB + "-shm");
    } catch {}
  });

  test("run() inserts a row and returns changes", () => {
    const result = db.run(
      "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
      "u1",
      "Alice",
      "alice@example.com"
    );
    expect(result.changes).toBe(1);
  });

  test("get() returns a single row", () => {
    db.run(
      "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
      "u1",
      "Bob",
      "bob@example.com"
    );
    const row = db.get("SELECT * FROM users WHERE id = ?", "u1");
    expect(row).not.toBeNull();
    expect(row.name).toBe("Bob");
    expect(row.email).toBe("bob@example.com");
  });

  test("get() returns null for no match", () => {
    const row = db.get("SELECT * FROM users WHERE id = ?", "nonexistent");
    expect(row).toBeNull();
  });

  test("all() returns all matching rows", () => {
    db.run("INSERT INTO users (id, name) VALUES (?, ?)", "u1", "Alice");
    db.run("INSERT INTO users (id, name) VALUES (?, ?)", "u2", "Bob");
    db.run("INSERT INTO users (id, name) VALUES (?, ?)", "u3", "Charlie");

    const rows = db.all("SELECT * FROM users ORDER BY name");
    expect(rows).toHaveLength(3);
    expect(rows[0].name).toBe("Alice");
    expect(rows[2].name).toBe("Charlie");
  });

  test("exec() runs DDL statements", () => {
    db.exec("CREATE TABLE IF NOT EXISTS tags (id TEXT PRIMARY KEY, label TEXT)");
    db.run("INSERT INTO tags (id, label) VALUES (?, ?)", "t1", "important");
    const row = db.get("SELECT * FROM tags WHERE id = ?", "t1");
    expect(row.label).toBe("important");
  });

  test("prepare() returns a reusable statement", () => {
    const stmt = db.prepare(
      "INSERT INTO users (id, name) VALUES (?, ?)"
    );
    stmt.run("u1", "Alice");
    stmt.run("u2", "Bob");
    stmt.finalize();

    const rows = db.all("SELECT * FROM users ORDER BY name");
    expect(rows).toHaveLength(2);
  });

  test("transaction() commits on success", () => {
    db.transaction(() => {
      db.run("INSERT INTO users (id, name) VALUES (?, ?)", "u1", "Alice");
      db.run("INSERT INTO users (id, name) VALUES (?, ?)", "u2", "Bob");
    });

    const rows = db.all("SELECT * FROM users");
    expect(rows).toHaveLength(2);
  });

  test("transaction() rolls back on error", () => {
    try {
      db.transaction(() => {
        db.run("INSERT INTO users (id, name) VALUES (?, ?)", "u1", "Alice");
        throw new Error("deliberate failure");
      });
    } catch {}

    const rows = db.all("SELECT * FROM users");
    expect(rows).toHaveLength(0);
  });

  test("run() updates and returns changes count", () => {
    db.run("INSERT INTO users (id, name) VALUES (?, ?)", "u1", "Alice");
    db.run("INSERT INTO users (id, name) VALUES (?, ?)", "u2", "Bob");

    const result = db.run(
      "UPDATE users SET email = ? WHERE name = ?",
      "alice@test.com",
      "Alice"
    );
    expect(result.changes).toBe(1);
  });

  test("run() deletes and returns changes count", () => {
    db.run("INSERT INTO users (id, name) VALUES (?, ?)", "u1", "Alice");
    db.run("INSERT INTO users (id, name) VALUES (?, ?)", "u2", "Bob");

    const result = db.run("DELETE FROM users WHERE id = ?", "u1");
    expect(result.changes).toBe(1);

    const rows = db.all("SELECT * FROM users");
    expect(rows).toHaveLength(1);
  });
});
