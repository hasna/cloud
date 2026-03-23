import { describe, test, expect } from "bun:test";
import { translateSql, translateDdl, translateParams } from "../src/dialect";

describe("translateSql", () => {
  test("returns SQL unchanged for sqlite dialect", () => {
    const sql = "SELECT * FROM users WHERE id = ?";
    expect(translateSql(sql, "sqlite")).toBe(sql);
  });

  test("translates ? to $N positional params", () => {
    const sql = "INSERT INTO users (id, name, email) VALUES (?, ?, ?)";
    const result = translateSql(sql, "pg");
    expect(result).toBe(
      "INSERT INTO users (id, name, email) VALUES ($1, $2, $3)"
    );
  });

  test("translates datetime('now') to NOW()", () => {
    const sql = "SELECT * FROM events WHERE created_at > datetime('now')";
    const result = translateSql(sql, "pg");
    expect(result).toContain("NOW()");
    expect(result).not.toContain("datetime");
  });

  test("translates datetime('now', '-30 minutes')", () => {
    const sql =
      "SELECT * FROM events WHERE created_at > datetime('now', '-30 minutes')";
    const result = translateSql(sql, "pg");
    expect(result).toContain("NOW() - INTERVAL '30 minutes'");
  });

  test("translates datetime('now', '-1 hour')", () => {
    const sql =
      "SELECT * FROM events WHERE created_at > datetime('now', '-1 hour')";
    const result = translateSql(sql, "pg");
    expect(result).toContain("NOW() - INTERVAL '1 hour'");
  });

  test("translates lower(hex(randomblob(16))) to gen_random_uuid()", () => {
    const sql = "SELECT lower(hex(randomblob(16))) as id";
    const result = translateSql(sql, "pg");
    expect(result).toContain("gen_random_uuid()::text");
  });

  test("translates GROUP_CONCAT to STRING_AGG", () => {
    const sql = "SELECT GROUP_CONCAT(name, ',') FROM users";
    const result = translateSql(sql, "pg");
    expect(result).toContain("STRING_AGG(name, ',')");
  });

  test("translates json_extract to ->> operator", () => {
    const sql = "SELECT json_extract(data, '$.name') FROM records";
    const result = translateSql(sql, "pg");
    expect(result).toContain("data->>'name'");
  });

  test("translates LIKE to ILIKE", () => {
    const sql = "SELECT * FROM users WHERE name LIKE ?";
    const result = translateSql(sql, "pg");
    expect(result).toContain("ILIKE");
  });

  test("does not double-translate ILIKE", () => {
    const sql = "SELECT * FROM users WHERE name ILIKE ?";
    const result = translateSql(sql, "pg");
    // Should have exactly one ILIKE, not IILIKE
    expect(result).not.toContain("IILIKE");
    expect(result).toContain("ILIKE");
  });

  test("translates IFNULL to COALESCE", () => {
    const sql = "SELECT IFNULL(name, 'unknown') FROM users";
    const result = translateSql(sql, "pg");
    expect(result).toContain("COALESCE(name, 'unknown')");
  });

  test("translates INSERT OR REPLACE INTO", () => {
    const sql = "INSERT OR REPLACE INTO users (id, name) VALUES (?, ?)";
    const result = translateSql(sql, "pg");
    expect(result).toContain("INSERT INTO");
    expect(result).not.toContain("OR REPLACE");
  });

  test("translates INSERT OR IGNORE INTO", () => {
    const sql = "INSERT OR IGNORE INTO users (id, name) VALUES (?, ?)";
    const result = translateSql(sql, "pg");
    expect(result).toContain("INSERT INTO");
    expect(result).not.toContain("OR IGNORE");
  });

  test("translates AUTOINCREMENT", () => {
    const sql =
      "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)";
    const result = translateSql(sql, "pg");
    expect(result).not.toContain("AUTOINCREMENT");
    expect(result).toContain("GENERATED ALWAYS AS IDENTITY");
  });
});

describe("translateDdl", () => {
  test("returns DDL unchanged for sqlite dialect", () => {
    const ddl =
      "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)";
    expect(translateDdl(ddl, "sqlite")).toBe(ddl);
  });

  test("translates INTEGER PRIMARY KEY AUTOINCREMENT to BIGSERIAL", () => {
    const ddl =
      "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)";
    const result = translateDdl(ddl, "pg");
    expect(result).toContain("BIGSERIAL PRIMARY KEY");
  });

  test("translates REAL to DOUBLE PRECISION", () => {
    const ddl = "CREATE TABLE t (id TEXT PRIMARY KEY, score REAL)";
    const result = translateDdl(ddl, "pg");
    expect(result).toContain("DOUBLE PRECISION");
  });

  test("translates BLOB to BYTEA", () => {
    const ddl = "CREATE TABLE t (id TEXT PRIMARY KEY, data BLOB)";
    const result = translateDdl(ddl, "pg");
    expect(result).toContain("BYTEA");
  });
});

describe("translateParams", () => {
  test("flattens single array argument", () => {
    expect(translateParams([["a", "b", "c"]])).toEqual(["a", "b", "c"]);
  });

  test("passes through variadic arguments", () => {
    expect(translateParams(["a", "b", "c"])).toEqual(["a", "b", "c"]);
  });

  test("converts undefined to null", () => {
    expect(translateParams(["a", undefined, "c"])).toEqual(["a", null, "c"]);
  });
});
