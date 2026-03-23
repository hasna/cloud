import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { SqliteAdapter } from "../src/adapter";
import {
  saveFeedback,
  listFeedback,
  ensureFeedbackTable,
} from "../src/feedback";
import { unlinkSync, existsSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";

const TEST_DB = join(tmpdir(), `cloud-test-feedback-${Date.now()}.db`);

describe("feedback", () => {
  let db: SqliteAdapter;

  beforeEach(() => {
    db = new SqliteAdapter(TEST_DB);
  });

  afterEach(() => {
    db.close();
    try {
      if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
      if (existsSync(TEST_DB + "-wal")) unlinkSync(TEST_DB + "-wal");
      if (existsSync(TEST_DB + "-shm")) unlinkSync(TEST_DB + "-shm");
    } catch {}
  });

  test("ensureFeedbackTable creates the table", () => {
    ensureFeedbackTable(db);
    const tables = db.all(
      "SELECT name FROM sqlite_master WHERE type='table' AND name='feedback'"
    );
    expect(tables).toHaveLength(1);
  });

  test("saveFeedback inserts a record", () => {
    const id = saveFeedback(db, {
      service: "test-service",
      version: "1.0.0",
      message: "Great tool!",
      email: "user@example.com",
    });

    expect(id).toBeTruthy();

    const row = db.get("SELECT * FROM feedback WHERE id = ?", id);
    expect(row).not.toBeNull();
    expect(row.service).toBe("test-service");
    expect(row.version).toBe("1.0.0");
    expect(row.message).toBe("Great tool!");
    expect(row.email).toBe("user@example.com");
  });

  test("saveFeedback uses provided id", () => {
    const id = saveFeedback(db, {
      id: "custom-id-123",
      service: "test-service",
      message: "Nice work",
    });

    expect(id).toBe("custom-id-123");
    const row = db.get("SELECT * FROM feedback WHERE id = ?", "custom-id-123");
    expect(row).not.toBeNull();
  });

  test("listFeedback returns all entries", () => {
    saveFeedback(db, { service: "svc-a", message: "msg1" });
    saveFeedback(db, { service: "svc-b", message: "msg2" });
    saveFeedback(db, { service: "svc-a", message: "msg3" });

    const items = listFeedback(db);
    expect(items).toHaveLength(3);
  });
});
