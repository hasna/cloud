import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { SqliteAdapter } from "../src/adapter";
import { SyncProgressTracker, type SyncProgressInfo } from "../src/sync-progress";
import { unlinkSync, existsSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";

const DB_PATH = join(tmpdir(), `cloud-test-sync-progress-${Date.now()}.db`);

describe("SyncProgressTracker", () => {
  let db: SqliteAdapter;
  let tracker: SyncProgressTracker;

  beforeEach(() => {
    db = new SqliteAdapter(DB_PATH);
    tracker = new SyncProgressTracker(db);
  });

  afterEach(() => {
    db.close();
    for (const suffix of ["", "-wal", "-shm"]) {
      try {
        if (existsSync(DB_PATH + suffix)) unlinkSync(DB_PATH + suffix);
      } catch {}
    }
  });

  test("creates _sync_resume table on construction", () => {
    const tables = db.all(
      `SELECT name FROM sqlite_master WHERE type='table' AND name='_sync_resume'`
    );
    expect(tables).toHaveLength(1);
  });

  test("start() initializes progress for a table", () => {
    tracker.start("items", 100, "push");

    const progress = tracker.getProgress("items");
    expect(progress).not.toBeNull();
    expect(progress!.table).toBe("items");
    expect(progress!.total).toBe(100);
    expect(progress!.done).toBe(0);
    expect(progress!.percent).toBe(0);
    expect(progress!.status).toBe("in_progress");
  });

  test("start() sets status to resumed if previous sync was interrupted", () => {
    // Simulate an interrupted sync
    db.run(
      `INSERT INTO _sync_resume (table_name, last_row_id, direction, started_at, status)
       VALUES (?, ?, ?, datetime('now'), ?)`,
      "items",
      "row-50",
      "push",
      "in_progress"
    );

    tracker.start("items", 100, "push");

    const progress = tracker.getProgress("items");
    expect(progress!.status).toBe("resumed");
  });

  test("update() tracks progress and calculates ETA", () => {
    tracker.start("items", 100, "push");

    // Simulate some time passing (update after processing 50 rows)
    tracker.update("items", 50, "row-50");

    const progress = tracker.getProgress("items");
    expect(progress).not.toBeNull();
    expect(progress!.done).toBe(50);
    expect(progress!.percent).toBe(50);
    expect(progress!.elapsed_ms).toBeGreaterThanOrEqual(0);
    expect(progress!.status).toBe("in_progress");
  });

  test("update() stores resume point in database", () => {
    tracker.start("items", 100, "push");
    tracker.update("items", 25, "row-25");

    const resume = db.get(
      `SELECT * FROM _sync_resume WHERE table_name = ?`,
      "items"
    );
    expect(resume).not.toBeNull();
    expect(resume.last_row_id).toBe("row-25");
    expect(resume.status).toBe("in_progress");
  });

  test("markComplete() sets status to completed", () => {
    tracker.start("items", 100, "push");
    tracker.update("items", 100, "row-100");
    tracker.markComplete("items");

    const progress = tracker.getProgress("items");
    expect(progress!.status).toBe("completed");
    expect(progress!.percent).toBe(100);
    expect(progress!.done).toBe(100);
    expect(progress!.eta_ms).toBe(0);

    const resume = db.get(
      `SELECT * FROM _sync_resume WHERE table_name = ?`,
      "items"
    );
    expect(resume.status).toBe("completed");
  });

  test("markFailed() sets status to failed", () => {
    tracker.start("items", 100, "push");
    tracker.update("items", 30, "row-30");
    tracker.markFailed("items", "Connection lost");

    const progress = tracker.getProgress("items");
    expect(progress!.status).toBe("failed");

    const resume = db.get(
      `SELECT * FROM _sync_resume WHERE table_name = ?`,
      "items"
    );
    expect(resume.status).toBe("failed");
  });

  test("canResume() returns true for interrupted sync", () => {
    tracker.start("items", 100, "push");
    tracker.update("items", 50, "row-50");

    // Simulate crash — status stays in_progress
    expect(tracker.canResume("items")).toBe(true);
  });

  test("canResume() returns false for completed sync", () => {
    tracker.start("items", 100, "push");
    tracker.markComplete("items");

    expect(tracker.canResume("items")).toBe(false);
  });

  test("canResume() returns false for unknown table", () => {
    expect(tracker.canResume("unknown_table")).toBe(false);
  });

  test("getResumePoint() returns last row ID and direction", () => {
    tracker.start("items", 100, "push");
    tracker.update("items", 50, "row-50");

    const resume = tracker.getResumePoint("items");
    expect(resume).not.toBeNull();
    expect(resume!.table_name).toBe("items");
    expect(resume!.last_row_id).toBe("row-50");
    expect(resume!.direction).toBe("push");
    expect(resume!.status).toBe("in_progress");
  });

  test("getResumePoint() returns null for completed table", () => {
    tracker.start("items", 100, "push");
    tracker.markComplete("items");

    const resume = tracker.getResumePoint("items");
    expect(resume).toBeNull();
  });

  test("clearResume() removes resume state", () => {
    tracker.start("items", 100, "push");
    tracker.update("items", 50, "row-50");
    tracker.clearResume("items");

    expect(tracker.canResume("items")).toBe(false);
    expect(tracker.getProgress("items")).toBeNull();

    const row = db.get(
      `SELECT * FROM _sync_resume WHERE table_name = ?`,
      "items"
    );
    expect(row).toBeNull();
  });

  test("getAllProgress() returns all tracked tables", () => {
    tracker.start("items", 100, "push");
    tracker.start("users", 50, "push");

    const all = tracker.getAllProgress();
    expect(all).toHaveLength(2);
    expect(all.map((p) => p.table).sort()).toEqual(["items", "users"]);
  });

  test("listResumeRecords() returns all resume records", () => {
    tracker.start("items", 100, "push");
    tracker.start("users", 50, "pull");

    const records = tracker.listResumeRecords();
    expect(records.length).toBeGreaterThanOrEqual(2);
  });

  test("progress callback is invoked on start, update, complete", () => {
    const events: SyncProgressInfo[] = [];
    const tracked = new SyncProgressTracker(db, (p) => events.push(p));

    tracked.start("items", 10, "push");
    tracked.update("items", 5, "row-5");
    tracked.markComplete("items");

    expect(events.length).toBe(3);
    expect(events[0].status).toBe("in_progress");
    expect(events[1].status).toBe("in_progress");
    expect(events[1].done).toBe(5);
    expect(events[2].status).toBe("completed");
  });

  test("progress callback is invoked on failure", () => {
    const events: SyncProgressInfo[] = [];
    const tracked = new SyncProgressTracker(db, (p) => events.push(p));

    tracked.start("items", 10, "push");
    tracked.markFailed("items", "boom");

    expect(events.length).toBe(2);
    expect(events[1].status).toBe("failed");
  });

  test("percent calculation handles zero-total tables", () => {
    tracker.start("empty", 0, "push");
    tracker.update("empty", 0, "");

    const progress = tracker.getProgress("empty");
    expect(progress!.percent).toBe(0);
  });
});
