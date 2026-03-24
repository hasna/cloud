import type { DbAdapter } from "./adapter.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface SyncProgressInfo {
  table: string;
  total: number;
  done: number;
  percent: number;
  elapsed_ms: number;
  eta_ms: number;
  status: "pending" | "in_progress" | "completed" | "failed" | "resumed";
}

export type ProgressCallback = (progress: SyncProgressInfo) => void;

export interface ResumePoint {
  table_name: string;
  last_row_id: string;
  direction: string;
  started_at: string;
  status: string;
}

// ---------------------------------------------------------------------------
// SyncProgressTracker — tracks per-table progress + resume state
// ---------------------------------------------------------------------------

export class SyncProgressTracker {
  private db: DbAdapter;
  private progress: Map<string, SyncProgressInfo> = new Map();
  private startTimes: Map<string, number> = new Map();
  private callback?: ProgressCallback;

  constructor(db: DbAdapter, callback?: ProgressCallback) {
    this.db = db;
    this.callback = callback;
    this.ensureResumeTable();
  }

  // -------------------------------------------------------------------------
  // Resume table management
  // -------------------------------------------------------------------------

  private ensureResumeTable(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS _sync_resume (
        table_name TEXT PRIMARY KEY,
        last_row_id TEXT,
        direction TEXT,
        started_at TEXT,
        status TEXT DEFAULT 'in_progress'
      )
    `);
  }

  // -------------------------------------------------------------------------
  // Progress tracking
  // -------------------------------------------------------------------------

  /**
   * Start tracking a table sync. Sets status to in_progress or resumed.
   */
  start(table: string, total: number, direction: string): void {
    const resumed = this.canResume(table);
    const now = Date.now();
    this.startTimes.set(table, now);

    const status = resumed ? "resumed" : "in_progress";

    const info: SyncProgressInfo = {
      table,
      total,
      done: 0,
      percent: 0,
      elapsed_ms: 0,
      eta_ms: 0,
      status,
    };
    this.progress.set(table, info);

    // Upsert resume record
    this.db.run(
      `INSERT INTO _sync_resume (table_name, last_row_id, direction, started_at, status)
       VALUES (?, ?, ?, datetime('now'), ?)
       ON CONFLICT (table_name) DO UPDATE SET
         direction = excluded.direction,
         started_at = datetime('now'),
         status = excluded.status`,
      table,
      "",
      direction,
      status
    );

    this.notify(table);
  }

  /**
   * Update progress for a table after processing rows.
   */
  update(table: string, done: number, lastRowId: string): void {
    const info = this.progress.get(table);
    if (!info) return;

    const startTime = this.startTimes.get(table) ?? Date.now();
    const elapsed = Date.now() - startTime;
    const rate = done > 0 ? elapsed / done : 0; // ms per row
    const remaining = info.total - done;
    const eta = remaining > 0 ? Math.round(rate * remaining) : 0;

    info.done = done;
    info.percent = info.total > 0 ? Math.round((done / info.total) * 100) : 0;
    info.elapsed_ms = elapsed;
    info.eta_ms = eta;
    info.status = "in_progress";

    // Update resume point
    this.db.run(
      `UPDATE _sync_resume SET last_row_id = ?, status = 'in_progress' WHERE table_name = ?`,
      lastRowId,
      table
    );

    this.notify(table);
  }

  /**
   * Mark a table sync as completed.
   */
  markComplete(table: string): void {
    const info = this.progress.get(table);
    if (info) {
      const startTime = this.startTimes.get(table) ?? Date.now();
      info.elapsed_ms = Date.now() - startTime;
      info.done = info.total;
      info.percent = 100;
      info.eta_ms = 0;
      info.status = "completed";
      this.notify(table);
    }

    this.db.run(
      `UPDATE _sync_resume SET status = 'completed' WHERE table_name = ?`,
      table
    );
  }

  /**
   * Mark a table sync as failed.
   */
  markFailed(table: string, _error: string): void {
    const info = this.progress.get(table);
    if (info) {
      const startTime = this.startTimes.get(table) ?? Date.now();
      info.elapsed_ms = Date.now() - startTime;
      info.status = "failed";
      this.notify(table);
    }

    this.db.run(
      `UPDATE _sync_resume SET status = 'failed' WHERE table_name = ?`,
      table
    );
  }

  // -------------------------------------------------------------------------
  // Resume support
  // -------------------------------------------------------------------------

  /**
   * Check if a previous sync was interrupted (status is 'in_progress' or 'resumed').
   */
  canResume(table: string): boolean {
    const row = this.db.get(
      `SELECT status FROM _sync_resume WHERE table_name = ?`,
      table
    );
    if (!row) return false;
    return row.status === "in_progress" || row.status === "resumed";
  }

  /**
   * Returns the last successfully synced row ID for a table, or null.
   */
  getResumePoint(table: string): ResumePoint | null {
    const row = this.db.get(
      `SELECT table_name, last_row_id, direction, started_at, status FROM _sync_resume WHERE table_name = ?`,
      table
    );
    if (!row) return null;
    if (row.status !== "in_progress" && row.status !== "resumed") return null;
    return row as ResumePoint;
  }

  /**
   * Clear resume state for a table (e.g., after a fresh sync starts).
   */
  clearResume(table: string): void {
    this.db.run(
      `DELETE FROM _sync_resume WHERE table_name = ?`,
      table
    );
    this.progress.delete(table);
    this.startTimes.delete(table);
  }

  // -------------------------------------------------------------------------
  // Query
  // -------------------------------------------------------------------------

  /**
   * Get current progress info for a table.
   */
  getProgress(table: string): SyncProgressInfo | null {
    return this.progress.get(table) ?? null;
  }

  /**
   * Get progress info for all tracked tables.
   */
  getAllProgress(): SyncProgressInfo[] {
    return Array.from(this.progress.values());
  }

  /**
   * List all resume records from the database (including historical).
   */
  listResumeRecords(): ResumePoint[] {
    return this.db.all(
      `SELECT table_name, last_row_id, direction, started_at, status FROM _sync_resume ORDER BY started_at DESC`
    ) as ResumePoint[];
  }

  // -------------------------------------------------------------------------
  // Private
  // -------------------------------------------------------------------------

  private notify(table: string): void {
    const info = this.progress.get(table);
    if (info && this.callback) {
      this.callback({ ...info });
    }
  }
}
