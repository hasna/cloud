import { hostname } from "os";
import type { DbAdapter } from "./adapter.js";
import { getCloudConfig } from "./config.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface Feedback {
  id?: string;
  service: string;
  version?: string;
  message: string;
  email?: string;
  machine_id?: string;
  created_at?: string;
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

const FEEDBACK_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS feedback (
  id TEXT PRIMARY KEY,
  service TEXT NOT NULL,
  version TEXT DEFAULT '',
  message TEXT NOT NULL,
  email TEXT DEFAULT '',
  machine_id TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now'))
)`;

/**
 * Ensure the feedback table exists in the given database.
 */
export function ensureFeedbackTable(db: DbAdapter): void {
  db.exec(FEEDBACK_TABLE_SQL);
}

// ---------------------------------------------------------------------------
// Local save
// ---------------------------------------------------------------------------

/**
 * Save feedback to the local database.
 */
export function saveFeedback(db: DbAdapter, feedback: Feedback): string {
  ensureFeedbackTable(db);

  const id =
    feedback.id ??
    Math.random().toString(36).slice(2) + Date.now().toString(36);
  const now = new Date().toISOString();
  const machineId = feedback.machine_id ?? hostname();

  db.run(
    `INSERT INTO feedback (id, service, version, message, email, machine_id, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    id,
    feedback.service,
    feedback.version ?? "",
    feedback.message,
    feedback.email ?? "",
    machineId,
    feedback.created_at ?? now
  );

  return id;
}

// ---------------------------------------------------------------------------
// Remote send
// ---------------------------------------------------------------------------

/**
 * Send feedback to the remote endpoint.
 * If the POST fails, saves locally and does NOT throw.
 */
export async function sendFeedback(
  feedback: Feedback,
  db?: DbAdapter
): Promise<{ sent: boolean; id: string; error?: string }> {
  const config = getCloudConfig();
  const id =
    feedback.id ??
    Math.random().toString(36).slice(2) + Date.now().toString(36);
  const machineId = feedback.machine_id ?? hostname();
  const now = new Date().toISOString();

  const payload = {
    id,
    service: feedback.service,
    version: feedback.version ?? "",
    message: feedback.message,
    email: feedback.email ?? "",
    machine_id: machineId,
    created_at: feedback.created_at ?? now,
  };

  try {
    const res = await fetch(config.feedback_endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(10_000),
    });

    if (!res.ok) {
      throw new Error(`HTTP ${res.status}: ${res.statusText}`);
    }

    // Also save locally for record keeping
    if (db) {
      try {
        saveFeedback(db, { ...feedback, id });
      } catch {
        // Non-critical — remote send succeeded
      }
    }

    return { sent: true, id };
  } catch (err: any) {
    const errorMsg = err?.message ?? String(err);

    // Save locally as fallback
    if (db) {
      try {
        saveFeedback(db, { ...feedback, id });
      } catch {
        // Can't even save locally
      }
    }

    return { sent: false, id, error: errorMsg };
  }
}

// ---------------------------------------------------------------------------
// List unsent feedback
// ---------------------------------------------------------------------------

/**
 * Get all feedback entries from the local database.
 */
export function listFeedback(db: DbAdapter): Feedback[] {
  ensureFeedbackTable(db);
  return db.all(
    `SELECT id, service, version, message, email, machine_id, created_at FROM feedback ORDER BY created_at DESC`
  );
}
