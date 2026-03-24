import type { DbAdapter } from "./adapter.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface IncrementalSyncStats {
  table: string;
  total_rows: number;
  synced_rows: number;
  skipped_rows: number;
  errors: string[];
  first_sync: boolean;
}

export interface SyncMeta {
  table_name: string;
  last_synced_at: string;
  last_synced_row_count: number;
  direction: "push" | "pull";
}

export interface IncrementalSyncOptions {
  /** Primary key column name (default: "id"). */
  primaryKey?: string;
  /** Conflict resolution column (default: "updated_at"). */
  conflictColumn?: string;
  /** Batch size for writes (default: 500). */
  batchSize?: number;
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

const SYNC_META_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS _sync_meta (
  table_name TEXT PRIMARY KEY,
  last_synced_at TEXT,
  last_synced_row_count INTEGER DEFAULT 0,
  direction TEXT DEFAULT 'push'
)`;

/**
 * Ensure the `_sync_meta` table exists in the given database.
 */
export function ensureSyncMetaTable(db: DbAdapter): void {
  db.exec(SYNC_META_TABLE_SQL);
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function getSyncMeta(db: DbAdapter, table: string): SyncMeta | null {
  ensureSyncMetaTable(db);
  return (
    db.get(
      `SELECT table_name, last_synced_at, last_synced_row_count, direction FROM _sync_meta WHERE table_name = ?`,
      table
    ) ?? null
  );
}

function upsertSyncMeta(
  db: DbAdapter,
  meta: SyncMeta
): void {
  ensureSyncMetaTable(db);
  const existing = db.get(
    `SELECT table_name FROM _sync_meta WHERE table_name = ?`,
    meta.table_name
  );
  if (existing) {
    db.run(
      `UPDATE _sync_meta SET last_synced_at = ?, last_synced_row_count = ?, direction = ? WHERE table_name = ?`,
      meta.last_synced_at,
      meta.last_synced_row_count,
      meta.direction,
      meta.table_name
    );
  } else {
    db.run(
      `INSERT INTO _sync_meta (table_name, last_synced_at, last_synced_row_count, direction) VALUES (?, ?, ?, ?)`,
      meta.table_name,
      meta.last_synced_at,
      meta.last_synced_row_count,
      meta.direction
    );
  }
}

function transferRows(
  source: DbAdapter,
  target: DbAdapter,
  table: string,
  rows: any[],
  options: IncrementalSyncOptions
): { written: number; skipped: number; errors: string[] } {
  const { primaryKey = "id", conflictColumn = "updated_at" } = options;
  let written = 0;
  let skipped = 0;
  const errors: string[] = [];

  if (rows.length === 0) return { written, skipped, errors };

  const columns = Object.keys(rows[0]);
  const hasConflictCol = columns.includes(conflictColumn);
  const hasPrimaryKey = columns.includes(primaryKey);

  if (!hasPrimaryKey) {
    errors.push(`Table "${table}" has no "${primaryKey}" column -- skipping`);
    return { written, skipped, errors };
  }

  for (const row of rows) {
    try {
      const existing = target.get(
        `SELECT "${primaryKey}"${hasConflictCol ? `, "${conflictColumn}"` : ""} FROM "${table}" WHERE "${primaryKey}" = ?`,
        row[primaryKey]
      );

      if (existing) {
        // Conflict resolution: newest wins
        if (
          hasConflictCol &&
          existing[conflictColumn] &&
          row[conflictColumn]
        ) {
          const existingTime = new Date(existing[conflictColumn]).getTime();
          const incomingTime = new Date(row[conflictColumn]).getTime();
          if (existingTime >= incomingTime) {
            skipped++;
            continue;
          }
        }

        // Update
        const setClauses = columns
          .filter((c) => c !== primaryKey)
          .map((c) => `"${c}" = ?`)
          .join(", ");
        const values = columns
          .filter((c) => c !== primaryKey)
          .map((c) => row[c]);
        values.push(row[primaryKey]);

        target.run(
          `UPDATE "${table}" SET ${setClauses} WHERE "${primaryKey}" = ?`,
          ...values
        );
      } else {
        // Insert
        const placeholders = columns.map(() => "?").join(", ");
        const colList = columns.map((c) => `"${c}"`).join(", ");
        const values = columns.map((c) => row[c]);

        target.run(
          `INSERT INTO "${table}" (${colList}) VALUES (${placeholders})`,
          ...values
        );
      }

      written++;
    } catch (err: any) {
      errors.push(
        `Row ${row[primaryKey]}: ${err?.message ?? String(err)}`
      );
    }
  }

  return { written, skipped, errors };
}

// ---------------------------------------------------------------------------
// Incremental Push: Local -> Remote
// ---------------------------------------------------------------------------

/**
 * Push only changed rows (since last sync) from local to remote.
 *
 * - Checks `_sync_meta` in the local DB for `last_synced_at`.
 * - If found: only selects rows where `updated_at > last_synced_at`.
 * - If not found: full push (first-time sync).
 * - After push, updates `_sync_meta` with current timestamp and row count.
 */
export function incrementalSyncPush(
  local: DbAdapter,
  remote: DbAdapter,
  tables: string[],
  options: IncrementalSyncOptions = {}
): IncrementalSyncStats[] {
  const { conflictColumn = "updated_at", batchSize = 500 } = options;
  const results: IncrementalSyncStats[] = [];

  ensureSyncMetaTable(local);

  for (const table of tables) {
    const stat: IncrementalSyncStats = {
      table,
      total_rows: 0,
      synced_rows: 0,
      skipped_rows: 0,
      errors: [],
      first_sync: false,
    };

    try {
      // Get total row count for stats
      const countResult = local.get(`SELECT COUNT(*) as cnt FROM "${table}"`);
      stat.total_rows = countResult?.cnt ?? 0;

      // Check sync meta
      const meta = getSyncMeta(local, table);
      let rows: any[];

      if (meta?.last_synced_at) {
        // Incremental: only changed rows
        try {
          rows = local.all(
            `SELECT * FROM "${table}" WHERE "${conflictColumn}" > ?`,
            meta.last_synced_at
          );
        } catch {
          // Column might not exist -- fall back to full sync
          rows = local.all(`SELECT * FROM "${table}"`);
          stat.first_sync = true;
        }
      } else {
        // First sync -- full push
        rows = local.all(`SELECT * FROM "${table}"`);
        stat.first_sync = true;
      }

      // Process in batches
      for (let offset = 0; offset < rows.length; offset += batchSize) {
        const batch = rows.slice(offset, offset + batchSize);
        const result = transferRows(local, remote, table, batch, options);
        stat.synced_rows += result.written;
        stat.skipped_rows += result.skipped;
        stat.errors.push(...result.errors);
      }

      // If no rows to process, still not an error
      if (rows.length === 0) {
        stat.skipped_rows = stat.total_rows;
      }

      // Update sync meta
      const now = new Date().toISOString();
      upsertSyncMeta(local, {
        table_name: table,
        last_synced_at: now,
        last_synced_row_count: stat.synced_rows,
        direction: "push",
      });
    } catch (err: any) {
      stat.errors.push(`Table "${table}": ${err?.message ?? String(err)}`);
    }

    results.push(stat);
  }

  return results;
}

// ---------------------------------------------------------------------------
// Incremental Pull: Remote -> Local
// ---------------------------------------------------------------------------

/**
 * Pull only changed rows (since last sync) from remote to local.
 *
 * - Checks `_sync_meta` in the local DB for `last_synced_at`.
 * - If found: only selects rows where `updated_at > last_synced_at`.
 * - If not found: full pull (first-time sync).
 * - After pull, updates `_sync_meta` with current timestamp and row count.
 */
export function incrementalSyncPull(
  remote: DbAdapter,
  local: DbAdapter,
  tables: string[],
  options: IncrementalSyncOptions = {}
): IncrementalSyncStats[] {
  const { conflictColumn = "updated_at", batchSize = 500 } = options;
  const results: IncrementalSyncStats[] = [];

  ensureSyncMetaTable(local);

  for (const table of tables) {
    const stat: IncrementalSyncStats = {
      table,
      total_rows: 0,
      synced_rows: 0,
      skipped_rows: 0,
      errors: [],
      first_sync: false,
    };

    try {
      // Get total row count from remote for stats
      const countResult = remote.get(`SELECT COUNT(*) as cnt FROM "${table}"`);
      stat.total_rows = countResult?.cnt ?? 0;

      // Check sync meta in local DB
      const meta = getSyncMeta(local, table);
      let rows: any[];

      if (meta?.last_synced_at) {
        // Incremental: only changed rows from remote
        try {
          rows = remote.all(
            `SELECT * FROM "${table}" WHERE "${conflictColumn}" > ?`,
            meta.last_synced_at
          );
        } catch {
          // Column might not exist -- fall back to full pull
          rows = remote.all(`SELECT * FROM "${table}"`);
          stat.first_sync = true;
        }
      } else {
        // First sync -- full pull
        rows = remote.all(`SELECT * FROM "${table}"`);
        stat.first_sync = true;
      }

      // Process in batches
      for (let offset = 0; offset < rows.length; offset += batchSize) {
        const batch = rows.slice(offset, offset + batchSize);
        const result = transferRows(remote, local, table, batch, options);
        stat.synced_rows += result.written;
        stat.skipped_rows += result.skipped;
        stat.errors.push(...result.errors);
      }

      // If no rows to process, still not an error
      if (rows.length === 0) {
        stat.skipped_rows = stat.total_rows;
      }

      // Update sync meta in local DB
      const now = new Date().toISOString();
      upsertSyncMeta(local, {
        table_name: table,
        last_synced_at: now,
        last_synced_row_count: stat.synced_rows,
        direction: "pull",
      });
    } catch (err: any) {
      stat.errors.push(`Table "${table}": ${err?.message ?? String(err)}`);
    }

    results.push(stat);
  }

  return results;
}

/**
 * Get the sync metadata for all tables or a specific table.
 */
export function getSyncMetaAll(db: DbAdapter): SyncMeta[] {
  ensureSyncMetaTable(db);
  return db.all(
    `SELECT table_name, last_synced_at, last_synced_row_count, direction FROM _sync_meta ORDER BY table_name`
  );
}

/**
 * Get sync metadata for a specific table.
 */
export function getSyncMetaForTable(
  db: DbAdapter,
  table: string
): SyncMeta | null {
  return getSyncMeta(db, table);
}

/**
 * Reset sync metadata for a table (forces full re-sync on next run).
 */
export function resetSyncMeta(db: DbAdapter, table: string): void {
  ensureSyncMetaTable(db);
  db.run(`DELETE FROM _sync_meta WHERE table_name = ?`, table);
}

/**
 * Reset all sync metadata (forces full re-sync for all tables).
 */
export function resetAllSyncMeta(db: DbAdapter): void {
  ensureSyncMetaTable(db);
  db.run(`DELETE FROM _sync_meta`);
}
