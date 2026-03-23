import type { DbAdapter } from "./adapter.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface SyncProgress {
  table: string;
  phase: "reading" | "writing" | "done";
  rowsRead: number;
  rowsWritten: number;
  totalTables: number;
  currentTableIndex: number;
}

export type SyncProgressCallback = (progress: SyncProgress) => void;

export interface SyncOptions {
  /** Tables to sync. */
  tables: string[];
  /** Optional progress callback. */
  onProgress?: SyncProgressCallback;
  /** Batch size for UPSERT operations. Default: 500 */
  batchSize?: number;
  /** Conflict resolution column (default: "updated_at"). Newest wins. */
  conflictColumn?: string;
  /** Primary key column name (default: "id"). */
  primaryKey?: string;
}

export interface SyncResult {
  table: string;
  rowsRead: number;
  rowsWritten: number;
  rowsSkipped: number;
  errors: string[];
}

// ---------------------------------------------------------------------------
// Push: Local (SQLite) → Cloud (PostgreSQL)
// ---------------------------------------------------------------------------

/**
 * Push data from a local database to the cloud database.
 * For each table: SELECT * from source → UPSERT into target.
 * Conflict resolution: compare `updated_at`, newest wins.
 */
export function syncPush(
  local: DbAdapter,
  cloud: DbAdapter,
  options: SyncOptions
): SyncResult[] {
  return syncTransfer(local, cloud, options, "push");
}

// ---------------------------------------------------------------------------
// Pull: Cloud (PostgreSQL) → Local (SQLite)
// ---------------------------------------------------------------------------

/**
 * Pull data from the cloud database into the local database.
 */
export function syncPull(
  local: DbAdapter,
  cloud: DbAdapter,
  options: SyncOptions
): SyncResult[] {
  return syncTransfer(cloud, local, options, "pull");
}

// ---------------------------------------------------------------------------
// Core sync logic
// ---------------------------------------------------------------------------

function syncTransfer(
  source: DbAdapter,
  target: DbAdapter,
  options: SyncOptions,
  _direction: "push" | "pull"
): SyncResult[] {
  const {
    tables,
    onProgress,
    batchSize = 500,
    conflictColumn = "updated_at",
    primaryKey = "id",
  } = options;

  const results: SyncResult[] = [];

  for (let i = 0; i < tables.length; i++) {
    const table = tables[i];
    const result: SyncResult = {
      table,
      rowsRead: 0,
      rowsWritten: 0,
      rowsSkipped: 0,
      errors: [],
    };

    try {
      // Notify: reading
      onProgress?.({
        table,
        phase: "reading",
        rowsRead: 0,
        rowsWritten: 0,
        totalTables: tables.length,
        currentTableIndex: i,
      });

      // Read all rows from source
      const rows = source.all(`SELECT * FROM "${table}"`);
      result.rowsRead = rows.length;

      if (rows.length === 0) {
        onProgress?.({
          table,
          phase: "done",
          rowsRead: 0,
          rowsWritten: 0,
          totalTables: tables.length,
          currentTableIndex: i,
        });
        results.push(result);
        continue;
      }

      // Get column names from the first row
      const columns = Object.keys(rows[0]);
      const hasConflictCol = columns.includes(conflictColumn);
      const hasPrimaryKey = columns.includes(primaryKey);

      if (!hasPrimaryKey) {
        result.errors.push(
          `Table "${table}" has no "${primaryKey}" column — skipping`
        );
        results.push(result);
        continue;
      }

      // Notify: writing
      onProgress?.({
        table,
        phase: "writing",
        rowsRead: result.rowsRead,
        rowsWritten: 0,
        totalTables: tables.length,
        currentTableIndex: i,
      });

      // Process in batches
      for (let offset = 0; offset < rows.length; offset += batchSize) {
        const batch = rows.slice(offset, offset + batchSize);

        for (const row of batch) {
          try {
            // Check if row exists in target
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
                const existingTime = new Date(
                  existing[conflictColumn]
                ).getTime();
                const incomingTime = new Date(row[conflictColumn]).getTime();
                if (existingTime >= incomingTime) {
                  result.rowsSkipped++;
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

            result.rowsWritten++;
          } catch (err: any) {
            result.errors.push(
              `Row ${row[primaryKey]}: ${err?.message ?? String(err)}`
            );
          }
        }

        // Progress update after each batch
        onProgress?.({
          table,
          phase: "writing",
          rowsRead: result.rowsRead,
          rowsWritten: result.rowsWritten,
          totalTables: tables.length,
          currentTableIndex: i,
        });
      }

      // Done with this table
      onProgress?.({
        table,
        phase: "done",
        rowsRead: result.rowsRead,
        rowsWritten: result.rowsWritten,
        totalTables: tables.length,
        currentTableIndex: i,
      });
    } catch (err: any) {
      result.errors.push(`Table "${table}": ${err?.message ?? String(err)}`);
    }

    results.push(result);
  }

  return results;
}

// ---------------------------------------------------------------------------
// Table discovery helpers
// ---------------------------------------------------------------------------

/**
 * List all user tables in a SQLite database.
 */
export function listSqliteTables(db: DbAdapter): string[] {
  const rows = db.all(
    `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
  );
  return rows.map((r: any) => r.name);
}

/**
 * List all user tables in a PostgreSQL database.
 */
export function listPgTables(db: DbAdapter): string[] {
  const rows = db.all(
    `SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename`
  );
  return rows.map((r: any) => r.tablename);
}
