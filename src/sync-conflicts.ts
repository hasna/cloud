import type { DbAdapter } from "./adapter.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface SyncConflict {
  table: string;
  row_id: string;
  local_updated_at: string;
  remote_updated_at: string;
  local_data: Record<string, any>;
  remote_data: Record<string, any>;
  resolved: boolean;
  resolution?: "local-wins" | "remote-wins" | "newest-wins" | "manual";
}

export type ConflictStrategy = "local-wins" | "remote-wins" | "newest-wins";

export interface StoredConflict {
  id: string;
  table_name: string;
  row_id: string;
  local_data: string;
  remote_data: string;
  local_updated_at: string;
  remote_updated_at: string;
  resolution: string | null;
  resolved_at: string | null;
  created_at: string;
}

// ---------------------------------------------------------------------------
// Conflict detection
// ---------------------------------------------------------------------------

/**
 * Find rows that exist in BOTH local and remote datasets with DIFFERENT
 * `updated_at` values. Returns a list of SyncConflict objects.
 *
 * @param local - Array of rows from the local database
 * @param remote - Array of rows from the remote database
 * @param table - The table name these rows belong to
 * @param primaryKey - Column used as primary key (default: "id")
 * @param conflictColumn - Column used for timestamp comparison (default: "updated_at")
 */
export function detectConflicts(
  local: Record<string, any>[],
  remote: Record<string, any>[],
  table: string,
  primaryKey = "id",
  conflictColumn = "updated_at"
): SyncConflict[] {
  const conflicts: SyncConflict[] = [];

  // Index remote rows by primary key for O(1) lookup
  const remoteMap = new Map<string, Record<string, any>>();
  for (const row of remote) {
    const key = String(row[primaryKey]);
    remoteMap.set(key, row);
  }

  for (const localRow of local) {
    const key = String(localRow[primaryKey]);
    const remoteRow = remoteMap.get(key);

    if (!remoteRow) continue; // exists only locally — not a conflict

    const localTs = localRow[conflictColumn];
    const remoteTs = remoteRow[conflictColumn];

    // Both exist — check if timestamps differ
    if (localTs !== remoteTs) {
      conflicts.push({
        table,
        row_id: key,
        local_updated_at: String(localTs ?? ""),
        remote_updated_at: String(remoteTs ?? ""),
        local_data: { ...localRow },
        remote_data: { ...remoteRow },
        resolved: false,
      });
    }
  }

  return conflicts;
}

// ---------------------------------------------------------------------------
// Conflict resolution
// ---------------------------------------------------------------------------

/**
 * Resolve a list of conflicts using the given strategy.
 * Returns the winning row data for each conflict.
 *
 * @param conflicts - The conflicts to resolve
 * @param strategy - Resolution strategy (default: "newest-wins")
 * @returns Array of resolved conflicts with `resolved: true` and `resolution` set
 */
export function resolveConflicts(
  conflicts: SyncConflict[],
  strategy: ConflictStrategy = "newest-wins"
): SyncConflict[] {
  return conflicts.map((conflict) => {
    const resolved = { ...conflict, resolved: true, resolution: strategy as SyncConflict["resolution"] };

    switch (strategy) {
      case "local-wins":
        // Keep local_data — no changes needed to the data itself
        break;

      case "remote-wins":
        // Keep remote_data — no changes needed to the data itself
        break;

      case "newest-wins": {
        const localTime = new Date(conflict.local_updated_at).getTime();
        const remoteTime = new Date(conflict.remote_updated_at).getTime();

        if (remoteTime > localTime) {
          resolved.resolution = "newest-wins";
        } else {
          resolved.resolution = "newest-wins";
        }
        break;
      }
    }

    return resolved;
  });
}

/**
 * Get the winning data for a resolved conflict.
 */
export function getWinningData(conflict: SyncConflict): Record<string, any> {
  if (!conflict.resolved || !conflict.resolution) {
    throw new Error(`Conflict for row ${conflict.row_id} is not resolved`);
  }

  switch (conflict.resolution) {
    case "local-wins":
      return conflict.local_data;

    case "remote-wins":
      return conflict.remote_data;

    case "newest-wins": {
      const localTime = new Date(conflict.local_updated_at).getTime();
      const remoteTime = new Date(conflict.remote_updated_at).getTime();
      return remoteTime >= localTime ? conflict.remote_data : conflict.local_data;
    }

    case "manual":
      // Manual resolution should have been applied externally
      return conflict.local_data;

    default:
      return conflict.local_data;
  }
}

// ---------------------------------------------------------------------------
// Conflict storage — persists unresolved conflicts to _sync_conflicts table
// ---------------------------------------------------------------------------

/**
 * Ensure the _sync_conflicts table exists.
 */
export function ensureConflictsTable(db: DbAdapter): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS _sync_conflicts (
      id TEXT PRIMARY KEY,
      table_name TEXT,
      row_id TEXT,
      local_data TEXT,
      remote_data TEXT,
      local_updated_at TEXT,
      remote_updated_at TEXT,
      resolution TEXT,
      resolved_at TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )
  `);
}

/**
 * Store unresolved conflicts in the database for later review.
 */
export function storeConflicts(db: DbAdapter, conflicts: SyncConflict[]): void {
  ensureConflictsTable(db);

  for (const conflict of conflicts) {
    const id = `${conflict.table}:${conflict.row_id}:${Date.now()}`;
    db.run(
      `INSERT INTO _sync_conflicts (id, table_name, row_id, local_data, remote_data, local_updated_at, remote_updated_at, resolution, resolved_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      id,
      conflict.table,
      conflict.row_id,
      JSON.stringify(conflict.local_data),
      JSON.stringify(conflict.remote_data),
      conflict.local_updated_at,
      conflict.remote_updated_at,
      conflict.resolution ?? null,
      conflict.resolved ? new Date().toISOString() : null
    );
  }
}

// ---------------------------------------------------------------------------
// CLI helpers — list and resolve stored conflicts
// ---------------------------------------------------------------------------

/**
 * List all stored conflicts, optionally filtered by resolved status.
 */
export function listConflicts(
  db: DbAdapter,
  opts?: { resolved?: boolean; table?: string }
): StoredConflict[] {
  ensureConflictsTable(db);

  let sql = `SELECT * FROM _sync_conflicts WHERE 1=1`;
  const params: any[] = [];

  if (opts?.resolved !== undefined) {
    if (opts.resolved) {
      sql += ` AND resolution IS NOT NULL AND resolved_at IS NOT NULL`;
    } else {
      sql += ` AND (resolution IS NULL OR resolved_at IS NULL)`;
    }
  }

  if (opts?.table) {
    sql += ` AND table_name = ?`;
    params.push(opts.table);
  }

  sql += ` ORDER BY created_at DESC`;

  return db.all(sql, ...params) as StoredConflict[];
}

/**
 * Resolve a stored conflict by ID using the given strategy.
 */
export function resolveConflict(
  db: DbAdapter,
  conflictId: string,
  strategy: ConflictStrategy | "manual"
): StoredConflict | null {
  ensureConflictsTable(db);

  const row = db.get(
    `SELECT * FROM _sync_conflicts WHERE id = ?`,
    conflictId
  ) as StoredConflict | null;

  if (!row) return null;

  db.run(
    `UPDATE _sync_conflicts SET resolution = ?, resolved_at = datetime('now') WHERE id = ?`,
    strategy,
    conflictId
  );

  return db.get(
    `SELECT * FROM _sync_conflicts WHERE id = ?`,
    conflictId
  ) as StoredConflict;
}

/**
 * Get a single stored conflict by ID.
 */
export function getConflict(db: DbAdapter, conflictId: string): StoredConflict | null {
  ensureConflictsTable(db);
  return db.get(
    `SELECT * FROM _sync_conflicts WHERE id = ?`,
    conflictId
  ) as StoredConflict | null;
}

/**
 * Delete all resolved conflicts (cleanup).
 */
export function purgeResolvedConflicts(db: DbAdapter): number {
  ensureConflictsTable(db);
  const result = db.run(
    `DELETE FROM _sync_conflicts WHERE resolution IS NOT NULL AND resolved_at IS NOT NULL`
  );
  return result.changes;
}
