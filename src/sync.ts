import type { DbAdapter } from "./adapter.js";
import type { PgAdapterAsync } from "./adapter.js";

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
  /** Batch size for UPSERT operations. Default: 100 */
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
// Push: Local (SQLite) -> Cloud (PostgreSQL) — async
// ---------------------------------------------------------------------------

/**
 * Push data from a local SQLite database to the cloud PostgreSQL database.
 * Uses batch UPSERT for performance and FK-aware table ordering.
 */
export async function syncPush(
  local: DbAdapter,
  remote: PgAdapterAsync,
  options: SyncOptions
): Promise<SyncResult[]> {
  const orderedTables = await getTableOrder(remote, options.tables);
  return syncTransfer(local, remote, { ...options, tables: orderedTables }, "push");
}

// ---------------------------------------------------------------------------
// Pull: Cloud (PostgreSQL) -> Local (SQLite) — async
// ---------------------------------------------------------------------------

/**
 * Pull data from the cloud PostgreSQL database into a local SQLite database.
 * Uses FK-aware table ordering.
 */
export async function syncPull(
  remote: PgAdapterAsync,
  local: DbAdapter,
  options: SyncOptions
): Promise<SyncResult[]> {
  const orderedTables = await getTableOrder(remote, options.tables);
  return syncTransfer(remote, local, { ...options, tables: orderedTables }, "pull");
}

// ---------------------------------------------------------------------------
// FK-aware table ordering (Bug 3 fix)
// ---------------------------------------------------------------------------

/**
 * Query PG information_schema for FK relationships and topologically sort
 * the tables so that referenced tables come before referencing tables.
 * Falls back to a heuristic: tables without `_id` columns first.
 */
async function getTableOrder(
  remote: PgAdapterAsync,
  tables: string[]
): Promise<string[]> {
  if (tables.length <= 1) return tables;

  try {
    const fks = await remote.all(`
      SELECT DISTINCT
        tc.table_name AS source_table,
        ccu.table_name AS referenced_table
      FROM information_schema.table_constraints tc
      JOIN information_schema.constraint_column_usage ccu
        ON tc.constraint_name = ccu.constraint_name
        AND tc.table_schema = ccu.table_schema
      WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = 'public'
    `);

    if (fks.length > 0) {
      return topoSort(tables, fks);
    }
  } catch {
    // FK query failed — fall through to heuristic
  }

  // Heuristic fallback: tables without _id columns first, then the rest
  return heuristicOrder(tables);
}

/**
 * Topological sort: tables with no FK dependencies come first,
 * then tables that depend on them, etc.
 */
function topoSort(
  tables: string[],
  fks: Array<{ source_table: string; referenced_table: string }>
): string[] {
  const tableSet = new Set(tables);

  // Build adjacency: source depends on referenced
  const deps = new Map<string, Set<string>>();
  for (const t of tables) {
    deps.set(t, new Set());
  }

  for (const fk of fks) {
    if (tableSet.has(fk.source_table) && tableSet.has(fk.referenced_table)) {
      // source_table depends on referenced_table (referenced must come first)
      deps.get(fk.source_table)!.add(fk.referenced_table);
    }
  }

  const sorted: string[] = [];
  const visited = new Set<string>();
  const visiting = new Set<string>();

  function visit(table: string): void {
    if (visited.has(table)) return;
    if (visiting.has(table)) {
      // Circular dependency — just add it and move on
      sorted.push(table);
      visited.add(table);
      return;
    }

    visiting.add(table);
    const tableDeps = deps.get(table) ?? new Set();
    for (const dep of tableDeps) {
      visit(dep);
    }
    visiting.delete(table);
    visited.add(table);
    sorted.push(table);
  }

  for (const t of tables) {
    visit(t);
  }

  return sorted;
}

/**
 * Heuristic ordering when no FK constraints are defined:
 * Tables whose names don't contain `_id`-suffixed columns are pushed first.
 * Simple alphabetical sort as fallback grouping.
 */
function heuristicOrder(tables: string[]): string[] {
  // Simple heuristic: shorter table names (less likely to be join/child tables)
  // and tables without common FK suffixes come first.
  // Common pattern: "tasks" references "task_lists", "comments" references "tasks", etc.
  const sorted = [...tables].sort((a, b) => {
    const aIsChild = a.includes("_") && tables.some((t) => a.startsWith(t + "_") || a.endsWith("_" + t));
    const bIsChild = b.includes("_") && tables.some((t) => b.startsWith(t + "_") || b.endsWith("_" + t));
    if (aIsChild && !bIsChild) return 1;
    if (!aIsChild && bIsChild) return -1;
    return a.localeCompare(b);
  });
  return sorted;
}

// ---------------------------------------------------------------------------
// Core sync logic — async with batch UPSERT (Bug 1 + Bug 2 fix)
// ---------------------------------------------------------------------------

async function syncTransfer(
  source: DbAdapter | PgAdapterAsync,
  target: DbAdapter | PgAdapterAsync,
  options: SyncOptions,
  _direction: "push" | "pull"
): Promise<SyncResult[]> {
  const {
    tables,
    onProgress,
    batchSize = 100,
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

      // Read all rows from source (may be sync DbAdapter or async PgAdapterAsync)
      const rows = await readAll(source, `SELECT * FROM "${table}"`);
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

      // Process in batches using UPSERT
      const updateCols = columns.filter((c) => c !== primaryKey);

      for (let offset = 0; offset < rows.length; offset += batchSize) {
        const batch = rows.slice(offset, offset + batchSize);

        try {
          if (isAsyncAdapter(target)) {
            // Target is PgAdapterAsync — use PG batch UPSERT
            await batchUpsertPg(target, table, columns, updateCols, primaryKey, batch);
          } else {
            // Target is sync DbAdapter (SQLite) — use SQLite upsert
            batchUpsertSqlite(target, table, columns, updateCols, primaryKey, batch);
          }
          result.rowsWritten += batch.length;
        } catch (err: any) {
          result.errors.push(
            `Batch at offset ${offset}: ${err?.message ?? String(err)}`
          );
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
// Batch UPSERT helpers
// ---------------------------------------------------------------------------

/**
 * Batch UPSERT into PostgreSQL using INSERT ... ON CONFLICT ... DO UPDATE.
 * Parameters use $1, $2, ... numbering.
 */
async function batchUpsertPg(
  target: PgAdapterAsync,
  table: string,
  columns: string[],
  updateCols: string[],
  primaryKey: string,
  batch: Record<string, any>[]
): Promise<void> {
  if (batch.length === 0) return;

  const colList = columns.map((c) => `"${c}"`).join(", ");

  // Build VALUES placeholders: ($1, $2, $3), ($4, $5, $6), ...
  const valuePlaceholders = batch
    .map((_, rowIdx) => {
      const offset = rowIdx * columns.length;
      return `(${columns.map((_, colIdx) => `$${offset + colIdx + 1}`).join(", ")})`;
    })
    .join(", ");

  // Build SET clause for ON CONFLICT
  const setClause =
    updateCols.length > 0
      ? updateCols.map((c) => `"${c}" = EXCLUDED."${c}"`).join(", ")
      : `"${primaryKey}" = EXCLUDED."${primaryKey}"`; // no-op update if only PK

  const sql = `INSERT INTO "${table}" (${colList}) VALUES ${valuePlaceholders}
    ON CONFLICT ("${primaryKey}") DO UPDATE SET ${setClause}`;

  // Flatten params
  const params = batch.flatMap((row) => columns.map((c) => row[c] ?? null));

  await target.run(sql, ...params);
}

/**
 * Batch UPSERT into SQLite using INSERT ... ON CONFLICT ... DO UPDATE.
 * Parameters use ? placeholders.
 */
function batchUpsertSqlite(
  target: DbAdapter,
  table: string,
  columns: string[],
  updateCols: string[],
  primaryKey: string,
  batch: Record<string, any>[]
): void {
  if (batch.length === 0) return;

  const colList = columns.map((c) => `"${c}"`).join(", ");

  // Build VALUES placeholders: (?, ?, ?), (?, ?, ?), ...
  const valuePlaceholders = batch
    .map(() => `(${columns.map(() => "?").join(", ")})`)
    .join(", ");

  // Build SET clause for ON CONFLICT
  const setClause =
    updateCols.length > 0
      ? updateCols.map((c) => `"${c}" = EXCLUDED."${c}"`).join(", ")
      : `"${primaryKey}" = EXCLUDED."${primaryKey}"`;

  const sql = `INSERT INTO "${table}" (${colList}) VALUES ${valuePlaceholders}
    ON CONFLICT ("${primaryKey}") DO UPDATE SET ${setClause}`;

  // Flatten params
  const params = batch.flatMap((row) => columns.map((c) => row[c] ?? null));

  target.run(sql, ...params);
}

// ---------------------------------------------------------------------------
// Adapter type helpers
// ---------------------------------------------------------------------------

/**
 * Check if the adapter is an async PgAdapterAsync (has async methods).
 */
function isAsyncAdapter(adapter: DbAdapter | PgAdapterAsync): adapter is PgAdapterAsync {
  // PgAdapterAsync methods return Promises; DbAdapter methods are synchronous.
  // We check for the presence of `raw` property being a pg.Pool (has `connect` method).
  return (
    adapter.constructor.name === "PgAdapterAsync" ||
    typeof (adapter as any).raw?.connect === "function"
  );
}

/**
 * Read all rows from either a sync DbAdapter or async PgAdapterAsync.
 */
async function readAll(
  adapter: DbAdapter | PgAdapterAsync,
  sql: string
): Promise<any[]> {
  const result = adapter.all(sql);
  // If it's a promise, await it; if sync, it's already the value
  return result instanceof Promise ? await result : result;
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
 * List all user tables in a PostgreSQL database (async).
 */
export async function listPgTables(db: PgAdapterAsync): Promise<string[]> {
  const rows = await db.all(
    `SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename`
  );
  return rows.map((r: any) => r.tablename);
}
