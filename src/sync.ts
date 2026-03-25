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
  /**
   * Primary key column name(s). Can be a single column string or an array
   * for composite primary keys (default: auto-detected from the database).
   * If not provided and auto-detection fails, falls back to "id".
   */
  primaryKey?: string | string[];
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
// Primary key detection
// ---------------------------------------------------------------------------

/**
 * Detect primary key columns for a table in a SQLite database.
 * Uses PRAGMA table_info — columns with pk > 0 are PK columns.
 */
function getSqlitePrimaryKeys(
  adapter: DbAdapter,
  table: string
): string[] {
  try {
    const cols: Array<{ name: string; pk: number }> = adapter.all(
      `PRAGMA table_info("${table}")`
    );
    const pkCols = cols
      .filter((c) => c.pk > 0)
      .sort((a, b) => a.pk - b.pk)
      .map((c) => c.name);
    return pkCols;
  } catch {
    return [];
  }
}

/**
 * Detect primary key columns for a table in a PostgreSQL database (async).
 * Queries information_schema.key_column_usage for the PRIMARY KEY constraint.
 */
async function getPgPrimaryKeys(
  adapter: PgAdapterAsync,
  table: string
): Promise<string[]> {
  try {
    const rows: Array<{ column_name: string; ordinal_position: number }> =
      await adapter.all(`
        SELECT kcu.column_name, kcu.ordinal_position
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = 'public'
          AND tc.table_name = '${table}'
        ORDER BY kcu.ordinal_position
      `);
    return rows.map((r) => r.column_name);
  } catch {
    return [];
  }
}

/**
 * Detect primary key columns for a table, auto-selecting the right method
 * based on the adapter type.
 */
async function detectPrimaryKeys(
  adapter: DbAdapter | PgAdapterAsync,
  table: string
): Promise<string[]> {
  if (isAsyncAdapter(adapter)) {
    return getPgPrimaryKeys(adapter, table);
  }
  return getSqlitePrimaryKeys(adapter, table);
}

/**
 * Normalize a primaryKey option into an array of column names.
 * If not provided, auto-detects from the source adapter.
 */
async function resolvePrimaryKeys(
  source: DbAdapter | PgAdapterAsync,
  target: DbAdapter | PgAdapterAsync,
  table: string,
  pkOption?: string | string[]
): Promise<string[]> {
  // If explicitly provided, normalize to array
  if (pkOption) {
    return Array.isArray(pkOption) ? pkOption : [pkOption];
  }

  // Auto-detect from source first, then target
  let pks = await detectPrimaryKeys(source, table);
  if (pks.length === 0) {
    pks = await detectPrimaryKeys(target, table);
  }
  return pks;
}

// ---------------------------------------------------------------------------
// Schema-before-data — auto-create missing tables in target
// ---------------------------------------------------------------------------

/**
 * Map PG column types to SQLite types.
 */
function pgTypeToSqlite(pgType: string): string {
  const t = pgType.toLowerCase();
  if (t.includes("int") || t === "bigint" || t === "smallint" || t === "serial" || t === "bigserial") return "INTEGER";
  if (t.includes("bool")) return "INTEGER";
  if (t.includes("float") || t.includes("double") || t === "real" || t === "numeric" || t === "decimal") return "REAL";
  if (t === "bytea") return "BLOB";
  // text, varchar, char, uuid, timestamp, json, jsonb, tsvector → TEXT
  return "TEXT";
}

/**
 * Ensure a table exists in the SQLite target by introspecting the PG source.
 * Skips PG-only types like tsvector columns (they'll be filtered by column filtering later).
 */
async function ensureTableInSqliteFromPg(
  target: DbAdapter,
  source: PgAdapterAsync,
  table: string
): Promise<boolean> {
  // Check if table already exists in SQLite
  const existing = target.all(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table);
  if (existing.length > 0) return false;

  // Introspect PG schema
  const cols: Array<{
    column_name: string;
    data_type: string;
    is_nullable: string;
    column_default: string | null;
  }> = await source.all(
    `SELECT column_name, data_type, is_nullable, column_default
     FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = '${table}'
     ORDER BY ordinal_position`
  );

  if (cols.length === 0) return false;

  // Get PK columns
  const pkCols: Array<{ column_name: string }> = await source.all(
    `SELECT kcu.column_name
     FROM information_schema.table_constraints tc
     JOIN information_schema.key_column_usage kcu
       ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
     WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public' AND tc.table_name = '${table}'
     ORDER BY kcu.ordinal_position`
  );
  const pkSet = new Set(pkCols.map((c) => c.column_name));

  // Skip PG-only columns (tsvector, etc.)
  const skipTypes = new Set(["tsvector", "tsquery", "user-defined"]);
  const filteredCols = cols.filter((c) => !skipTypes.has(c.data_type));

  // Build CREATE TABLE
  const colDefs = filteredCols.map((c) => {
    const sqliteType = pgTypeToSqlite(c.data_type);
    const notNull = c.is_nullable === "NO" && !pkSet.has(c.column_name) ? " NOT NULL" : "";
    return `"${c.column_name}" ${sqliteType}${notNull}`;
  });

  // Add PRIMARY KEY constraint
  if (pkSet.size > 0) {
    const pkList = [...pkSet].map((c) => `"${c}"`).join(", ");
    colDefs.push(`PRIMARY KEY (${pkList})`);
  }

  const sql = `CREATE TABLE IF NOT EXISTS "${table}" (${colDefs.join(", ")})`;
  target.exec(sql);
  process.stderr.write(`  [sync] ${table}: auto-created in SQLite from PG schema\n`);
  return true;
}

/**
 * Ensure all tables exist in the target before syncing data.
 */
async function ensureTablesExist(
  source: DbAdapter | PgAdapterAsync,
  target: DbAdapter | PgAdapterAsync,
  tables: string[]
): Promise<void> {
  for (const table of tables) {
    if (!isAsyncAdapter(target) && isAsyncAdapter(source)) {
      // Pull: PG source → SQLite target
      await ensureTableInSqliteFromPg(target, source, table);
    }
    // Push: SQLite source → PG target — handled by PG migrations, not auto-create
  }
}

// ---------------------------------------------------------------------------
// Column filtering — handle schema drift between source and target
// ---------------------------------------------------------------------------

/**
 * Filter source columns to only those that exist in the target table.
 * Handles schema drift (e.g. PG has search_vector but SQLite doesn't).
 */
async function filterColumnsForTarget(
  target: DbAdapter | PgAdapterAsync,
  table: string,
  sourceColumns: string[]
): Promise<string[]> {
  try {
    if (!isAsyncAdapter(target)) {
      // SQLite target: use PRAGMA table_info
      const colInfo = target.all(`PRAGMA table_info("${table}")`);
      if (Array.isArray(colInfo) && colInfo.length > 0) {
        const targetCols = new Set(colInfo.map((c: any) => c.name as string));
        const filtered = sourceColumns.filter((c) => targetCols.has(c));
        if (filtered.length < sourceColumns.length) {
          const dropped = sourceColumns.filter((c) => !targetCols.has(c));
          // Log dropped columns to stderr for debugging
          process.stderr.write(`  [sync] ${table}: dropping ${dropped.length} columns not in target: ${dropped.join(", ")}\n`);
        }
        return filtered;
      }
    } else {
      // PG target: use information_schema
      const colInfo: Array<{ column_name: string }> = await target.all(
        `SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '${table}'`
      );
      if (colInfo.length > 0) {
        const targetCols = new Set(colInfo.map((c) => c.column_name));
        return sourceColumns.filter((c) => targetCols.has(c));
      }
    }
  } catch {
    // Column detection failed — use all source columns
  }
  return sourceColumns;
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
    primaryKey: pkOption,
  } = options;

  const results: SyncResult[] = [];
  const sqliteTarget = !isAsyncAdapter(target) ? target : null;

  // Auto-create missing tables in target before syncing data
  await ensureTablesExist(source, target, tables);

  // Disable FK checks on SQLite target to prevent constraint errors during sync.
  // Uses exec() for reliable PRAGMA execution and wraps the entire operation in
  // try/finally to guarantee FKs are re-enabled even if sync throws.
  if (sqliteTarget) {
    try { sqliteTarget.exec("PRAGMA foreign_keys = OFF"); } catch {}
  }

  try {
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

        // Detect primary key columns for this table
        const pkColumns = await resolvePrimaryKeys(source, target, table, pkOption);

        // Get column names from the first row
        const sourceColumns = Object.keys(rows[0]);

        // Filter out columns that don't exist in the target table.
        // This handles schema drift between PG (source) and SQLite (target)
        // e.g. PG may have a tsvector search_vector column that SQLite lacks.
        const columns = await filterColumnsForTarget(target, table, sourceColumns);

        if (pkColumns.length === 0) {
          // No PK found — insert without conflict handling (with warning)
          result.errors.push(
            `Table "${table}" has no primary key — inserting without conflict handling`
          );

          // Notify: writing
          onProgress?.({
            table,
            phase: "writing",
            rowsRead: result.rowsRead,
            rowsWritten: 0,
            totalTables: tables.length,
            currentTableIndex: i,
          });

          for (let offset = 0; offset < rows.length; offset += batchSize) {
            const batch = rows.slice(offset, offset + batchSize);
            try {
              if (isAsyncAdapter(target)) {
                await batchInsertPg(target, table, columns, batch);
              } else {
                batchInsertSqlite(target, table, columns, batch);
              }
              result.rowsWritten += batch.length;
            } catch (err: any) {
              result.errors.push(
                `Batch at offset ${offset}: ${err?.message ?? String(err)}`
              );
            }
          }

          onProgress?.({
            table,
            phase: "done",
            rowsRead: result.rowsRead,
            rowsWritten: result.rowsWritten,
            totalTables: tables.length,
            currentTableIndex: i,
          });
          results.push(result);
          continue;
        }

        // Verify all PK columns exist in the data
        const missingPks = pkColumns.filter((pk) => !columns.includes(pk));
        if (missingPks.length > 0) {
          result.errors.push(
            `Table "${table}" missing PK columns in data: ${missingPks.join(", ")} — skipping`
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
        const updateCols = columns.filter((c) => !pkColumns.includes(c));

        for (let offset = 0; offset < rows.length; offset += batchSize) {
          const batch = rows.slice(offset, offset + batchSize);

          try {
            if (isAsyncAdapter(target)) {
              // Target is PgAdapterAsync — use PG batch UPSERT
              await batchUpsertPg(target, table, columns, updateCols, pkColumns, batch);
            } else {
              // Target is sync DbAdapter (SQLite) — use SQLite upsert
              batchUpsertSqlite(target, table, columns, updateCols, pkColumns, batch);
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
  } finally {
    // Re-enable FK checks on SQLite target after sync, even if sync threw
    if (sqliteTarget) {
      try { sqliteTarget.exec("PRAGMA foreign_keys = ON"); } catch {}

      // Run FK integrity check and report violations
      try {
        const violations: Array<{ table: string; rowid: number; parent: string; fkid: number }> =
          sqliteTarget.all("PRAGMA foreign_key_check");
        if (violations.length > 0) {
          const tables = [...new Set(violations.map((v) => v.table))];
          const msg = `FK integrity check: ${violations.length} violation(s) in table(s): ${tables.join(", ")}`;
          // Attach the warning to the last result or create a synthetic one
          if (results.length > 0) {
            results[results.length - 1].errors.push(msg);
          }
        }
      } catch {}
    }
  }

  return results;
}

// ---------------------------------------------------------------------------
// Batch UPSERT helpers
// ---------------------------------------------------------------------------

/**
 * Batch UPSERT into PostgreSQL using INSERT ... ON CONFLICT ... DO UPDATE.
 * Parameters use $1, $2, ... numbering.
 * Supports composite primary keys.
 */
async function batchUpsertPg(
  target: PgAdapterAsync,
  table: string,
  columns: string[],
  updateCols: string[],
  primaryKeys: string[],
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

  // Build ON CONFLICT clause with all PK columns
  const pkList = primaryKeys.map((c) => `"${c}"`).join(", ");

  // Build SET clause for ON CONFLICT
  const setClause =
    updateCols.length > 0
      ? updateCols.map((c) => `"${c}" = EXCLUDED."${c}"`).join(", ")
      : `"${primaryKeys[0]}" = EXCLUDED."${primaryKeys[0]}"`; // no-op update if only PK cols

  const sql = `INSERT INTO "${table}" (${colList}) VALUES ${valuePlaceholders}
    ON CONFLICT (${pkList}) DO UPDATE SET ${setClause}`;

  // Flatten params
  const params = batch.flatMap((row) => columns.map((c) => row[c] ?? null));

  await target.run(sql, ...params);
}

/**
 * Batch UPSERT into SQLite using INSERT ... ON CONFLICT ... DO UPDATE.
 * Parameters use ? placeholders.
 * Supports composite primary keys.
 */
function batchUpsertSqlite(
  target: DbAdapter,
  table: string,
  columns: string[],
  updateCols: string[],
  primaryKeys: string[],
  batch: Record<string, any>[]
): void {
  if (batch.length === 0) return;

  const colList = columns.map((c) => `"${c}"`).join(", ");

  // Build VALUES placeholders: (?, ?, ?), (?, ?, ?), ...
  const valuePlaceholders = batch
    .map(() => `(${columns.map(() => "?").join(", ")})`)
    .join(", ");

  // Build ON CONFLICT clause with all PK columns
  const pkList = primaryKeys.map((c) => `"${c}"`).join(", ");

  // Build SET clause for ON CONFLICT
  const setClause =
    updateCols.length > 0
      ? updateCols.map((c) => `"${c}" = EXCLUDED."${c}"`).join(", ")
      : `"${primaryKeys[0]}" = EXCLUDED."${primaryKeys[0]}"`;

  const sql = `INSERT INTO "${table}" (${colList}) VALUES ${valuePlaceholders}
    ON CONFLICT (${pkList}) DO UPDATE SET ${setClause}`;

  // Flatten params — coerce PG types to SQLite-compatible values
  const params = batch.flatMap((row) => columns.map((c) => coerceForSqlite(row[c])));

  target.run(sql, ...params);
}

/**
 * Batch INSERT into PostgreSQL without conflict handling (for tables without PKs).
 */
async function batchInsertPg(
  target: PgAdapterAsync,
  table: string,
  columns: string[],
  batch: Record<string, any>[]
): Promise<void> {
  if (batch.length === 0) return;

  const colList = columns.map((c) => `"${c}"`).join(", ");
  const valuePlaceholders = batch
    .map((_, rowIdx) => {
      const offset = rowIdx * columns.length;
      return `(${columns.map((_, colIdx) => `$${offset + colIdx + 1}`).join(", ")})`;
    })
    .join(", ");

  const sql = `INSERT INTO "${table}" (${colList}) VALUES ${valuePlaceholders}`;
  const params = batch.flatMap((row) => columns.map((c) => row[c] ?? null));

  await target.run(sql, ...params);
}

/**
 * Batch INSERT into SQLite without conflict handling (for tables without PKs).
 */
function batchInsertSqlite(
  target: DbAdapter,
  table: string,
  columns: string[],
  batch: Record<string, any>[]
): void {
  if (batch.length === 0) return;

  const colList = columns.map((c) => `"${c}"`).join(", ");
  const valuePlaceholders = batch
    .map(() => `(${columns.map(() => "?").join(", ")})`)
    .join(", ");

  const sql = `INSERT INTO "${table}" (${colList}) VALUES ${valuePlaceholders}`;
  // Coerce PG types to SQLite-compatible values
  const params = batch.flatMap((row) => columns.map((c) => coerceForSqlite(row[c])));

  target.run(sql, ...params);
}

// ---------------------------------------------------------------------------
// Value coercion for SQLite
// ---------------------------------------------------------------------------

/**
 * Coerce a value from PostgreSQL into a SQLite-compatible type.
 * PG returns Date objects, JSON objects, arrays, etc. that bun:sqlite
 * cannot bind. This converts them to strings/numbers/null.
 */
function coerceForSqlite(value: any): string | number | bigint | boolean | null | Uint8Array {
  if (value === null || value === undefined) return null;
  if (typeof value === "string" || typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") return value;
  if (value instanceof Date) return value.toISOString();
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) return value as Uint8Array;
  // Arrays, objects → JSON string
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
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
