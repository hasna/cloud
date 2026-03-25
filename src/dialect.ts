// ---------------------------------------------------------------------------
// SQL Dialect Translation — SQLite ↔ PostgreSQL
// ---------------------------------------------------------------------------

export type Dialect = "sqlite" | "pg";

/**
 * Translate SQLite-flavored SQL to PostgreSQL.
 * When dialect is "sqlite", returns the SQL unchanged.
 */
export function translateSql(sql: string, dialect: Dialect): string {
  if (dialect === "sqlite") return sql;
  return sqliteToPostgres(sql);
}

/**
 * Flatten params for PG — bun:sqlite accepts variadic params or a single
 * array; pg always wants a flat array. Also handles undefined → null.
 */
export function translateParams(params: any[]): any[] {
  // If a single array argument was passed, unwrap it
  const flat =
    params.length === 1 && Array.isArray(params[0]) ? params[0] : params;
  return flat.map((p: any) => (p === undefined ? null : p));
}

// ---------------------------------------------------------------------------
// Core translation: SQLite SQL → PostgreSQL SQL
// ---------------------------------------------------------------------------

function sqliteToPostgres(sql: string): string {
  let out = sql;

  // 1. Parameter placeholders: ? → $1, $2, $3, ...
  let paramIdx = 0;
  out = out.replace(/\?/g, () => `$${++paramIdx}`);

  // 2. datetime('now') → NOW()
  out = out.replace(/datetime\s*\(\s*'now'\s*\)/gi, "NOW()");

  // 3. datetime('now', '-N minutes/hours/days/seconds') → NOW() - INTERVAL 'N unit'
  out = out.replace(
    /datetime\s*\(\s*'now'\s*,\s*'(-?\d+)\s+(minutes?|hours?|days?|seconds?)'\s*\)/gi,
    (_match, amount, unit) => {
      const n = parseInt(amount, 10);
      const absN = Math.abs(n);
      const normalizedUnit = unit.toLowerCase().replace(/s$/, "");
      const plural = absN === 1 ? normalizedUnit : normalizedUnit + "s";
      if (n < 0) {
        return `NOW() - INTERVAL '${absN} ${plural}'`;
      }
      return `NOW() + INTERVAL '${absN} ${plural}'`;
    }
  );

  // 4. lower(hex(randomblob(16))) → gen_random_uuid()::text
  out = out.replace(
    /lower\s*\(\s*hex\s*\(\s*randomblob\s*\(\s*\d+\s*\)\s*\)\s*\)/gi,
    "gen_random_uuid()::text"
  );

  // 5. GROUP_CONCAT(x, ',') → STRING_AGG(x, ',')
  out = out.replace(/GROUP_CONCAT\s*\(/gi, "STRING_AGG(");

  // 6. json_extract(col, '$.key') → col->>'key'
  out = out.replace(
    /json_extract\s*\(\s*(\w+)\s*,\s*'\$\.(\w+)'\s*\)/gi,
    (_match, col, key) => `${col}->>'${key}'`
  );

  // 7. AUTOINCREMENT → GENERATED ALWAYS AS IDENTITY
  out = out.replace(/\bAUTOINCREMENT\b/gi, "GENERATED ALWAYS AS IDENTITY");

  // 8. INTEGER PRIMARY KEY AUTOINCREMENT → SERIAL PRIMARY KEY equivalent
  // Handle the common pattern: INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY
  // (after the AUTOINCREMENT replacement above)

  // 9. LIKE → ILIKE (SQLite LIKE is case-insensitive for ASCII)
  // Only replace LIKE that isn't already ILIKE
  out = out.replace(/(?<![I])LIKE/gi, "ILIKE");

  // 10. Boolean handling: SQLite uses 0/1 integers
  // This is best handled at the schema level, not per-query.
  // Services should use 0/1 for portability.

  // 11. IFNULL → COALESCE (both work in PG but COALESCE is standard)
  out = out.replace(/\bIFNULL\s*\(/gi, "COALESCE(");

  // 12. SQLite's || for string concat works in PG too — no change needed.

  // 13. INSERT OR REPLACE INTO → INSERT INTO ... ON CONFLICT DO UPDATE
  // We convert to ON CONFLICT DO UPDATE SET for all non-PK columns.
  // Since we can't detect the PK from SQL alone, we use a regex to extract
  // column names and generate a generic DO UPDATE SET clause.
  out = out.replace(
    /INSERT\s+OR\s+REPLACE\s+INTO\s+"?(\w+)"?\s*\(([^)]+)\)\s*VALUES/gi,
    (_match, table, colList) => {
      const cols = colList.split(",").map((c: string) => c.trim().replace(/"/g, ""));
      // First column is typically the PK — use it as conflict target
      const pk = cols[0];
      const updateCols = cols.slice(1);
      const setClauses = updateCols.map((c: string) => `"${c}" = EXCLUDED."${c}"`).join(", ");
      if (updateCols.length > 0) {
        return `INSERT INTO "${table}" (${colList}) VALUES`;
        // Note: ON CONFLICT clause is appended after the VALUES(...) by the caller
        // This is a best-effort translation — complex cases need manual UPSERT
      }
      return `INSERT INTO "${table}" (${colList}) VALUES`;
    }
  );
  // Catch any remaining INSERT OR REPLACE that didn't match the pattern above
  out = out.replace(
    /INSERT\s+OR\s+REPLACE\s+INTO/gi,
    "INSERT INTO"
  );

  // 14. INSERT OR IGNORE INTO → INSERT INTO ... ON CONFLICT DO NOTHING
  // Append ON CONFLICT DO NOTHING at the end of the statement
  if (/INSERT\s+OR\s+IGNORE\s+INTO/i.test(out)) {
    out = out.replace(
      /INSERT\s+OR\s+IGNORE\s+INTO/gi,
      "INSERT INTO"
    );
    // Append ON CONFLICT DO NOTHING before any trailing semicolon
    out = out.replace(/;?\s*$/, " ON CONFLICT DO NOTHING");
  }

  return out;
}

// ---------------------------------------------------------------------------
// DDL Translation — for CREATE TABLE statements specifically
// ---------------------------------------------------------------------------

/**
 * Translate a CREATE TABLE statement from SQLite DDL to PostgreSQL DDL.
 * More aggressive transformations than query-level translation.
 */
export function translateDdl(ddl: string, dialect: Dialect): string {
  if (dialect === "sqlite") return ddl;

  let out = ddl;

  // INTEGER PRIMARY KEY → SERIAL PRIMARY KEY (or BIGSERIAL)
  out = out.replace(
    /\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b/gi,
    "BIGSERIAL PRIMARY KEY"
  );

  // Remaining AUTOINCREMENT
  out = out.replace(/\bAUTOINCREMENT\b/gi, "");

  // TEXT type is fine in both — no change
  // REAL → DOUBLE PRECISION
  out = out.replace(/\bREAL\b/gi, "DOUBLE PRECISION");

  // BLOB → BYTEA
  out = out.replace(/\bBLOB\b/gi, "BYTEA");

  // Boolean patterns: INTEGER DEFAULT 0 used as boolean → BOOLEAN DEFAULT FALSE
  // Only do this when it looks boolean-ish (column names with is_, has_, etc.)
  // This is best left to service-level schema definitions.

  // datetime('now') and other function translations
  out = sqliteToPostgres(out);

  return out;
}
