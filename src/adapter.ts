import { Database } from "bun:sqlite";
import pg from "pg";
import { translateParams, translateSql } from "./dialect.js";

// ---------------------------------------------------------------------------
// Interfaces
// ---------------------------------------------------------------------------

export interface RunResult {
  changes: number;
  lastInsertRowid: number | bigint;
}

export interface PreparedStatement {
  run(...params: any[]): RunResult;
  get(...params: any[]): any;
  all(...params: any[]): any[];
  finalize(): void;
}

export interface DbAdapter {
  run(sql: string, ...params: any[]): RunResult;
  get(sql: string, ...params: any[]): any;
  all(sql: string, ...params: any[]): any[];
  exec(sql: string): void;
  prepare(sql: string): PreparedStatement;
  close(): void;
  transaction<T>(fn: () => T): T;
}

// ---------------------------------------------------------------------------
// SQLite Adapter — thin passthrough over bun:sqlite
// ---------------------------------------------------------------------------

export class SqliteAdapter implements DbAdapter {
  private db: Database;

  constructor(path: string) {
    this.db = new Database(path, { create: true });
    this.db.exec("PRAGMA journal_mode=WAL");
    this.db.exec("PRAGMA foreign_keys=ON");
  }

  run(sql: string, ...params: any[]): RunResult {
    const stmt = this.db.prepare(sql);
    const result = stmt.run(...params);
    return {
      changes: result.changes,
      lastInsertRowid: result.lastInsertRowid,
    };
  }

  get(sql: string, ...params: any[]): any {
    const stmt = this.db.prepare(sql);
    return stmt.get(...params);
  }

  all(sql: string, ...params: any[]): any[] {
    const stmt = this.db.prepare(sql);
    return stmt.all(...params);
  }

  exec(sql: string): void {
    this.db.exec(sql);
  }

  prepare(sql: string): PreparedStatement {
    const stmt = this.db.prepare(sql);
    return {
      run(...params: any[]): RunResult {
        const r = stmt.run(...params);
        return { changes: r.changes, lastInsertRowid: r.lastInsertRowid };
      },
      get(...params: any[]): any {
        return stmt.get(...params);
      },
      all(...params: any[]): any[] {
        return stmt.all(...params);
      },
      finalize(): void {
        stmt.finalize();
      },
    };
  }

  close(): void {
    this.db.close();
  }

  transaction<T>(fn: () => T): T {
    const wrapped = this.db.transaction(fn);
    return wrapped();
  }

  /** Expose the underlying bun:sqlite Database for advanced usage. */
  get raw(): Database {
    return this.db;
  }
}

// ---------------------------------------------------------------------------
// PostgreSQL Adapter — maps bun:sqlite-style API onto pg Pool
// ---------------------------------------------------------------------------

export class PgAdapter implements DbAdapter {
  private pool: pg.Pool;
  private _client: pg.PoolClient | null = null;

  constructor(connectionString: string);
  constructor(pool: pg.Pool);
  constructor(arg: string | pg.Pool) {
    if (typeof arg === "string") {
      this.pool = new pg.Pool({ connectionString: arg });
    } else {
      this.pool = arg;
    }
  }

  // Synchronous-style helpers that block using Bun's built-in async→sync bridge
  // In Bun, top-level await and blocking on promises is supported, but for the
  // sync API we use a workaround: each call acquires a client, runs, releases.
  //
  // IMPORTANT: All public methods are synchronous to match the DbAdapter
  // interface (which mirrors bun:sqlite). We achieve this by using
  // Bun's ability to run async operations synchronously via a helper.

  private runSync<T>(fn: () => Promise<T>): T {
    // Bun supports top-level await natively. For sync contexts we
    // rely on the fact that pg operations are fast enough that we can
    // use a simple async wrapper. In practice, services should prefer
    // the async variants where possible — this sync wrapper exists
    // purely for API compatibility with bun:sqlite.
    //
    // We use a blocking approach via Bun's internal scheduler.
    let result: T | undefined;
    let error: Error | undefined;
    let done = false;

    fn()
      .then((r) => {
        result = r;
        done = true;
      })
      .catch((e) => {
        error = e;
        done = true;
      });

    // Spin-wait is not ideal but necessary for sync API compat.
    // In practice, PgAdapter should be used in async contexts.
    const deadline = Date.now() + 30_000;
    while (!done && Date.now() < deadline) {
      Bun.sleepSync(1);
    }

    if (error) throw error;
    if (!done) throw new Error("PgAdapter: query timed out (30s)");
    return result as T;
  }

  run(sql: string, ...params: any[]): RunResult {
    const pgSql = translateSql(sql, "pg");
    const pgParams = translateParams(params);
    return this.runSync(async () => {
      const res = await this.pool.query(pgSql, pgParams);
      return {
        changes: res.rowCount ?? 0,
        lastInsertRowid: res.rows?.[0]?.id ?? 0,
      };
    });
  }

  get(sql: string, ...params: any[]): any {
    const pgSql = translateSql(sql, "pg");
    const pgParams = translateParams(params);
    return this.runSync(async () => {
      const res = await this.pool.query(pgSql, pgParams);
      return res.rows[0] ?? null;
    });
  }

  all(sql: string, ...params: any[]): any[] {
    const pgSql = translateSql(sql, "pg");
    const pgParams = translateParams(params);
    return this.runSync(async () => {
      const res = await this.pool.query(pgSql, pgParams);
      return res.rows;
    });
  }

  exec(sql: string): void {
    const pgSql = translateSql(sql, "pg");
    this.runSync(async () => {
      await this.pool.query(pgSql);
    });
  }

  prepare(sql: string): PreparedStatement {
    const pgSql = translateSql(sql, "pg");
    const adapter = this;
    return {
      run(...params: any[]): RunResult {
        const pgParams = translateParams(params);
        return adapter.runSync(async () => {
          const res = await adapter.pool.query(pgSql, pgParams);
          return {
            changes: res.rowCount ?? 0,
            lastInsertRowid: res.rows?.[0]?.id ?? 0,
          };
        });
      },
      get(...params: any[]): any {
        const pgParams = translateParams(params);
        return adapter.runSync(async () => {
          const res = await adapter.pool.query(pgSql, pgParams);
          return res.rows[0] ?? null;
        });
      },
      all(...params: any[]): any[] {
        const pgParams = translateParams(params);
        return adapter.runSync(async () => {
          const res = await adapter.pool.query(pgSql, pgParams);
          return res.rows;
        });
      },
      finalize(): void {
        // No-op for PG prepared statements via pool
      },
    };
  }

  close(): void {
    this.runSync(async () => {
      await this.pool.end();
    });
  }

  transaction<T>(fn: () => T): T {
    return this.runSync(async () => {
      const client = await this.pool.connect();
      try {
        await client.query("BEGIN");
        // Temporarily swap pool query to use this client
        const origQuery = this.pool.query.bind(this.pool);
        (this.pool as any).query = client.query.bind(client);
        let result: T;
        try {
          result = fn();
        } finally {
          (this.pool as any).query = origQuery;
        }
        await client.query("COMMIT");
        return result;
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      } finally {
        client.release();
      }
    });
  }

  /** Expose the underlying pg Pool for advanced usage. */
  get raw(): pg.Pool {
    return this.pool;
  }
}

// ---------------------------------------------------------------------------
// Async PostgreSQL Adapter — native async API (recommended for PG usage)
// ---------------------------------------------------------------------------

export class PgAdapterAsync {
  private pool: pg.Pool;

  constructor(connectionString: string);
  constructor(pool: pg.Pool);
  constructor(arg: string | pg.Pool) {
    if (typeof arg === "string") {
      this.pool = new pg.Pool({ connectionString: arg });
    } else {
      this.pool = arg;
    }
  }

  async run(sql: string, ...params: any[]): Promise<RunResult> {
    const pgSql = translateSql(sql, "pg");
    const pgParams = translateParams(params);
    const res = await this.pool.query(pgSql, pgParams);
    return {
      changes: res.rowCount ?? 0,
      lastInsertRowid: res.rows?.[0]?.id ?? 0,
    };
  }

  async get(sql: string, ...params: any[]): Promise<any> {
    const pgSql = translateSql(sql, "pg");
    const pgParams = translateParams(params);
    const res = await this.pool.query(pgSql, pgParams);
    return res.rows[0] ?? null;
  }

  async all(sql: string, ...params: any[]): Promise<any[]> {
    const pgSql = translateSql(sql, "pg");
    const pgParams = translateParams(params);
    const res = await this.pool.query(pgSql, pgParams);
    return res.rows;
  }

  async exec(sql: string): Promise<void> {
    const pgSql = translateSql(sql, "pg");
    await this.pool.query(pgSql);
  }

  async close(): Promise<void> {
    await this.pool.end();
  }

  async transaction<T>(fn: (client: pg.PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await fn(client);
      await client.query("COMMIT");
      return result;
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  get raw(): pg.Pool {
    return this.pool;
  }
}
