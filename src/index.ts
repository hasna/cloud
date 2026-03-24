// ---------------------------------------------------------------------------
// @hasna/cloud — Shared cloud infrastructure
// ---------------------------------------------------------------------------

// Database adapters
export {
  SqliteAdapter,
  PgAdapter,
  PgAdapterAsync,
  type DbAdapter,
  type PreparedStatement,
  type RunResult,
} from "./adapter.js";

// SQL dialect translation
export {
  translateSql,
  translateDdl,
  translateParams,
  type Dialect,
} from "./dialect.js";

// Configuration
export {
  getCloudConfig,
  saveCloudConfig,
  getConnectionString,
  createDatabase,
  getConfigDir,
  getConfigPath,
  CloudConfigSchema,
  type CloudConfig,
  type CreateDatabaseOptions,
} from "./config.js";

// Sync engine
export {
  syncPush,
  syncPull,
  listSqliteTables,
  listPgTables,
  type SyncOptions,
  type SyncResult,
  type SyncProgress,
  type SyncProgressCallback,
} from "./sync.js";

// Feedback system
export {
  saveFeedback,
  sendFeedback,
  listFeedback,
  ensureFeedbackTable,
  type Feedback,
} from "./feedback.js";

// Dotfile migration
export {
  migrateDotfile,
  getDataDir,
  getDbPath,
  hasLegacyDotfile,
  getHasnaDir,
} from "./dotfile.js";

// Sync progress tracking & resumability
export {
  SyncProgressTracker,
  type SyncProgressInfo,
  type ProgressCallback,
  type ResumePoint,
} from "./sync-progress.js";

// Sync conflict resolution
export {
  detectConflicts,
  resolveConflicts,
  getWinningData,
  ensureConflictsTable,
  storeConflicts,
  listConflicts,
  resolveConflict,
  getConflict,
  purgeResolvedConflicts,
  type SyncConflict,
  type ConflictStrategy,
  type StoredConflict,
} from "./sync-conflicts.js";

// Incremental sync (change tracking)
export {
  incrementalSyncPush,
  incrementalSyncPull,
  ensureSyncMetaTable,
  getSyncMetaAll,
  getSyncMetaForTable,
  resetSyncMeta,
  resetAllSyncMeta,
  type IncrementalSyncStats,
  type IncrementalSyncOptions,
  type SyncMeta,
} from "./sync-incremental.js";

// Auto-sync on start/stop
export {
  setupAutoSync,
  enableAutoSync,
  getAutoSyncConfig,
  type AutoSyncConfig,
  type AutoSyncContext,
  type AutoSyncResult,
} from "./auto-sync.js";

// Integration helpers (for services to embed cloud features)
export { registerCloudTools } from "./mcp-helpers.js";
export { registerCloudCommands } from "./cli-helpers.js";
