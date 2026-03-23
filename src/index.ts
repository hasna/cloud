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

// Integration helpers (for services to embed cloud features)
export { registerCloudTools } from "./mcp-helpers.js";
export { registerCloudCommands } from "./cli-helpers.js";
