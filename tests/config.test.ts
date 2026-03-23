import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import {
  getCloudConfig,
  saveCloudConfig,
  getConfigPath,
  type CloudConfig,
} from "../src/config";
import { existsSync, unlinkSync, mkdirSync } from "fs";
import { dirname } from "path";

describe("config", () => {
  const configPath = getConfigPath();
  let hadExistingConfig = false;
  let existingContent: string | null = null;

  beforeEach(() => {
    // Preserve existing config if any
    if (existsSync(configPath)) {
      hadExistingConfig = true;
      const { readFileSync } = require("fs");
      existingContent = readFileSync(configPath, "utf-8");
    }
  });

  afterEach(() => {
    // Restore or clean up
    if (hadExistingConfig && existingContent !== null) {
      const { writeFileSync } = require("fs");
      writeFileSync(configPath, existingContent, "utf-8");
    } else if (existsSync(configPath)) {
      unlinkSync(configPath);
    }
  });

  test("getCloudConfig returns defaults when no config file", () => {
    // Remove config if exists
    if (existsSync(configPath)) {
      unlinkSync(configPath);
    }

    const config = getCloudConfig();
    expect(config.mode).toBe("local");
    expect(config.rds.host).toBe("");
    expect(config.rds.port).toBe(5432);
    expect(config.rds.username).toBe("");
    expect(config.rds.ssl).toBe(true);
    expect(config.auto_sync_interval_minutes).toBe(0);
  });

  test("saveCloudConfig writes and getCloudConfig reads back", () => {
    const config: CloudConfig = {
      rds: {
        host: "test-host.example.com",
        port: 5433,
        username: "testuser",
        password_env: "TEST_RDS_PASSWORD",
        ssl: false,
      },
      mode: "hybrid",
      auto_sync_interval_minutes: 15,
      feedback_endpoint: "https://test.example.com/feedback",
    };

    saveCloudConfig(config);
    expect(existsSync(configPath)).toBe(true);

    const loaded = getCloudConfig();
    expect(loaded.mode).toBe("hybrid");
    expect(loaded.rds.host).toBe("test-host.example.com");
    expect(loaded.rds.port).toBe(5433);
    expect(loaded.rds.username).toBe("testuser");
    expect(loaded.rds.password_env).toBe("TEST_RDS_PASSWORD");
    expect(loaded.rds.ssl).toBe(false);
    expect(loaded.auto_sync_interval_minutes).toBe(15);
    expect(loaded.feedback_endpoint).toBe(
      "https://test.example.com/feedback"
    );
  });

  test("getCloudConfig handles malformed JSON gracefully", () => {
    mkdirSync(dirname(configPath), { recursive: true });
    const { writeFileSync } = require("fs");
    writeFileSync(configPath, "not json!", "utf-8");

    const config = getCloudConfig();
    // Should return defaults
    expect(config.mode).toBe("local");
    expect(config.rds.host).toBe("");
  });
});
