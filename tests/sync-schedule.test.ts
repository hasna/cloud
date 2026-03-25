import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import {
  parseInterval,
  minutesToCron,
  getSyncScheduleStatus,
} from "../src/sync-schedule";
import {
  getCloudConfig,
  saveCloudConfig,
  getConfigPath,
} from "../src/config";
import { existsSync, readFileSync, writeFileSync, unlinkSync } from "fs";

describe("sync-schedule", () => {
  // -----------------------------------------------------------------------
  // parseInterval
  // -----------------------------------------------------------------------

  describe("parseInterval", () => {
    test("parses minutes with 'm' suffix", () => {
      expect(parseInterval("5m")).toBe(5);
      expect(parseInterval("10m")).toBe(10);
      expect(parseInterval("30m")).toBe(30);
    });

    test("parses hours with 'h' suffix", () => {
      expect(parseInterval("1h")).toBe(60);
      expect(parseInterval("2h")).toBe(120);
    });

    test("parses plain numbers as minutes", () => {
      expect(parseInterval("5")).toBe(5);
      expect(parseInterval("15")).toBe(15);
    });

    test("handles whitespace", () => {
      expect(parseInterval("  5m  ")).toBe(5);
      expect(parseInterval(" 1h ")).toBe(60);
    });

    test("is case insensitive", () => {
      expect(parseInterval("5M")).toBe(5);
      expect(parseInterval("1H")).toBe(60);
    });

    test("throws on invalid input", () => {
      expect(() => parseInterval("")).toThrow();
      expect(() => parseInterval("abc")).toThrow();
      expect(() => parseInterval("0m")).toThrow();
      expect(() => parseInterval("-5m")).toThrow();
    });
  });

  // -----------------------------------------------------------------------
  // minutesToCron
  // -----------------------------------------------------------------------

  describe("minutesToCron", () => {
    test("generates minute-level cron for < 60 minutes", () => {
      expect(minutesToCron(5)).toBe("*/5 * * * *");
      expect(minutesToCron(15)).toBe("*/15 * * * *");
      expect(minutesToCron(1)).toBe("*/1 * * * *");
    });

    test("generates hourly cron for exact hour multiples", () => {
      expect(minutesToCron(60)).toBe("0 */1 * * *");
      expect(minutesToCron(120)).toBe("0 */2 * * *");
    });

    test("generates minute-level cron for non-exact hour multiples", () => {
      // 90 minutes = 1h30m — not an exact hour multiple
      expect(minutesToCron(90)).toBe("*/90 * * * *");
    });

    test("throws on zero or negative", () => {
      expect(() => minutesToCron(0)).toThrow();
      expect(() => minutesToCron(-5)).toThrow();
    });
  });

  // -----------------------------------------------------------------------
  // getSyncScheduleStatus
  // -----------------------------------------------------------------------

  describe("getSyncScheduleStatus", () => {
    const configPath = getConfigPath();
    let hadExistingConfig = false;
    let existingContent: string | null = null;

    beforeEach(() => {
      if (existsSync(configPath)) {
        hadExistingConfig = true;
        existingContent = readFileSync(configPath, "utf-8");
      }
    });

    afterEach(() => {
      if (hadExistingConfig && existingContent !== null) {
        writeFileSync(configPath, existingContent, "utf-8");
      } else if (existsSync(configPath)) {
        unlinkSync(configPath);
      }
    });

    test("returns disabled status when schedule_minutes is 0", () => {
      const config = getCloudConfig();
      config.sync.schedule_minutes = 0;
      saveCloudConfig(config);

      const status = getSyncScheduleStatus();
      expect(status.registered).toBe(false);
      expect(status.schedule_minutes).toBe(0);
      expect(status.cron_expression).toBeNull();
    });

    test("returns enabled status when schedule_minutes > 0", () => {
      const config = getCloudConfig();
      config.sync.schedule_minutes = 10;
      saveCloudConfig(config);

      const status = getSyncScheduleStatus();
      expect(status.registered).toBe(true);
      expect(status.schedule_minutes).toBe(10);
      expect(status.cron_expression).toBe("*/10 * * * *");
    });
  });
});
