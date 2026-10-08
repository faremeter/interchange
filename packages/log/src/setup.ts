import {
  configure,
  getConsoleSink,
  ansiColorFormatter,
  getJsonLinesFormatter,
  type LogLevel,
} from "@logtape/logtape";

export type SetupOptions = {
  /**
   * Override the log level for specific categories, keyed by dot-separated
   * path (e.g. "hub.requests").
   */
  levels?: Record<string, LogLevel>;

  /**
   * Force development mode (pretty console output) regardless of NODE_ENV.
   */
  dev?: boolean;

  /**
   * Force production mode (JSON output) regardless of NODE_ENV.
   */
  prod?: boolean;
};

type LoggerConfigEntry = {
  category: string[];
  lowestLevel: LogLevel;
  sinks: ["console"];
};

/**
 * Configures LogTape for the project: ANSI-colored console output at
 * debug level in development, JSON Lines at info level in production.
 * Call once at application startup.
 *
 * Each call passes `reset: true` to LogTape's `configure()`, so it
 * replaces the current configuration — including the module-load default
 * sink and any prior `setup()` call. A second invocation silently wins.
 */
export async function setup(options: SetupOptions = {}): Promise<void> {
  const isDev =
    options.dev ?? (!options.prod && process.env["NODE_ENV"] !== "production");
  const defaultLevel: LogLevel = isDev ? "debug" : "info";

  const loggers: LoggerConfigEntry[] = [
    {
      category: ["logtape", "meta"],
      lowestLevel: "warning",
      sinks: ["console"],
    },
    {
      category: [],
      lowestLevel: defaultLevel,
      sinks: ["console"],
    },
  ];

  if (options.levels) {
    for (const [categoryPath, level] of Object.entries(options.levels)) {
      const category = categoryPath.split(".");
      loggers.push({
        category,
        lowestLevel: level,
        sinks: ["console"],
      });
    }
  }

  await configure({
    reset: true,
    sinks: {
      console: getConsoleSink({
        formatter: isDev ? ansiColorFormatter : getJsonLinesFormatter(),
      }),
    },
    loggers,
  });
}
