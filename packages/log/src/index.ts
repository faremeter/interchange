// Re-export core LogTape functionality
export {
  // Logger creation and usage
  getLogger,
  type Logger,
  type LogMethod,

  // Configuration
  configure,
  configureSync,
  dispose,
  disposeSync,
  reset,
  resetSync,
  getConfig,
  type Config,
  type ConfigError,
  type LoggerConfig,

  // Log levels
  type LogLevel,
  isLogLevel,
  parseLogLevel,
  compareLogLevel,
  getLogLevels,

  // Log records
  type LogRecord,

  // Sinks
  getConsoleSink,
  getStreamSink,
  withFilter,
  fingersCrossed,
  fromAsyncSink,
  type Sink,
  type AsyncSink,
  type ConsoleSinkOptions,
  type StreamSinkOptions,
  type FingersCrossedOptions,

  // Filters
  getLevelFilter,
  toFilter,
  type Filter,
  type FilterLike,

  // Formatters
  ansiColorFormatter,
  jsonLinesFormatter,
  defaultTextFormatter,
  defaultConsoleFormatter,
  getAnsiColorFormatter,
  getJsonLinesFormatter,
  getTextFormatter,
  type TextFormatter,
  type ConsoleFormatter,
  type TextFormatterOptions,
  type AnsiColorFormatterOptions,
  type JsonLinesFormatterOptions,
  type AnsiColor,
  type AnsiStyle,
  type FormattedValues,

  // Context
  withContext,
  withCategoryPrefix,
  type ContextLocalStorage,

  // Lazy evaluation
  lazy,
  isLazy,
  type Lazy,
} from "@logtape/logtape";

// Re-export our setup helper
export { setup, type SetupOptions } from "./setup";
