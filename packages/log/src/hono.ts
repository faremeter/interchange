// Install the default sink for consumers that import only @intx/log/hono.
import "./default-sink";

// Re-export Hono middleware from @logtape/hono
export {
  honoLogger,
  type HonoLogTapeOptions,
  type HonoContext,
  type PredefinedFormat,
  type FormatFunction,
  type RequestLogProperties,
} from "@logtape/hono";
