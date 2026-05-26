// Install the default console sink so consumers that import only
// @intx/log/hono still get diagnostics before they call setup().
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
