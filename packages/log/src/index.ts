// Install the default console sink before any caller can get a logger.
import "./default-sink";

// `@intx/log` is a narrow re-export over `@logtape/logtape`. It carries
// only the symbols other `@intx/*` packages actually use today, plus the
// project's `setup()` helper. Consumers that need a piece of LogTape not
// re-exported here should import it from `@logtape/logtape` directly
// rather than widening this surface speculatively — widen it only when
// at least one consumer needs the symbol.
export {
  getLogger,
  configureSync,
  resetSync,
  getConfig,
} from "@logtape/logtape";
export { setup, type SetupOptions } from "./setup";
