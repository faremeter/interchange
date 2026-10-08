// Install the default console sink before any caller can get a logger.
import "./default-sink";

// Narrow re-export over `@logtape/logtape`: only the symbols `@intx/*`
// packages use today, plus the `setup()` helper. Import missing symbols
// from `@logtape/logtape` directly; widen this only when a consumer
// needs them.
export {
  getLogger,
  configureSync,
  resetSync,
  getConfig,
} from "@logtape/logtape";
export { setup, type SetupOptions } from "./setup";
