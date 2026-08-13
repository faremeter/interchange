import { createIsogitStorage } from "./index";
import { createBrowserIsogitRuntime } from "./browser-runtime";

export * from "./index";
export {
  createBrowserIsogitRuntime,
  type BrowserIsogitRuntime,
} from "./browser-runtime";

/**
 * Claim a fresh, single-owner LightningFS IndexedDB storage API.
 *
 * Construction clears prior contents for `name`. Create this once per name
 * and pass the returned API to every consumer in the page or worker.
 */
export function createBrowserIsogitStorage(name = "interchange") {
  const { runtime, fs } = createBrowserIsogitRuntime(name);
  return {
    ...createIsogitStorage(runtime),
    runtime,
    fs,
  };
}
