import type { SuspendableChildHandle } from "./env";

/**
 * The park reads of a fake body whose parks no caller reads or watches. A
 * call is a bug in the test that built the fake, so each rejects.
 */
export const noParkReleases: Pick<
  SuspendableChildHandle,
  "readParkRelease" | "waitForParkRelease"
> = {
  readParkRelease: (name) =>
    Promise.reject(new Error(`this fake body has no park on ${name} to read`)),
  waitForParkRelease: (name) =>
    Promise.reject(new Error(`this fake body has no park on ${name} to watch`)),
};
