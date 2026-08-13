import { expect, test } from "@playwright/test";

declare global {
  var indexedDB: {
    databases(): Promise<{ name?: string }[]>;
  };
}

test("keeps single-owner storage within a page and resets it on reload", async ({
  page,
}) => {
  const filesystemName = `interchange-playwright-${String(Date.now())}`;

  await page.goto("/");
  const written = await page.evaluate((name) => {
    return globalThis.storageIsogitSmoke.writeAndFlush(name);
  }, filesystemName);

  expect(written.packCount).toBe(1);
  expect(written.looseObjectCount).toBe(0);

  const inSession = await page.evaluate((name) => {
    return globalThis.storageIsogitSmoke.readInSession(name);
  }, filesystemName);

  expect(inSession.firstTurnText).toBe("stored for this browser session");
  expect(inSession.auditCallIds).toEqual(["browser-call-1"]);
  expect(inSession.sourceCommitHashes).toContain(written.sourceCommitSha);
  expect(inSession.targetCommitSha).toBe(written.deployCommitSha);

  const databaseNames = await page.evaluate(async () => {
    return (await globalThis.indexedDB.databases()).map(
      (database) => database.name,
    );
  });
  expect(databaseNames).toContain(filesystemName);

  await page.reload();
  const existsAfterReload = await page.evaluate((name) => {
    return globalThis.storageIsogitSmoke.repositoryExists(name, "/source");
  }, filesystemName);

  expect(existsAfterReload).toBe(false);
});
