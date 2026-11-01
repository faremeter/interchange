import { describe, expect, test } from "bun:test";

import { pushRunGrants } from "./run-grants-push";

describe("pushRunGrants", () => {
  test("sends to every address and resolves when sends fail or are skipped", async () => {
    const sent: string[] = [];
    const router = {
      sendRunGrants: async (address: string) => {
        sent.push(address);
        if (address === "run_down@deploy.test")
          throw new Error("socket closed");
        return address === "run_live@deploy.test";
      },
    };

    await pushRunGrants(router, [
      "run_live@deploy.test",
      "run_offline@deploy.test",
      "run_down@deploy.test",
    ]);

    expect(sent).toEqual([
      "run_live@deploy.test",
      "run_offline@deploy.test",
      "run_down@deploy.test",
    ]);
  });
});
