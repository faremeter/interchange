import { describe, expect, test } from "bun:test";

import { resolveEffectiveSidecarPlacement } from "./placement-policy";

describe("resolveEffectiveSidecarPlacement", () => {
  test("permits shared placement when no policy requires exclusivity", () => {
    expect(
      resolveEffectiveSidecarPlacement({ tenantConfigs: [{}] }),
    ).toBeNull();
  });

  test("inherits an exclusive requirement from an ancestor tenant", () => {
    expect(
      resolveEffectiveSidecarPlacement({
        tenantConfigs: [{}, { sidecarPlacement: { sharing: "exclusive" } }],
      }),
    ).toEqual({ sharing: "exclusive", reuse: "never" });
  });

  test("allows reuse only when every declaring policy allows it", () => {
    expect(
      resolveEffectiveSidecarPlacement({
        tenantConfigs: [
          {
            sidecarPlacement: {
              sharing: "exclusive",
              reuse: "same-deployment",
            },
          },
        ],
      }),
    ).toEqual({ sharing: "exclusive", reuse: "same-deployment" });

    expect(
      resolveEffectiveSidecarPlacement({
        tenantConfigs: [
          {
            sidecarPlacement: {
              sharing: "exclusive",
              reuse: "same-deployment",
            },
          },
          { sidecarPlacement: { sharing: "exclusive" } },
        ],
      }),
    ).toEqual({ sharing: "exclusive", reuse: "never" });
  });
});
