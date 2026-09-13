import { describe, expect, test } from "bun:test";

import { isCoordType } from "@intx/authz";

import {
  senderCoordinates,
  type SenderKeyResolution,
} from "./sender-key-resolver";

describe("senderCoordinates", () => {
  test("a user sender yields principal + tenant coordinates", () => {
    const resolution: SenderKeyResolution = {
      source: "user",
      publicKey: "0xuserkey",
      principalId: "prn_user",
      tenantId: "tnt_user",
    };

    expect(senderCoordinates(resolution)).toEqual([
      { coordType: "principal", id: "prn_user" },
      { coordType: "tenant", id: "tnt_user" },
    ]);
  });

  test("a run sender yields principal + definition + tenant coordinates", () => {
    const resolution: SenderKeyResolution = {
      source: "run",
      publicKey: "0xrunkey",
      principalId: "prn_run",
      definitionId: "def_run",
      tenantId: "tnt_run",
    };

    expect(senderCoordinates(resolution)).toEqual([
      { coordType: "principal", id: "prn_run" },
      { coordType: "definition", id: "def_run" },
      { coordType: "tenant", id: "tnt_run" },
    ]);
  });

  test("a run sender with no principal fails closed to null", () => {
    // An acked anchor holds a signing key before its first trigger mints its
    // principal. It can sign, but has no principal to key admission on, so the
    // coordinate set is null (the gate treats it as the unknown sender).
    const resolution: SenderKeyResolution = {
      source: "run",
      publicKey: "0xrunkey",
      principalId: null,
      definitionId: "def_run",
      tenantId: "tnt_run",
    };

    expect(senderCoordinates(resolution)).toBeNull();
  });

  test("every produced coordinate is MailAcceptCoordinate-shaped", () => {
    const resolutions: SenderKeyResolution[] = [
      {
        source: "user",
        publicKey: "0xuserkey",
        principalId: "prn_user",
        tenantId: "tnt_user",
      },
      {
        source: "run",
        publicKey: "0xrunkey",
        principalId: "prn_run",
        definitionId: "def_run",
        tenantId: "tnt_run",
      },
    ];

    for (const resolution of resolutions) {
      const coordinates = senderCoordinates(resolution);
      expect(coordinates).not.toBeNull();
      for (const coordinate of coordinates ?? []) {
        expect(isCoordType(coordinate.coordType)).toBe(true);
        expect(coordinate.id.length).toBeGreaterThan(0);
      }
    }
  });
});
