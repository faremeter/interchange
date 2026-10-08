import { type } from "arktype";

import { hexDecode, hexEncode } from "@intx/types/hex";

const IPC_HMAC_KEY_BYTES = 32;

// Env keys the host sets on the child's fresh spawn env. The child reads
// exactly these plus PATH/HOME/TMPDIR (for exec + tmp); nothing else
// crosses the airlock.
const PROBE_CHANNEL_ID_ENV = "PROBE_IPC_CHANNEL_ID";
const PROBE_HMAC_KEY_ENV = "PROBE_IPC_HMAC_KEY";
const PROBE_PACKAGE_DIR_ENV = "PROBE_PACKAGE_DIR";

export function buildProbeChildEnv(args: {
  packageDir: string;
  channelId: string;
  hmacKey: Uint8Array;
}): Record<string, string> {
  // A fresh, minimal env: exactly the IPC anchors and the materialized
  // package dir, plus the OS handles the shebang needs to exec `bun` and
  // land temp files on the host's temp root. No `process.env` spread, so
  // no sidecar secret or ambient input crosses the airlock.
  const env: Record<string, string> = {
    [PROBE_CHANNEL_ID_ENV]: args.channelId,
    [PROBE_HMAC_KEY_ENV]: hexEncode(args.hmacKey),
    [PROBE_PACKAGE_DIR_ENV]: args.packageDir,
  };
  const path = process.env["PATH"];
  if (path !== undefined) env["PATH"] = path;
  const home = process.env["HOME"];
  if (home !== undefined) env["HOME"] = home;
  const tmpdir = process.env["TMPDIR"];
  if (tmpdir !== undefined) env["TMPDIR"] = tmpdir;
  return env;
}

interface ProbeChildEnv {
  readonly channelId: string;
  readonly hmacKey: Uint8Array;
  readonly packageDir: string;
}

const NonEmptyString = type("string > 0");

export function parseProbeChildEnv(
  rawEnv: Readonly<Record<string, string | undefined>>,
): ProbeChildEnv {
  const channelId = requireEnv(rawEnv, PROBE_CHANNEL_ID_ENV);
  const packageDir = requireEnv(rawEnv, PROBE_PACKAGE_DIR_ENV);
  const hmacKeyHex = requireEnv(rawEnv, PROBE_HMAC_KEY_ENV);
  const hmacKey = hexDecode(hmacKeyHex);
  if (hmacKey.length !== IPC_HMAC_KEY_BYTES) {
    throw new Error(
      `workflow probe child env: ${PROBE_HMAC_KEY_ENV} must decode to ${String(IPC_HMAC_KEY_BYTES)} bytes, got ${String(hmacKey.length)}`,
    );
  }
  return { channelId, hmacKey, packageDir };
}

function requireEnv(
  rawEnv: Readonly<Record<string, string | undefined>>,
  key: string,
): string {
  const value = NonEmptyString(rawEnv[key]);
  if (value instanceof type.errors) {
    throw new Error(
      `workflow probe child env: required key ${key} is unset or empty`,
    );
  }
  return value;
}
