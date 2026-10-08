// A fresh worker isolates both the payload cache and the ArkType module mock
// from tests that load the aggregate control-channel schema in the same worker.
import { mock } from "bun:test";
import { parentPort } from "node:worker_threads";
import * as arktype from "arktype";

const constructed: string[] = [];
const observedType = new Proxy(arktype.type, {
  apply(target, thisArg: unknown, args: unknown[]) {
    const definition = args[0];
    if (
      typeof definition === "object" &&
      definition !== null &&
      "type" in definition &&
      (definition.type === "'shutdown'" || definition.type === "'drain'")
    ) {
      constructed.push(definition.type);
    }
    const result: unknown = Reflect.apply(target, thisArg, args);
    return result;
  },
});
const observedArktype = { ...arktype, type: observedType };
await mock.module("arktype", () => observedArktype);

const { parseControlPayload } = await import("./control-payloads");
const afterImport = [...constructed];
parseControlPayload({ type: "unknown", data: {} });
const afterUnknown = [...constructed];
parseControlPayload({ type: "shutdown", data: { reason: "done" } });
const afterFirst = [...constructed];
parseControlPayload({ type: "shutdown", data: { reason: "again" } });
const afterRepeat = [...constructed];
parseControlPayload({ type: "drain", data: { deadlineMs: 100 } });
const afterSecond = [...constructed];
parseControlPayload({ type: "drain", data: { deadlineMs: 200 } });

if (!parentPort) throw new Error("Payload cache observation requires a worker");
parentPort.postMessage({
  afterImport,
  afterUnknown,
  afterFirst,
  afterRepeat,
  afterSecond,
  afterSecondRepeat: [...constructed],
});
parentPort.close();
