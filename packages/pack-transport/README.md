# @intx/pack-transport

Git pack chunking and reassembly for the hub-sidecar wire. Splits
a packfile into ordered `repo.pack.push` frames on the sender, and
reassembles the chunks on the receiver while validating sequence
continuity and rejecting concurrent transfers for the same agent.

Consumed by `@intx/hub-sessions` and `@intx/hub-agent` to ship
agent repository state across the WebSocket link.

```ts
import { chunkPack, createPackReceiver } from "@intx/pack-transport";

for (const chunk of chunkPack(packBytes)) {
  await ws.send({
    type: "repo.pack.push",
    agentAddress,
    repoId,
    transferId,
    seq: chunk.seq,
    data: chunk.data,
  });
}

const receiver = createPackReceiver();
// On each inbound frame:
const reject = receiver.handlePush(frame);
if (reject) throw new Error(`pack transfer rejected: ${reject}`);
const completed = receiver.handleDone(doneFrame);
if (completed) {
  // completed.pack is the reassembled packfile bytes.
}
```
