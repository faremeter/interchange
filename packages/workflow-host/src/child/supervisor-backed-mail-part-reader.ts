// The step invoker resolves a mail part's ref through this reader. The
// supervisor owns the committed blob and answers `readMailPart`. This
// reader does not open the workflow-run repo.

import { base64Decode } from "@intx/types";
import type { MailPartReader } from "@intx/types/runtime";

import type { ChildMailboxCallBridge } from "./mailbox-call-bridge";

export function createSupervisorBackedMailPartReader(opts: {
  readonly callBridge: ChildMailboxCallBridge;
  readonly runId: string;
}): MailPartReader {
  return {
    async read(ref) {
      const response = await opts.callBridge.submit({
        op: "readMailPart",
        // The frame schema requires a run id. The part ref names the blob;
        // the supervisor does not consult this field.
        runId: opts.runId,
        partRef: ref,
      });
      if (response.op !== "readMailPart") {
        throw new Error(
          `supervisor-backed mail part reader: mailbox.call.response op ${JSON.stringify(response.op)} does not match "readMailPart"`,
        );
      }
      return base64Decode(response.value.contentBase64);
    },
  };
}
