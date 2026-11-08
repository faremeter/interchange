// Source-entry builder for a workflow whose run mails itself through the hub's
// run mail route. An `onTrigger` section runs one body per mail. The body's
// `relay` action reads the mail it was given:
//
//   - Mail from anyone but the run itself (the caller's kickoff): inside one
//     `perform`, compose a message to the run's own address with the
//     deployment key, optionally post a copy with its signed text changed,
//     then post the real message with the run's hub token. It returns each
//     post's status and error code, and the message id it composed.
//   - Mail from the run's own address: return its From, Message-ID and text,
//     and send nothing, so the run does not mail itself forever.
//
// The entry module exports the handler next to `workflow`. The deployment
// points `interchange.actions` at this same module.

/** The body action, so the test can read its step output. */
export const RELAY_STEP_ID = "relay";

/** The `onTrigger` section, so the test can derive its body run ids. */
export const SECTION_STEP_ID = "section";

/** The text of the message the run composes to itself. */
export const SELF_MAIL_TEXT = "from-self";

export type RunRestMailFixtureParams = {
  /** The run's own address: the mail trigger, the section, and the target. */
  address: string;
  /** The `defineWorkflow` id. */
  workflowId: string;
  /** The hub path of the run mail route for this run. */
  mailPath: string;
  /** Whether to post a copy with a changed signed body before the real one. */
  postTampered: boolean;
};

export function runRestMailWorkflowEntry(
  params: RunRestMailFixtureParams,
): string {
  return `
import { action, defineWorkflow, onTrigger } from "@intx/workflow/definition";

const body = defineWorkflow({
  id: ${JSON.stringify(`${params.workflowId}_body`)},
  trigger: { type: "manual" },
  steps: {
    ${JSON.stringify(RELAY_STEP_ID)}: action({
      handler: "relay",
      input: { from: "trigger.payload" },
      credentials: ["interchange.hub"],
      effect: { requires: ["call"] },
    }),
  },
});

export const workflow = defineWorkflow({
  id: ${JSON.stringify(params.workflowId)},
  trigger: { type: "mail", to: ${JSON.stringify(params.address)} },
  // The run mails itself, so it needs manage on its own run.
  grantRequirements: [
    { resource: "workflow-run:self", action: "manage", source: "creator" },
  ],
  steps: {
    ${JSON.stringify(SECTION_STEP_ID)}: onTrigger({
      on: { type: "mail", to: ${JSON.stringify(params.address)} },
      body,
    }),
  },
});

const ADDRESS = ${JSON.stringify(params.address.toLowerCase())};

// The addr-spec of a From value, which can carry a display name.
function addrSpec(from) {
  const angle = /<([^>]+)>/.exec(from);
  return (angle === null ? from : angle[1]).trim().toLowerCase();
}

async function post(hub, rawMessage) {
  const response = await hub.fetch(${JSON.stringify(params.mailPath)}, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ rawMessage }),
  });
  const body = await response.json().catch(() => null);
  const error =
    body !== null && typeof body === "object" && body.error !== null &&
    typeof body.error === "object" ? body.error : {};
  return {
    status: response.status,
    code: typeof error.code === "string" ? error.code : null,
  };
}

export async function relay(input, ctx, _signal) {
  const from = input?.headers?.from;
  if (typeof from !== "string" || from === "") {
    throw new Error("relay expected a mail with a From header");
  }
  if (addrSpec(from) === ADDRESS) {
    return {
      from: addrSpec(from),
      messageId: input.headers.messageId,
      text: input.parts?.[0]?.text ?? null,
    };
  }
  return ctx.perform({
    effectId: "self-mail",
    capability: "call",
    run: async ({ credentials, mailSigner }) => {
      const hub = await credentials.resolve("interchange.hub");
      if (hub.kind !== "http") {
        throw new Error("expected an http mediated credential, got " + hub.kind);
      }
      const composed = await mailSigner.compose({
        to: ADDRESS,
        content: ${JSON.stringify(SELF_MAIL_TEXT)},
      });
      let tampered = null;
      if (${JSON.stringify(params.postTampered)}) {
        const original = atob(composed.rawMessage);
        const changed = original.replace(
          ${JSON.stringify(SELF_MAIL_TEXT)},
          "from-evil",
        );
        if (changed === original) {
          throw new Error("the composed message does not carry its text verbatim");
        }
        tampered = await post(hub, btoa(changed));
      }
      const sent = await post(hub, composed.rawMessage);
      return { tampered, sent, messageId: composed.messageId };
    },
  });
}
`;
}
