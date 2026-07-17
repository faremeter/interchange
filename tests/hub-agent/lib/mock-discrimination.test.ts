// Fast guard for the property the approval capstone rests on: the
// `approvalToolCall` mock discriminates strictly on the presence of a REAL
// tool_result block in history. It re-issues the tool_use whenever history
// lacks a tool_result -- including the broken-rail shape, where the approval
// decision was appended as a bare user turn with no result -- and replies only
// once a real tool_result block (the adapter wire shape) is present.
//
// If a future edit ever latched the mock or loosened the result detection so it
// could reply WITHOUT a tool_result, the run could complete without the tool
// running and the expensive integration capstone would silently go vacuous
// while still passing. This probe catches that in isolation (<1s, no DB, no
// hub, no sidecar), so it lives in the fast unit suite next to the fixture.

import { test, expect } from "bun:test";

import { startMockInference, type InferenceMessage } from "./deploy-flow-env";

// The plain tool name. The mock decodes each request's tool names before
// matching, and `decodeToolName` is the identity on a name that carries no
// `IX_` marker, so the probe sends the plain form the adapter would send for
// this name and the mock still matches it.
const TOOL = "@intx/tools-mail/sidecar-bundle:mail_send";
const PREFIX = "done: ";
const RESULT_TEXT = "wrote approval-tool-ran.txt";

async function ask(
  port: number,
  messages: InferenceMessage[],
): Promise<string> {
  const res = await fetch(`http://localhost:${String(port)}/v1/messages`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      tools: [{ name: TOOL, input_schema: {} }],
      messages,
    }),
  });
  return res.text();
}

test("approvalToolCall replies only when a real tool_result is in history", async () => {
  const mock = startMockInference({
    approvalToolCall: {
      toolName: TOOL,
      input: { to: "x", body: "y" },
      resultPrefix: PREFIX,
    },
  });
  const port = mock.server.port;
  if (port === undefined) {
    throw new Error("mock inference server has no bound port");
  }
  try {
    // (1) Fresh request, no history: re-issue the tool_use, do not reply.
    const fresh = await ask(port, [{ role: "user", content: "go" }]);
    expect(fresh).toContain("tool_use");
    expect(fresh).not.toContain(PREFIX);

    // (2) Broken-rail shape: the approval decision arrives as a bare user turn
    // after the assistant's tool_use, with NO tool_result. Must STILL re-issue,
    // never reply -- this is the exact history the old resume rail produced.
    const brokenRail = await ask(port, [
      { role: "user", content: "go" },
      {
        role: "assistant",
        content: [{ type: "tool_use", id: "toolu_1" }],
      },
      { role: "user", content: "approved" },
    ]);
    expect(brokenRail).toContain("tool_use");
    expect(brokenRail).not.toContain(PREFIX);

    // (3) A user turn that merely mentions the words "tool_result" as text must
    // NOT trigger a reply: only a real tool_result BLOCK counts.
    const textOnly = await ask(port, [
      { role: "user", content: "go" },
      { role: "user", content: "the tool_result is pending" },
    ]);
    expect(textOnly).toContain("tool_use");
    expect(textOnly).not.toContain(PREFIX);

    // (4) Fixed-rail shape: a real tool_result block (adapter wire shape) is in
    // history. Now, and only now, the mock replies -- reflecting the result
    // text so the capstone can assert the resumed reply carries the tool output.
    const fixedRail = await ask(port, [
      { role: "user", content: "go" },
      {
        role: "assistant",
        content: [{ type: "tool_use", id: "toolu_1" }],
      },
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            tool_use_id: "toolu_1",
            content: [{ type: "text", text: RESULT_TEXT }],
          },
        ],
      },
    ]);
    expect(fixedRail).not.toContain("tool_use");
    expect(fixedRail).toContain(`${PREFIX}${RESULT_TEXT}`);
  } finally {
    await mock.server.stop(true);
  }
});
