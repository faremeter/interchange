// Reconstruct tool dispatches from a captured request transcript. The probe
// rig does not record tool dispatches directly; the tool calls and their
// fed-back results are embedded in the final request body of a
// multi-turn conversation. Walking that body recovers each (call, result) pair
// once, in call order, which is the session format's `dispatches/` record.
//
// The result is stored in the form that reproduces the wire when the replay
// harness feeds it back through the provider adapter: the verbatim content
// string for Anthropic and OpenAI (whose adapters pass strings through), and
// the response object for Google (which the canonical matcher re-sorts).

import { promises as fs } from "node:fs";
import * as path from "node:path";

export type ReconstructedDispatch = {
  toolName: string;
  args: unknown;
  result: unknown;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function anthropicResultString(content: unknown): string {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    if (content.length !== 1) {
      throw new Error(
        `dispatch reconstruction: Anthropic tool_result content must be a string or a single text block, got ${String(content.length)} blocks`,
      );
    }
    const only = content[0];
    if (
      !isRecord(only) ||
      only.type !== "text" ||
      typeof only.text !== "string"
    ) {
      throw new Error(
        "dispatch reconstruction: Anthropic tool_result content block must be a text block",
      );
    }
    return only.text;
  }
  throw new Error(
    "dispatch reconstruction: Anthropic tool_result content must be a string or text-block array",
  );
}

function pairById(
  calls: { id: string; name: string; args: unknown }[],
  results: Map<string, unknown>,
  provider: string,
): ReconstructedDispatch[] {
  const dispatches: ReconstructedDispatch[] = [];
  const paired = new Set<string>();
  for (const call of calls) {
    if (!results.has(call.id)) {
      throw new Error(
        `dispatch reconstruction: ${provider} tool_call ${JSON.stringify(call.id)} has no matching tool_result in the request transcript`,
      );
    }
    dispatches.push({
      toolName: call.name,
      args: call.args,
      result: results.get(call.id),
    });
    paired.add(call.id);
  }
  for (const id of results.keys()) {
    if (!paired.has(id)) {
      throw new Error(
        `dispatch reconstruction: ${provider} tool_result ${JSON.stringify(id)} has no matching tool_call in the request transcript`,
      );
    }
  }
  return dispatches;
}

function requireMessages(body: Record<string, unknown>): unknown[] {
  const messages = body.messages;
  if (!Array.isArray(messages)) {
    throw new Error(
      "dispatch reconstruction: request body has no messages array",
    );
  }
  return messages;
}

function extractAnthropicDispatches(
  body: Record<string, unknown>,
): ReconstructedDispatch[] {
  const calls: { id: string; name: string; args: unknown }[] = [];
  const results = new Map<string, unknown>();
  for (const message of requireMessages(body)) {
    if (!isRecord(message) || !Array.isArray(message.content)) continue;
    for (const block of message.content) {
      if (!isRecord(block)) continue;
      if (block.type === "tool_use") {
        calls.push({
          id: String(block.id),
          name: String(block.name),
          args: block.input,
        });
      } else if (block.type === "tool_result") {
        const id = String(block.tool_use_id);
        if (results.has(id)) {
          throw new Error(
            `dispatch reconstruction: Anthropic duplicate tool_result for ${JSON.stringify(id)}`,
          );
        }
        results.set(id, anthropicResultString(block.content));
      }
    }
  }
  return pairById(calls, results, "anthropic");
}

function extractOpenAIDispatches(
  body: Record<string, unknown>,
): ReconstructedDispatch[] {
  const calls: { id: string; name: string; args: unknown }[] = [];
  const results = new Map<string, unknown>();
  for (const message of requireMessages(body)) {
    if (!isRecord(message)) continue;
    if (message.role === "assistant" && Array.isArray(message.tool_calls)) {
      for (const toolCall of message.tool_calls) {
        if (!isRecord(toolCall)) continue;
        const fn = toolCall.function;
        if (!isRecord(fn)) {
          throw new Error(
            "dispatch reconstruction: OpenAI tool_call is missing a function object",
          );
        }
        calls.push({
          id: String(toolCall.id),
          name: String(fn.name),
          args: JSON.parse(String(fn.arguments)),
        });
      }
    } else if (message.role === "tool") {
      const id = String(message.tool_call_id);
      if (results.has(id)) {
        throw new Error(
          `dispatch reconstruction: OpenAI duplicate tool result for ${JSON.stringify(id)}`,
        );
      }
      // The tool message content is the verbatim string the client fed back;
      // storing it unparsed reproduces it exactly when the adapter re-emits it.
      results.set(id, message.content);
    }
  }
  return pairById(calls, results, "openai");
}

function extractGoogleDispatches(
  body: Record<string, unknown>,
): ReconstructedDispatch[] {
  const contents = body.contents;
  if (!Array.isArray(contents)) {
    throw new Error(
      "dispatch reconstruction: Google request body has no contents array",
    );
  }
  const calls: { name: string; args: unknown }[] = [];
  const responses: { name: string; response: unknown }[] = [];
  for (const content of contents) {
    if (!isRecord(content) || !Array.isArray(content.parts)) continue;
    for (const part of content.parts) {
      if (!isRecord(part)) continue;
      if (isRecord(part.functionCall)) {
        calls.push({
          name: String(part.functionCall.name),
          args: part.functionCall.args,
        });
      } else if (isRecord(part.functionResponse)) {
        responses.push({
          name: String(part.functionResponse.name),
          response: part.functionResponse.response,
        });
      }
    }
  }
  // Gemini carries no call ids; a functionResponse pairs with the functionCall
  // at the same ordinal position. Assert the names line up so a divergence
  // fails loudly instead of binding a result to the wrong call.
  if (calls.length !== responses.length) {
    throw new Error(
      `dispatch reconstruction: Google functionCall/functionResponse count mismatch (${String(calls.length)} vs ${String(responses.length)})`,
    );
  }
  const dispatches: ReconstructedDispatch[] = [];
  for (let i = 0; i < calls.length; i++) {
    const call = calls[i];
    const response = responses[i];
    if (call === undefined || response === undefined) continue;
    if (call.name !== response.name) {
      throw new Error(
        `dispatch reconstruction: Google functionResponse at position ${String(i)} names ${JSON.stringify(response.name)} but the functionCall names ${JSON.stringify(call.name)}`,
      );
    }
    dispatches.push({
      toolName: call.name,
      args: call.args,
      result: response.response,
    });
  }
  return dispatches;
}

// `adapterProvider` is the adapter-registry key the replay harness drives
// runInference against, so the extractor keys off the same name. The catalog
// providers `openai` and `opencode-zen` both speak the OpenAI protocol and
// resolve to the one `openai-compatible` adapter.
export function extractDispatches(
  adapterProvider: string,
  body: Record<string, unknown>,
): ReconstructedDispatch[] {
  switch (adapterProvider) {
    case "anthropic":
      return extractAnthropicDispatches(body);
    case "openai-compatible":
      return extractOpenAIDispatches(body);
    case "google-genai":
      return extractGoogleDispatches(body);
    default:
      throw new Error(
        `dispatch reconstruction: no dispatch extractor for adapter provider ${JSON.stringify(adapterProvider)}`,
      );
  }
}

export async function writeDispatches(
  sessionDir: string,
  dispatches: ReconstructedDispatch[],
): Promise<void> {
  if (dispatches.length === 0) return;
  const dispatchesDir = path.join(sessionDir, "dispatches");
  await fs.mkdir(dispatchesDir, { recursive: true });
  for (let i = 0; i < dispatches.length; i++) {
    const dispatch = dispatches[i];
    if (dispatch === undefined) continue;
    // The tool name comes from the captured transcript and becomes part of a
    // filename the session loader parses back out; reject anything that is not
    // a bare identifier so a stray path separator cannot escape the directory.
    if (!/^[A-Za-z0-9_-]+$/.test(dispatch.toolName)) {
      throw new Error(
        `dispatch reconstruction: tool name ${JSON.stringify(dispatch.toolName)} is not a bare identifier and cannot form a dispatch filename`,
      );
    }
    const file = path.join(
      dispatchesDir,
      `${String(i)}-${dispatch.toolName}.json`,
    );
    await fs.writeFile(
      file,
      `${JSON.stringify({ args: dispatch.args, result: dispatch.result }, null, 2)}\n`,
    );
  }
}
