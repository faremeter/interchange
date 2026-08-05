// The single-tool conversation shared by the live-session recorder
// (bin/record-live-sessions.ts) and the live-session replay test. The recorded
// request and the replayed request must declare the identical tool and prompt,
// so both sides read them from here rather than each holding their own copy.

import type { ToolDefinition } from "@intx/types/runtime";

export const LIVE_WEATHER_TOOL: ToolDefinition = {
  name: "weather",
  description: "Get the current weather for a city.",
  inputSchema: {
    type: "object",
    properties: {
      location: {
        type: "string",
        description: "City name, e.g. 'San Francisco'",
      },
    },
    required: ["location"],
  },
};

export const LIVE_TOOL_DEFINITIONS: ToolDefinition[] = [LIVE_WEATHER_TOOL];

export const LIVE_TOOL_PROMPT =
  "What is the current weather in San Francisco? " +
  "Use the weather tool to check, then tell me in one sentence.";
