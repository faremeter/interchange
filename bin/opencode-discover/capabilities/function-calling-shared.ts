export const TOOL_DEFINITION = {
  type: "function",
  function: {
    name: "getCurrentWeather",
    description: "Get the current weather conditions for a given city.",
    parameters: {
      type: "object",
      properties: {
        location: {
          type: "string",
          description: "The city and optional state, e.g. 'Boston, MA'.",
        },
      },
      required: ["location"],
    },
  },
} as const;

export const USER_PROMPT =
  "What is the weather in Boston, MA? Use the provided tool.";

export const SYNTHETIC_TOOL_RESPONSE = {
  weather: "62F partly cloudy",
  windMph: 8,
} as const;
