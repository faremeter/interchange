import { type } from "arktype";

export const ModelInfo = type({
  id: "string",
  providerId: "string",
  name: "string",
  "description?": "string | null",
  "capabilities?": "string[]",
  "pricing?": {
    "input?": "string",
    "output?": "string",
    "cacheRead?": "string",
    "cacheWrite?": "string",
  },
  "limits?": {
    "context?": "number",
    "output?": "number",
  },
});
