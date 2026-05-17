// Wire DSL barrel.
//
// Re-exports the per-provider helpers under namespaces (`wire.anthropic.*`,
// `wire.openai.*`) and exposes the agnostic helpers at the top level.
// Consumers import the whole namespace:
//
//     import { wire } from "@interchange/inference-testing";
//     stream.enqueueAt(0, wire.anthropic.messageStart({ usage: {...} }));
//     stream.enqueueAt(10, ...wire.assistantText("anthropic", "Hello"));
//
// The package only declares the `.` entry point in its `exports`, so
// `@interchange/inference-testing/wire/anthropic`-style deep imports do
// not resolve. Add the subpath to `package.json` if a future test author
// needs one.

export * as anthropic from "./anthropic";
export * as openai from "./openai";
export {
  assistantText,
  toolCall,
  usage,
  usageHead,
  completeResponse,
} from "./agnostic";
export type { Provider } from "./agnostic";
