// Operator-supplied custom inference adapter fixture for the cross-process
// integration test. It is reachable only through the operator-configured
// adapter manifest under provider id "custom-x"; resolving that provider
// inside the forked workflow child proves the manifest crossed the process
// boundary and was import()-ed child-side.
//
// It delegates to the built-in Anthropic adapter so it speaks the mock
// inference server's Anthropic-style SSE wire. The distinguishing proof is
// the provider id itself: "custom-x" is not a built-in, so a built-in-only
// registry would reject it with "Unknown inference provider".
//
// Import-side-effect-free: the module body only declares a factory export.
// The loader imports it once per process (sidecar boot and child boot).
import type { AdapterFactory } from "@intx/inference";
import { createAnthropicAdapter } from "@intx/inference/providers";

export const makeAdapter: AdapterFactory = (source) =>
  createAnthropicAdapter(source);
