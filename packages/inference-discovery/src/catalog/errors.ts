// Thrown by a provider's request builder when it cannot construct a request for
// a (model, capability) pair: the capability is not implemented for the
// provider, or the model's class does not offer it. The throw precedes any
// network call, so nothing is sent and no charge is incurred.
//
// The discovery probe catches this type specifically to record the cell as an
// `unsupported` outcome. Every other error — a malformed intent, a genuine bug
// in a body builder — is a different type and propagates, so the probe never
// silently reclassifies a defect as "unsupported".
export class CapabilityNotBuildableError extends Error {
  readonly capability: string;

  constructor(capability: string, message: string) {
    super(message);
    this.name = "CapabilityNotBuildableError";
    this.capability = capability;
  }
}
