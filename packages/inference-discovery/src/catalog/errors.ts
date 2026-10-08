// Thrown by a provider's request builder when it cannot construct a request
// for a (model, capability) pair. The throw precedes any network call, so
// nothing is sent and no charge is incurred. The discovery probe catches this
// type to record the cell as `unsupported`; every other error propagates, so
// a defect is never silently reclassified as "unsupported".
export class CapabilityNotBuildableError extends Error {
  readonly capability: string;

  constructor(capability: string, message: string) {
    super(message);
    this.name = "CapabilityNotBuildableError";
    this.capability = capability;
  }
}
