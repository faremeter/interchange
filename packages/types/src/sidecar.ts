// Websocket wire protocol for hub↔sidecar communication.
//
// One websocket connection per sidecar↔hub pair. All traffic is multiplexed
// as JSON frames with a `type` discriminator. The sidecar initiates the
// connection; the hub is the server.
//
// Mail bytes are base64-encoded in JSON frames. Binary frames would be more
// efficient but JSON is simpler to debug and inspect.

import { type } from "arktype";
import { WireGrantRule } from "./grant-wire";
import {
  BoundedApprovalSnapshot,
  ConnectorThreadState,
  HarnessConfig,
  InferenceEvent,
  InferenceSource,
} from "./runtime";
import { SignalKind } from "./signals";

// ---------------------------------------------------------------------------
// Sidecar → Hub
// ---------------------------------------------------------------------------

/**
 * Sent on first connect when the sidecar has no existing agents in its data
 * directory. Identifies the sidecar and declares it ready to receive
 * agent.deploy frames.
 */
export const RegisterFrame = type({
  type: "'register'",
  sidecarId: "string",
  token: "string",
  agentAddresses: "string[]",
});
export type RegisterFrame = typeof RegisterFrame.infer;

/**
 * Sent on connect when the sidecar has agent repositories or deployments
 * from a previous run. Lists the addresses it can serve, triggering the
 * challenge/response ownership-verification flow for every one of them --
 * launched agents and workflow deployments alike, so both are proven, not
 * routed on trust.
 */
export const ReconnectFrame = type({
  type: "'reconnect'",
  sidecarId: "string",
  token: "string",
  agentAddresses: "string[]",
  "deployRefs?": "Record<string, string>",
});
export type ReconnectFrame = typeof ReconnectFrame.infer;

/**
 * Response to a challenge frame. Contains a signature per run address
 * proving the sidecar holds the private key. Each signature is computed
 * over `nonce || utf8(agentAddress)`.
 */
export const ChallengeResponseFrame = type({
  type: "'challenge.response'",
  responses: type({ address: "string", signature: "string" }).array(),
});
export type ChallengeResponseFrame = typeof ChallengeResponseFrame.infer;

/**
 * Acknowledges a successful agent deployment. Includes the agent's Ed25519
 * public key (hex-encoded) so the hub can verify ownership on reconnect.
 */
export const AgentDeployAckFrame = type({
  type: "'agent.deploy.ack'",
  agentAddress: "string",
  publicKey: "string",
});
export type AgentDeployAckFrame = typeof AgentDeployAckFrame.infer;

/**
 * Reports a failed agent deployment.
 */
export const AgentErrorFrame = type({
  type: "'agent.error'",
  agentAddress: "string",
  error: "string",
});
export type AgentErrorFrame = typeof AgentErrorFrame.infer;

/**
 * A message from a local agent. When `delivered` is absent or false the hub
 * should route the message to its recipients. When `delivered` is true the
 * message was already delivered locally and is forwarded for audit/projection
 * only — the hub must not re-route it.
 *
 * Structured metadata (senderAddress, messageId, to, cc) is available for
 * audit and projection purposes without parsing the raw MIME bytes.
 */
export const MailOutboundFrame = type({
  type: "'mail.outbound'",
  rawMessage: "string",
  recipients: "string[]",
  "senderAddress?": "string",
  "sessionId?": "string",
  "messageId?": "string",
  "to?": "string[]",
  "cc?": "string[]",
  "delivered?": "boolean",
});
export type MailOutboundFrame = typeof MailOutboundFrame.infer;

/**
 * An InferenceEvent from the reactor, forwarded for UI consumption. Tagged
 * with the run address so the hub can route to the correct UI client.
 */
export const AgentEventFrame = type({
  type: "'agent.event'",
  agentAddress: "string",
  sessionId: "string",
  event: InferenceEvent,
});
export type AgentEventFrame = typeof AgentEventFrame.infer;

/**
 * Notifies the hub that the agent's connector-thread state has changed.
 * The sidecar emits this when the harness's connector router commits a
 * start/continue decision, when an outbound reply advances the
 * lastMessageId, and when load-time restore brings persisted state into
 * memory. The hub uses the cached state to set threading headers on
 * user-originated mail so the harness routes it as `continue` rather
 * than `passthrough`.
 *
 * `connectorState` is `null` when no active thread exists.
 */
export const ConnectorStateChangedFrame = type({
  type: "'connector.state.changed'",
  agentAddress: "string",
  connectorState: ConnectorThreadState.or("null"),
});
export type ConnectorStateChangedFrame =
  typeof ConnectorStateChangedFrame.infer;

/**
 * Keepalive ping sent by the sidecar. The hub responds with a pong frame.
 * If the hub stops receiving pings, it considers the sidecar dead.
 */
export const PingFrame = type({ type: "'ping'" });
export type PingFrame = typeof PingFrame.infer;

/**
 * Acknowledges a request from the hub (sources.update).
 */
export const SessionAckFrame = type({
  type: "'session.ack'",
  requestId: "string",
});
export type SessionAckFrame = typeof SessionAckFrame.infer;

/**
 * Reports an error processing a hub request.
 */
export const SessionErrorFrame = type({
  type: "'session.error'",
  requestId: "string",
  error: "string",
});
export type SessionErrorFrame = typeof SessionErrorFrame.infer;

/**
 * Acknowledges that an agent has been fully undeployed: harness stopped,
 * state pushed (best-effort), and directory deleted.
 */
export const AgentUndeployAckFrame = type({
  type: "'agent.undeploy.ack'",
  agentAddress: "string",
  statePushed: "boolean",
});
export type AgentUndeployAckFrame = typeof AgentUndeployAckFrame.infer;

/**
 * Registers a control-signal correlation as a workflow agent step suspends.
 * The fields on this frame all converge at the sidecar's suspend emit point;
 * the hub uses them to co-write the `signal_correlation` routing row and the
 * `approval` row in one transaction, so the eventual resolver can route a
 * delivered decision back to the parked run and flip its approval.
 *
 * `signalName` is deliberately NOT on the wire: it is a pure function of
 * `correlationId` (`signalName(correlationId)` in `./signals`), so the hub
 * computes it rather than trusting a value the sidecar could disagree on.
 * `anchorRunId` is the anchor run the parked run belongs to; `agentAddress`
 * is the anchor run's routable address the hub resolves tenancy from.
 */
export const SignalCorrelationRegisterFrame = type({
  type: "'signal.correlation.register'",
  correlationId: "string",
  runId: "string",
  anchorRunId: "string",
  agentAddress: "string",
  kind: SignalKind,
  // Approver-facing snapshot of the suspended tool call, size-capped at this
  // trust boundary. Required: the ask rail is the only producer of this frame
  // and always carries a snapshot, so a snapshot-absent frame fails this parse
  // at the receiver (logged and dropped, never co-written as a null row).
  snapshot: BoundedApprovalSnapshot,
});
export type SignalCorrelationRegisterFrame =
  typeof SignalCorrelationRegisterFrame.infer;

// ---------------------------------------------------------------------------
// Hub → Sidecar
// ---------------------------------------------------------------------------

/**
 * Hub acknowledges a `signal.correlation.register`: the routing + approval
 * co-write for this correlationId is durable (whether this frame inserted the
 * rows or found them already present). It lets the sidecar's link stop
 * retrying a register whose frame may have been lost on an open socket or
 * evicted from the bounded send queue. Keyed on correlationId alone -- every
 * producer of the register (the initial park, the respawn/reconnect re-emit, a
 * link retry) carries the same correlationId and drives the same idempotent
 * co-write, so the ack asserts the one fact that matters: a row exists for this
 * correlation.
 */
export const SignalCorrelationRegisterAckFrame = type({
  type: "'signal.correlation.register.ack'",
  agentAddress: "string",
  correlationId: "string",
});
export type SignalCorrelationRegisterAckFrame =
  typeof SignalCorrelationRegisterAckFrame.infer;

/**
 * A message to deliver to a local agent's INBOX. The hub routes inbound
 * mail (from UI users, from agents on other sidecars) to the correct
 * sidecar connection.
 *
 * `messageId` is the hub-minted id of this delivery, carried so the sidecar
 * can acknowledge durable receipt (`mail.inbound.ack`) keyed on the SAME id
 * the hub tracks -- no per-side re-derivation. It is the id the hub minted at
 * ingress (also the message's `Message-ID` header), so a redelivery replays
 * identical bytes and the downstream `RunStarted` dedup (consumedMessageIds)
 * makes at-least-once effectively-once. Present only on hub-originated mail
 * that participates in the ack/retry handshake (workflow trigger mail, session
 * conversation mail); agent-to-agent relayed mail omits it.
 */
export const MailInboundFrame = type({
  type: "'mail.inbound'",
  agentAddress: "string",
  rawMessage: "string",
  "messageId?": "string",
});
export type MailInboundFrame = typeof MailInboundFrame.infer;

/**
 * Sidecar acknowledges durable receipt of a `mail.inbound`: the message is in
 * the agent's on-disk inbox. The hub holds each delivered mail in a pending
 * map and retries until this ack lands (or reconnect-redelivers it), so a
 * message dropped in the connected/reconnecting window is not silently lost.
 * Keyed on the hub-minted `messageId` the `mail.inbound` carried, so the ack
 * clears exactly the pending entry it resolves; the ack is only sent AFTER the
 * durable inbox write resolves (a non-ack IS the retry signal). At-least-once
 * delivery is made effectively-once by the `RunStarted`/signal dedup guards.
 */
export const MailInboundAckFrame = type({
  type: "'mail.inbound.ack'",
  agentAddress: "string",
  messageId: "string",
});
export type MailInboundAckFrame = typeof MailInboundAckFrame.infer;

/**
 * Deliver a workflow-run signal to a multi-step deployment's
 * supervisor. The hub forwards the frame to the sidecar that hosts the
 * deployment named by `agentAddress` (the deployment-level mail
 * address). The sidecar's hub-link routes the frame into the matching
 * supervisor's `deliverSignal`, which sends a `signal.deliver` control
 * IPC frame to the workflow-process child. The child commits the
 * `SignalReceived` event through its own substrate -- the single
 * writer of the workflow-run repo on the sidecar side -- so the
 * pack-push pipeline that propagates the commit to the hub never sees
 * a concurrent writer at the same ref.
 *
 * `signalId` is supplied by the producer so the workflow-run state
 * machine's dedup index (`observedSignalIds`) rejects a duplicate
 * delivery cleanly; a fresh value per call is the producer's
 * responsibility.
 */
export const SignalDeliverFrame = type({
  type: "'signal.deliver'",
  agentAddress: "string",
  runId: "string",
  signalName: "string",
  signalId: "string",
  payload: "unknown",
});
export type SignalDeliverFrame = typeof SignalDeliverFrame.infer;

/**
 * Deliver a run's authorization grants to a multi-step deployment's
 * supervisor. The hub forwards the frame to the sidecar that hosts the
 * deployment named by `agentAddress` (the deployment-level mail
 * address). The sidecar's hub-link routes the frame into the matching
 * deployment's wiring, which writes the grants to `runs/<runId>/grants.json`
 * inside the deployment's `workflow-run` repo -- sibling to the run's
 * `runs/<runId>/events/` subtree.
 *
 * `stepGrants` carries the same `WireGrantRule` shape the `agent.deploy`
 * frame's `config.grants` ships, so the run's grants ride the same
 * validated grant encoding as the deploy-time step grants rather than a
 * new one.
 */
export const RunGrantsFrame = type({
  type: "'run.grants'",
  agentAddress: "string",
  runId: "string",
  stepGrants: WireGrantRule.array(),
});
export type RunGrantsFrame = typeof RunGrantsFrame.infer;

/**
 * Deliver a workflow-host drain control payload to a multi-step
 * deployment's supervisor. The hub forwards the frame to the sidecar
 * that hosts the deployment named by `agentAddress` (the
 * deployment-level mail address). The sidecar's hub-link routes the
 * frame into the matching supervisor's `drain`, which sends a `drain`
 * control IPC frame to the workflow-process child and arms one
 * `drainTimeout` accumulator per in-flight run. Cancel-mode in-flight
 * steps abort on the child side as the controller's signal flips;
 * wait-mode steps continue. Each accumulator commits a signed
 * `CancelRequested{origin: "supervisor-drain"}` against the
 * workflow-run repo through the supervisor's substrate when the
 * deadline expires.
 *
 * `deadlineMs` is the wire-level policy hint the child echoes in its
 * logs. The supervisor's accumulator is driven by its own bindings'
 * `drainTimeoutMs` -- a per-deployment operator setting -- not by this
 * value; the wire field exists so the child's log reflects the
 * caller's intent.
 */
export const DrainDeliverFrame = type({
  type: "'drain.deliver'",
  agentAddress: "string",
  deadlineMs: "number",
});
export type DrainDeliverFrame = typeof DrainDeliverFrame.infer;

/**
 * Closed plain-JSON schema for a single wire step, one arm per workflow
 * primitive variant. It replaces the former `unknown` passthrough: a step
 * on the deploy frame must now declare a `kind` drawn from the closed set
 * of ten primitive kinds; a step with no `kind`, or a `kind` outside the
 * set, is rejected at this boundary instead of carried through opaque.
 *
 * Each arm declares its variant's own fields (type-checked when present)
 * but leaves them optional and passes undeclared keys through unmodified.
 * That is deliberate and mirrors the envelope's own contract: deeper
 * authoring-time primitive validation — required-field presence, selector
 * resolvability, DAG shape — lives on the workflow definition surface in
 * `@intx/workflow`, not on the wire. The closure that matters here is over
 * the KIND discriminant, which is what makes the canonical JSON of a step
 * deterministic across the child->hub boundary.
 *
 * The nested `agent`, inner `step`, `body`, `on`, and selector fields are
 * typed `unknown` on purpose. Two producers feed this schema: the
 * live-deploy passthrough ships a step whose `agent.toolFactories` are
 * functions that JSON-encode to `null`, while the live->inert projector in
 * `@intx/workflow-deploy` ships a reified plain-data agent. Both must
 * validate here; reifying the grant surface into plain data is the
 * projector's job, not this envelope's.
 */
const commonStepFields = {
  "id?": "string",
  "after?": "string[]",
} as const;

const WireStepStep = type({
  kind: "'step'",
  ...commonStepFields,
  "agent?": "unknown",
  "input?": "unknown",
  "reads?": "unknown",
  "writes?": "unknown",
  "retry?": "unknown",
  "timeout?": "number",
  "drainBehavior?": "'cancel' | 'wait'",
  "triggers?": "number | 'unbounded'",
});

const WireStepMap = type({
  kind: "'map'",
  ...commonStepFields,
  "over?": "unknown",
  "step?": "unknown",
  "retry?": "unknown",
});

const WireStepGate = type({
  kind: "'gate'",
  ...commonStepFields,
  "when?": "unknown",
  "then?": "string",
  "else?": "string",
});

const WireStepAwaitSignal = type({
  kind: "'awaitSignal'",
  ...commonStepFields,
  "name?": "string",
  "timeout?": "number",
  "onTimeout?": "string",
  "drainBehavior?": "'cancel' | 'wait'",
});

const WireStepSleep = type({
  kind: "'sleep'",
  ...commonStepFields,
  "duration?": "number",
  "until?": "string",
  "drainBehavior?": "'cancel' | 'wait'",
});

const WireStepChildWorkflow = type({
  kind: "'childWorkflow'",
  ...commonStepFields,
  "definitionRef?": "string",
  "input?": "unknown",
  "drainBehavior?": "'cancel' | 'wait'",
});

const WireStepEscalation = type({
  kind: "'escalation'",
  ...commonStepFields,
  "to?": "string",
  "data?": "unknown",
});

const WireStepAction = type({
  kind: "'action'",
  ...commonStepFields,
  "handler?": "string",
  "input?": "unknown",
  "effect?": "unknown",
  "timeout?": "number",
  "drainBehavior?": "'cancel' | 'wait'",
});

const WireStepLoop = type({
  kind: "'loop'",
  ...commonStepFields,
  "body?": "unknown",
  "while?": "string",
  "carry?": "string",
  "input?": "unknown",
  "maxIterations?": "number",
  "onExhausted?": "string",
  "drainBehavior?": "'cancel' | 'wait'",
});

const WireStepOnTrigger = type({
  kind: "'onTrigger'",
  ...commonStepFields,
  "on?": "unknown",
  "body?": "unknown",
  "drainBehavior?": "'cancel' | 'wait'",
});

/**
 * The closed union of wire step variants. Exported so the live->inert
 * projector's producer and its mutation-test suite can validate a single
 * step against the same schema the deploy frame applies to every step.
 */
export const WorkflowStep = WireStepStep.or(WireStepMap)
  .or(WireStepGate)
  .or(WireStepAwaitSignal)
  .or(WireStepSleep)
  .or(WireStepChildWorkflow)
  .or(WireStepEscalation)
  .or(WireStepAction)
  .or(WireStepLoop)
  .or(WireStepOnTrigger);
export type WorkflowStep = typeof WorkflowStep.infer;

/**
 * The `steps` record on a wire projection: every value must validate
 * against the closed `WorkflowStep` union. The runtime constraint runs
 * through a `.narrow` over a `Record<string, unknown>` rather than a typed
 * `{ "[string]": WorkflowStep }` on purpose: the inferred type stays
 * `Record<string, unknown>` so the existing live-deploy producer
 * (`toWireWorkflowDefinition`, which hands a `Record<string, unknown>`
 * steps map to `sendAgentDeploy`) still typechecks, while the runtime
 * validation is fully closed over the primitive-kind set.
 */
const WorkflowSteps = type({ "[string]": "unknown" }).narrow((steps, ctx) => {
  for (const [stepId, step] of Object.entries(steps)) {
    const parsed = WorkflowStep(step);
    if (parsed instanceof type.errors) {
      return ctx.mustBe(
        `a record whose every step matches a known workflow primitive ` +
          `variant; step ${JSON.stringify(stepId)} did not (${parsed.summary})`,
      );
    }
  }
  return true;
});

/**
 * Workflow projection carried on an `agent.deploy` frame. Its presence
 * at the deploy router routes the frame to the workflow deploy path --
 * single- or multi-step, both of which spawn the workflow-process child
 * -- as opposed to a per-step provision frame.
 *
 * `definition` is the wire projection of `WorkflowDefinition` from
 * `@intx/workflow`. The arktype validator enforces the structural
 * envelope the workflow-process child re-parses on the sidecar after
 * materialization (`packages/hub-sessions/src/workflow-kind.ts`'s
 * `workflowDefinitionEnvelopeSchema`): `id`, `triggers`, `steps`,
 * `stepOrder`, optional `state`. The wire validator MUST require every
 * field the envelope requires — the sidecar's deploy router serializes
 * `projection.definition` verbatim into `workflow.json` and the child
 * rejects a tree missing any envelope-required field. Deeper validation
 * of authoring-time primitive shape lives on the workflow definition
 * surface in `@intx/workflow`, not on the wire.
 *
 * `sources` pins an ordered, non-empty inference-source list per step in
 * `definition.stepOrder` so the workflow-process child can resolve inference
 * at step invocation without a round trip to the hub. The list is the step's
 * failover chain: element 0 is the active source (its id is the step's
 * `defaultSource`), and the reactor fails over forward through the tail on a
 * transient inference error. A workflow step pins a single-element list (no
 * per-step failover); a single-agent workflow pins its full
 * ordered source chain. Every `stepOrder` entry must have a matching
 * `sources` entry; the validator rejects frames that violate this at the
 * boundary.
 */
export const WorkflowProjectionDefinition = type({
  id: "string > 0",
  triggers: "unknown[]",
  stepOrder: "string[]",
  steps: WorkflowSteps,
  "state?": "Record<string, unknown>",
  "+": "delete",
});
export type WorkflowProjectionDefinition =
  typeof WorkflowProjectionDefinition.infer;

/**
 * A workflow projection paired with its per-step inference-source pins, with
 * the invariant that every `stepOrder` entry has a `sources` failover chain.
 * The narrow here is the same coverage check the top-level `AgentDeployWorkflow`
 * applies to its own definition; this reusable form carries it into each
 * extracted onTrigger body under `referencedDefinitions`, so a body's sources
 * cover the body's stepOrder just as the top-level's cover the top-level's.
 */
const WorkflowProjectionWithSources = type({
  definition: WorkflowProjectionDefinition,
  sources: { "[string]": InferenceSource.array().atLeastLength(1) },
}).narrow((value, ctx) => {
  for (const stepId of value.definition.stepOrder) {
    if (!Object.prototype.hasOwnProperty.call(value.sources, stepId)) {
      return ctx.mustBe(
        `a workflow projection whose sources cover every step in stepOrder; ${JSON.stringify(stepId)} is missing`,
      );
    }
  }
  return true;
});

/**
 * The decrypted credential material and per-handle binding descriptors
 * delivered to a running agent so its tools can use provider-backed
 * credentials. Secrets are decrypted hub-side and ride this payload on the
 * live channel ONLY -- the deploy frame at launch, a `credentials.update`
 * frame on rotation, and the child's in-memory cell. They are NEVER written to
 * disk (they do not ride the git-committed grants file) and NEVER copied into
 * any snapshot, event, or state -- redaction is by construction, mirroring how
 * an `InferenceSource`'s `apiKey` stays off every egress type.
 *
 * `materials` is keyed by `credentialId` (a credential can back several handles,
 * so its secret is stored once); `bindings` maps each declared tool handle to
 * the credential that backs it and the consumer identity allowed to use it.
 */
export const CredentialMaterialEntry = type({
  credentialId: "string",
  providerKey: "string",
  origin: "string",
  secret: "string",
});
export type CredentialMaterialEntry = typeof CredentialMaterialEntry.infer;

export const CredentialBindingDescriptor = type({
  handle: "string",
  credentialId: "string",
  consumer: "string",
});
export type CredentialBindingDescriptor =
  typeof CredentialBindingDescriptor.infer;

export const CredentialDelivery = type({
  bindings: CredentialBindingDescriptor.array(),
  materials: CredentialMaterialEntry.array(),
});
export type CredentialDelivery = typeof CredentialDelivery.infer;

export const AgentDeployWorkflow = type({
  definition: WorkflowProjectionDefinition,
  sources: { "[string]": InferenceSource.array().atLeastLength(1) },
  // Extracted onTrigger section bodies, materialized to their own workflow
  // assets on the sidecar so a body child's spawn-child resolves the body by
  // ref without a hub round-trip (the body id IS the asset ref). Optional: only
  // an onTrigger deploy carries it, and every existing non-onTrigger deploy
  // omits it and still validates. Each entry carries the body definition AND
  // the body's own per-step inference-source pins, materialized beside the body
  // on disk (`sources.json`) so a body child -- in-process, its env lost across
  // a restart -- resolves inference durably without a hub round-trip.
  "referencedDefinitions?": WorkflowProjectionWithSources.array(),
  // Initial credential material for the deployment's tools, decrypted hub-side
  // and delivered on the deploy frame so it is resident before any step runs
  // (closing the race where a tool resolves a credential before a push lands).
  // Run-global: a credential's secret is stored once, keyed by credentialId.
  // Optional -- a deploy whose definition binds no credentials omits it.
  "credentials?": CredentialDelivery,
}).narrow((value, ctx) => {
  for (const stepId of value.definition.stepOrder) {
    if (!Object.prototype.hasOwnProperty.call(value.sources, stepId)) {
      return ctx.mustBe(
        `a workflow projection whose sources cover every step in stepOrder; ${JSON.stringify(stepId)} is missing`,
      );
    }
  }
  return true;
});
export type AgentDeployWorkflow = typeof AgentDeployWorkflow.infer;

/**
 * Deploy an agent to this sidecar. The sidecar spawns a supervised
 * workflow-process child to host the deployment.
 *
 * The deploy router discriminates two shapes by field presence without
 * consulting `config`:
 *   - `workflow` set: a workflow deployment (single-step head or multi-step)
 *     that spawns the supervised workflow-process child.
 *   - `provisionStep` true: a no-spawn per-step provision of a multi-step
 *     deploy -- the sidecar initializes the step's agent-state repo and
 *     records the hub key so the follow-up deploy pack applies and verifies,
 *     but spawns nothing. The deployment-level `workflow` frame (sent once
 *     after every step is provisioned) spawns the child.
 * A frame carrying neither is rejected -- there is no in-process
 * fall-through. `workflow` and `provisionStep` are mutually exclusive.
 */
export const AgentDeployFrame = type({
  type: "'agent.deploy'",
  agentAddress: "string",
  agentId: "string",
  config: HarnessConfig,
  hubPublicKey: "string",
  "workflow?": AgentDeployWorkflow,
  "provisionStep?": "boolean",
});
export type AgentDeployFrame = typeof AgentDeployFrame.infer;

/**
 * Remove an agent from this sidecar. The sidecar tears down the harness,
 * pushes state to the hub (best-effort), deletes the agent directory, and
 * responds with agent.undeploy.ack.
 */
export const AgentUndeployFrame = type({
  type: "'agent.undeploy'",
  agentAddress: "string",
  reason: "string",
});
export type AgentUndeployFrame = typeof AgentUndeployFrame.infer;

/**
 * Per-address cryptographic challenge. The sidecar must sign
 * `nonce || utf8(address)` with each agent's private key and respond
 * with a challenge.response frame.
 */
export const ChallengeFrame = type({
  type: "'challenge'",
  challenges: type({ address: "string", nonce: "string" }).array(),
});
export type ChallengeFrame = typeof ChallengeFrame.infer;

/**
 * Sent when challenge verification fails for a specific address.
 */
export const ChallengeFailedFrame = type({
  type: "'challenge.failed'",
  address: "string",
  reason: "string",
});
export type ChallengeFailedFrame = typeof ChallengeFailedFrame.infer;

/**
 * Keepalive pong sent by the hub in response to a ping frame.
 * If the sidecar stops receiving pongs, it considers the hub dead.
 */
export const PongFrame = type({ type: "'pong'" });
export type PongFrame = typeof PongFrame.infer;

/**
 * Push an updated inference-source list to a running single-step
 * deployment. The sidecar routes it to the deployment's supervisor, which
 * delivers it to the warm agent and swaps its sources in place. `sources`
 * is non-empty (validated at this boundary, mirroring the deploy frame's
 * per-step source arrays). Element 0 is the active source; the producer
 * sets `defaultSource` to its id -- that equality is producer-enforced,
 * not checked here. Responds with session.ack or session.error.
 */
export const SourcesUpdateFrame = type({
  type: "'sources.update'",
  requestId: "string",
  agentAddress: "string",
  sources: InferenceSource.array().atLeastLength(1),
  defaultSource: "string",
});
export type SourcesUpdateFrame = typeof SourcesUpdateFrame.infer;

/**
 * Push refreshed credential material to a running deployment (a rotation, or a
 * revocation delivered by omitting the revoked credential's material so the
 * child evicts it). Mirrors `SourcesUpdateFrame`: the sidecar routes it to the
 * deployment's supervisor, which forwards it to the child's in-memory cell.
 */
export const CredentialsUpdateFrame = type({
  type: "'credentials.update'",
  requestId: "string",
  agentAddress: "string",
  delivery: CredentialDelivery,
});
export type CredentialsUpdateFrame = typeof CredentialsUpdateFrame.infer;

// ---------------------------------------------------------------------------
// Pack transport (bidirectional)
// ---------------------------------------------------------------------------
//
// Git pack data is streamed between hub and sidecar over the existing JSON
// WebSocket. Chunks are base64-encoded (matching the mail convention above).
// A transfer is a sequence of repo.pack.push frames followed by a
// repo.pack.done, correlated by transferId. The receiver responds with
// repo.pack.ack or repo.pack.reject.
//
// Each pack frame carries two complementary addressing fields:
//
//   - `agentAddress` identifies the destination agent on the receiving
//     sidecar. The sidecar manages per-agent state and uses this field to
//     route the pack to the correct workspace. For agent-state packs the
//     sidecar applies the pack onto the agent's deploy/state tree.
//
//   - `repoId` identifies the source repo at the hub. The hub maps `repoId`
//     to the originating entry in its kind-keyed RepoStore. For
//     `repoId.kind === "agent-state"`, `repoId.id` is the run address
//     (the deploy/state repo and the destination agent are the same), so
//     the two fields carry the same value. Future kinds (e.g. assets) use
//     `repoId` to name a non-agent source while `agentAddress` continues
//     to address the destination agent.
//
// Flow control: deferred. Agent deploy trees are small enough that the sender
// can push all chunks without windowing. If this becomes a problem, a credit-
// based mechanism can be added later.

/**
 * Tag identifying a kind of repository in the hub's kind-keyed RepoStore.
 * Lives in `@intx/types` because the wire-level pack frames reference it;
 * the substrate package re-exports it for handler authors.
 */
export const RepoKind = type.enumerated(
  "agent-state",
  "skill",
  "package-registry",
  "workflow",
  "workflow-run",
);
export type RepoKind = typeof RepoKind.infer;

/**
 * Operations a principal may invoke against a repo in the RepoStore.
 * Lives in `@intx/types` so storage layers (e.g. `@intx/db`) can validate
 * persisted action vocabularies without depending on the substrate
 * package. The substrate re-exports it for handler authors.
 */
export const RepoAction = type.enumerated(
  "init",
  "writeTree",
  "receivePack",
  "createPack",
  "resolveRef",
);
export type RepoAction = typeof RepoAction.infer;

/**
 * Hub-side identity of a repository in the RepoStore. Pack frames carry
 * this alongside `agentAddress` so the hub can map a pack back to the
 * originating repo independently of which sidecar/agent it is destined for.
 */
export const RepoId = type({
  kind: RepoKind,
  id: "string",
});
export type RepoId = typeof RepoId.infer;

/**
 * A chunk of git pack data. The sender splits the packfile into chunks of at
 * most 64 KiB (before base64 encoding) and sends them in order.
 *
 * `seq` is monotonically increasing per transferId, starting at 0. The
 * receiver must reject the transfer if a gap is detected.
 */
export const PackPushFrame = type({
  type: "'repo.pack.push'",
  agentAddress: "string",
  repoId: RepoId,
  transferId: "string",
  seq: "number",
  data: "string",
});
export type PackPushFrame = typeof PackPushFrame.infer;

/**
 * Signals the end of a pack transfer. The receiver applies the pack and
 * updates `ref` to point at `commitSha`. If the post-apply HEAD does not
 * match `commitSha`, the receiver must reject with reason "sha_mismatch".
 *
 * When `mountPath` is set, the receiver materializes the pack at
 * `workspace/<mountPath>/` instead of the hardcoded agent deploy tree.
 * Absent for agent-state deploy/state flows and workflow-run restoration.
 * The receiver distinguishes those paths by `repoId.kind`.
 */
export const PackDoneFrame = type({
  type: "'repo.pack.done'",
  agentAddress: "string",
  repoId: RepoId,
  transferId: "string",
  ref: "string",
  commitSha: "string",
  "mountPath?": "string",
});
export type PackDoneFrame = typeof PackDoneFrame.infer;

/**
 * Receiver acknowledges successful application of a pack transfer.
 */
export const PackAckFrame = type({
  type: "'repo.pack.ack'",
  agentAddress: "string",
  repoId: RepoId,
  transferId: "string",
});
export type PackAckFrame = typeof PackAckFrame.infer;

export const PackRejectReason = type.enumerated(
  "signature_invalid",
  "path_violation",
  "conflict",
  "corrupt",
  "sha_mismatch",
  "timeout",
);
export type PackRejectReason = typeof PackRejectReason.infer;

/**
 * Receiver rejects a pack transfer.
 */
export const PackRejectFrame = type({
  type: "'repo.pack.reject'",
  agentAddress: "string",
  repoId: RepoId,
  transferId: "string",
  reason: PackRejectReason,
});
export type PackRejectFrame = typeof PackRejectFrame.infer;

/**
 * Categories of deploy-apply failure surfaced by the sidecar's
 * tool-package loader. Each value maps one-to-one to a distinct point in
 * the apply pipeline; a single category fires per failed attempt.
 *
 *   tarball.missing          — a manifest entry's asset-sourced tarball
 *                              is not present at the recorded path.
 *   asset.mount.missing      — a `kind: "asset"` manifest entry names
 *                              an `assetId` that the deploy pack's
 *                              `deploy/asset-mounts.json` does not
 *                              cover. Indicates a mismatch between the
 *                              resolver's view of attached assets and
 *                              the materialization fan-out, not a
 *                              missing file on disk.
 *   integrity.mismatch       — fetched tarball bytes do not match the
 *                              manifest's pinned SRI integrity.
 *   registry.fetch.failed    — the configured registry refused or
 *                              dropped the request for a tarball.
 *   registry.unknown         — the manifest entry references a registry
 *                              name not present in the sidecar's
 *                              registry config.
 *   registry.auth.failed     — the registry rejected the sidecar's
 *                              credentials.
 *   tarball.extract.failed   — tar extraction failed or the extracted
 *                              tree was malformed.
 *   manifest.invalid         — the manifest itself did not validate
 *                              at the loader boundary (JSON.parse
 *                              failure or arktype schema failure).
 *                              Peer-dependency violations are caught
 *                              earlier by the hub's resolver and
 *                              surface as a launch failure rather
 *                              than this frame.
 *   package.entry.missing    — a top-level package's package.json had
 *                              no `interchange.tools` field.
 *   package.entry.invalid    — the resolved `interchange.tools` module
 *                              exported nothing that looked like an
 *                              AnnotatedToolFactory.
 *   factory.construct.failed — a factory invocation threw, or required
 *                              a capability key the env did not provide.
 *   tool.name.duplicate      — a tool name is registered more than
 *                              once in the apply's loaded set. The
 *                              cross-bundle case (two pinned packages
 *                              share a bundle id, producing colliding
 *                              prefixed tool names) is rejected at
 *                              apply time, before the caller commits.
 *                              The intra-bundle case (one package
 *                              exports two definitions sharing a raw
 *                              name) surfaces at first agent
 *                              construction with the same category
 *                              instead of apply rejection: the loader
 *                              cannot see `bundle.definitions` without
 *                              invoking the factory, and the `BaseEnv`
 *                              the factory needs is constructed by the
 *                              sidecar harness AFTER the commit. Both
 *                              paths carry the same category so the
 *                              operator-facing failure shape is
 *                              uniform regardless of which check
 *                              fired; only the channel (apply.error
 *                              frame vs runtime construct failure)
 *                              differs.
 *   apply.swap.failed        — DEPRECATED, no longer emitted. The apply
 *                              protocol stages each deploy into a stable
 *                              per-deploy-id directory and commits via a
 *                              single `active-deploy-id` file write, so
 *                              there is no filesystem rename that can
 *                              fail. The value is retained in the enum
 *                              for wire compatibility: during a rolling
 *                              upgrade an older sidecar can still emit
 *                              it, and dropping the member would make a
 *                              newer hub's frame validator reject that
 *                              frame.
 *   apply.previous-rotation.failed
 *                            — every loaded factory validated and the
 *                              new deploy was staged, but persisting the
 *                              instance's `active-deploy-id` file (the
 *                              commit) degraded: the id was written
 *                              through the no-fsync / dirty-marker
 *                              fallback ladder rather than durably
 *                              flushed. The new deploy is logically
 *                              live, so `previousDeployId` on this
 *                              failure carries the NEW deploy id rather
 *                              than the pre-apply one. The next boot
 *                              reconciles the recorded id from the dirty
 *                              marker.
 */
export const DeployApplyErrorCategory = type.enumerated(
  "tarball.missing",
  "asset.mount.missing",
  "integrity.mismatch",
  "registry.fetch.failed",
  "registry.unknown",
  "registry.auth.failed",
  "tarball.extract.failed",
  "manifest.invalid",
  "package.entry.missing",
  "package.entry.invalid",
  "factory.construct.failed",
  "tool.name.duplicate",
  "apply.swap.failed",
  "apply.previous-rotation.failed",
);
export type DeployApplyErrorCategory = typeof DeployApplyErrorCategory.infer;

/**
 * Hub requests the sidecar to push its current agent state. The sidecar
 * responds by sending pack.push frames followed by pack.done using the
 * same transferId.
 */
export const SyncRequestFrame = type({
  type: "'sync.request'",
  agentAddress: "string",
  transferId: "string",
});
export type SyncRequestFrame = typeof SyncRequestFrame.infer;

// ---------------------------------------------------------------------------
// Discriminated frame unions
// ---------------------------------------------------------------------------

/** All frame types the sidecar sends to the hub. */
export const SidecarFrame = RegisterFrame.or(ReconnectFrame)
  .or(ChallengeResponseFrame)
  .or(AgentDeployAckFrame)
  .or(AgentErrorFrame)
  .or(MailOutboundFrame)
  .or(AgentEventFrame)
  .or(ConnectorStateChangedFrame)
  .or(PingFrame)
  .or(SessionAckFrame)
  .or(SessionErrorFrame)
  .or(AgentUndeployAckFrame)
  .or(SignalCorrelationRegisterFrame)
  .or(PackPushFrame)
  .or(PackDoneFrame)
  .or(PackAckFrame)
  .or(PackRejectFrame)
  .or(MailInboundAckFrame);
export type SidecarFrame = typeof SidecarFrame.infer;

/** All frame types the hub sends to the sidecar. */
export const HubFrame = MailInboundFrame.or(AgentDeployFrame)
  .or(AgentUndeployFrame)
  .or(ChallengeFrame)
  .or(ChallengeFailedFrame)
  .or(PongFrame)
  .or(SourcesUpdateFrame)
  .or(CredentialsUpdateFrame)
  .or(PackPushFrame)
  .or(PackDoneFrame)
  .or(PackAckFrame)
  .or(PackRejectFrame)
  .or(SyncRequestFrame)
  .or(SignalDeliverFrame)
  .or(RunGrantsFrame)
  .or(SignalCorrelationRegisterAckFrame)
  .or(DrainDeliverFrame);
export type HubFrame = typeof HubFrame.infer;

/** Any frame on the wire, regardless of direction. */
export const WireFrame = SidecarFrame.or(HubFrame);
export type WireFrame = typeof WireFrame.infer;
