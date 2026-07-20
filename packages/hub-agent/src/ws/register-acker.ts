import { getLogger } from "@intx/log";
import type { SignalCorrelationRegisterFrame } from "@intx/types/sidecar";

const logger = getLogger(["interchange", "hub-agent", "ws", "register-acker"]);

/**
 * Per-attempt watchdog for one register frame. Tight -- a single frame plus one
 * DB upsert, not a child enumerating runs -- so retries cover the
 * connected-but-lost-frame window without lingering.
 */
export const DEFAULT_REGISTER_ACK_TIMEOUT_MS = 2_000;

/**
 * Total sends before giving up (the initial send plus retries). On exhaustion
 * the acker stops and logs: the correlation is not lost, because the next
 * re-establishment (child respawn/recycle, hub reconnect) re-registers the whole
 * parked set from durable state.
 */
export const DEFAULT_REGISTER_ACK_MAX_ATTEMPTS = 3;

type PendingRegister = {
  frame: SignalCorrelationRegisterFrame;
  attempts: number;
  timer: ReturnType<typeof setTimeout>;
};

export type RegisterAckerConfig = {
  /**
   * Put a register frame on the wire. Called for the initial send and each
   * retry; the acker never touches the socket itself, so the link's normal
   * `send` (queue-on-disconnect) semantics are preserved.
   */
  sendFrame: (frame: SignalCorrelationRegisterFrame) => void;
  /**
   * True only when the link is OPEN. The acker abandons a pending retry the
   * moment the link is not open: re-sending onto a fresh, not-yet-challenged
   * socket would land "unrouted", and the reconnect re-emit re-registers the
   * whole parked set anyway.
   */
  isOpen: () => boolean;
  timeoutMs?: number;
  maxAttempts?: number;
};

/**
 * Reliable-resend helper for `signal.correlation.register` frames, modelled on
 * the pack sender's pending-ack machine. A register is fire-and-forget on the
 * wire and can be lost on an open socket or evicted from the link's bounded send
 * queue; without an ack the parked run is never registered and cannot be
 * approved. This tracks each register until the hub's
 * `signal.correlation.register.ack` lands, re-sending on a tight watchdog, and
 * gives up (leaving recovery to the next re-establishment) rather than retrying
 * across a disconnect.
 */
export interface RegisterAcker {
  /**
   * Send a register frame and track it until acked or abandoned. A second send
   * for a correlationId already pending refreshes the frame and resets the
   * watchdog rather than arming a second one -- the initial park, a
   * respawn/reconnect re-emit, and a retry all carry the same correlationId and
   * drive the same idempotent co-write, so one pending entry per correlation is
   * correct.
   */
  send(frame: SignalCorrelationRegisterFrame): void;
  /** Settle the pending retry for this correlationId; false if none was pending. */
  handleAck(correlationId: string): boolean;
  /**
   * Abandon every pending retry without re-sending or re-acking. Called on link
   * close and on the reconnect open edge, symmetric with the ping timer and the
   * pack sender's `cancelAll`, so no timer leaks and no retry fires onto a dead
   * or not-yet-challenged socket.
   */
  cancelAll(): void;
}

export function createRegisterAcker(
  config: RegisterAckerConfig,
): RegisterAcker {
  const timeoutMs = config.timeoutMs ?? DEFAULT_REGISTER_ACK_TIMEOUT_MS;
  const maxAttempts = config.maxAttempts ?? DEFAULT_REGISTER_ACK_MAX_ATTEMPTS;
  const pending = new Map<string, PendingRegister>();

  function schedule(correlationId: string): ReturnType<typeof setTimeout> {
    return setTimeout(() => onTimeout(correlationId), timeoutMs);
  }

  function onTimeout(correlationId: string): void {
    const entry = pending.get(correlationId);
    if (entry === undefined) return;
    // Abandon the moment the link is not open -- the reconnect re-emit owns
    // recovery from here, and a resend onto a fresh socket would land unrouted.
    if (!config.isOpen()) {
      pending.delete(correlationId);
      return;
    }
    if (entry.attempts >= maxAttempts) {
      pending.delete(correlationId);
      logger.warn`signal.correlation.register for ${correlationId} unacked after ${String(maxAttempts)} attempts; leaving recovery to the next re-establishment`;
      return;
    }
    entry.attempts += 1;
    config.sendFrame(entry.frame);
    entry.timer = schedule(correlationId);
  }

  function send(frame: SignalCorrelationRegisterFrame): void {
    const existing = pending.get(frame.correlationId);
    if (existing !== undefined) {
      clearTimeout(existing.timer);
    }
    config.sendFrame(frame);
    pending.set(frame.correlationId, {
      frame,
      attempts: 1,
      timer: schedule(frame.correlationId),
    });
  }

  function handleAck(correlationId: string): boolean {
    const entry = pending.get(correlationId);
    if (entry === undefined) return false;
    clearTimeout(entry.timer);
    pending.delete(correlationId);
    return true;
  }

  function cancelAll(): void {
    for (const entry of pending.values()) {
      clearTimeout(entry.timer);
    }
    pending.clear();
  }

  return { send, handleAck, cancelAll };
}
