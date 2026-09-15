import type { WsHandle } from "@intx/hub-sessions";

export function createMockWs(): WsHandle & {
  sent: string[];
  closed: boolean;
} {
  return {
    sent: [],
    closed: false,
    send(data: string) {
      this.sent.push(data);
    },
    close() {
      this.closed = true;
    },
  };
}
