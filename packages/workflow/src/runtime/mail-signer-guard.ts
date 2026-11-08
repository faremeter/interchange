// A mail signer that works only while a window is open.
//
// The same two windows guard it as guard an action's credentials: the
// invoker's view closes when the invocation settles, and each `perform`
// scope closes when its `run` settles. A signer kept past its `run`, or used
// by a `perform` the handler left pending, fails closed.

import type { MailSigner } from "@intx/types";

/**
 * Wrap `signer` so `compose` refuses with `refusal()` once `isOpen` returns
 * false.
 */
export function guardMailSigner(
  signer: MailSigner,
  isOpen: () => boolean,
  refusal: () => string,
): MailSigner {
  return {
    compose(message) {
      return isOpen()
        ? signer.compose(message)
        : Promise.reject(new Error(refusal()));
    },
  };
}
