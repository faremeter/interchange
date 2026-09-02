import { describe, test, expect } from "bun:test";

import type {
  CredentialBindingDescriptor,
  CredentialDelivery,
  CredentialMaterialEntry,
} from "@intx/types/sidecar";

import { mergeCredentialDelivery } from "./credential-cell";

function material(
  credentialId: string,
  secret: string,
  overrides: Partial<CredentialMaterialEntry> = {},
): CredentialMaterialEntry {
  return {
    credentialId,
    providerKey: "prov",
    origin: "https://api.example.com",
    secret,
    ...overrides,
  };
}

function binding(
  handle: string,
  credentialId: string,
  consumer: string,
): CredentialBindingDescriptor {
  return { handle, credentialId, consumer };
}

function delivery(
  materials: CredentialMaterialEntry[],
  bindings: CredentialBindingDescriptor[] = [],
): CredentialDelivery {
  return { bindings, materials };
}

describe("mergeCredentialDelivery", () => {
  test("upserts a material into an empty cell", () => {
    const next = mergeCredentialDelivery(
      null,
      delivery([material("c1", "sk-1")]),
      undefined,
    );
    expect(next.materials).toEqual([material("c1", "sk-1")]);
    expect(next.bindings).toEqual([]);
  });

  test("a partial delivery upserts without evicting a sibling credential", () => {
    // The clobber the unified cell introduced: an inference rotation carrying
    // only its own material must not drop a live-pushed tool credential.
    const current = delivery(
      [material("tool-cred", "sk-tool")],
      [binding("cred", "tool-cred", "@intx/pkg-a")],
    );
    const next = mergeCredentialDelivery(
      current,
      delivery([material("infer-cred", "sk-infer")]),
      undefined,
    );
    expect(next.materials).toEqual([
      material("tool-cred", "sk-tool"),
      material("infer-cred", "sk-infer"),
    ]);
    expect(next.bindings).toEqual([
      binding("cred", "tool-cred", "@intx/pkg-a"),
    ]);
  });

  test("a rotation replaces a credential's secret in place", () => {
    const current = delivery([material("c1", "sk-old")]);
    const next = mergeCredentialDelivery(
      current,
      delivery([material("c1", "sk-new")]),
      undefined,
    );
    expect(next.materials).toEqual([material("c1", "sk-new")]);
  });

  test("the same handle under two consumers coexists", () => {
    const current = delivery(
      [material("c1", "sk-1")],
      [binding("creds", "c1", "@intx/pkg-a")],
    );
    const next = mergeCredentialDelivery(
      current,
      delivery(
        [material("c2", "sk-2")],
        [binding("creds", "c2", "@intx/pkg-b")],
      ),
      undefined,
    );
    expect(next.bindings).toEqual([
      binding("creds", "c1", "@intx/pkg-a"),
      binding("creds", "c2", "@intx/pkg-b"),
    ]);
  });

  test("rebinding the same (consumer, handle) replaces the binding entry but leaves the old material", () => {
    // A rebind upserts the binding; the now-unreferenced material lingers.
    // Merge cannot garbage-collect it -- inference materials never carry a
    // binding, so "unreferenced" is not a drop signal. Only an explicit
    // `revoke` removes a material.
    const current = delivery(
      [material("c1", "sk-1"), material("c2", "sk-2")],
      [binding("creds", "c1", "@intx/pkg-a")],
    );
    const next = mergeCredentialDelivery(
      current,
      delivery([], [binding("creds", "c2", "@intx/pkg-a")]),
      undefined,
    );
    expect(next.bindings).toEqual([binding("creds", "c2", "@intx/pkg-a")]);
    expect(next.materials).toEqual([
      material("c1", "sk-1"),
      material("c2", "sk-2"),
    ]);
  });

  test("revoke drops a credential's material and every binding referencing it", () => {
    const current = delivery(
      [material("c1", "sk-1"), material("c2", "sk-2")],
      [
        binding("cred-a", "c1", "@intx/pkg-a"),
        binding("cred-b", "c1", "@intx/pkg-b"),
        binding("cred-c", "c2", "@intx/pkg-c"),
      ],
    );
    const next = mergeCredentialDelivery(current, delivery([]), ["c1"]);
    expect(next.materials).toEqual([material("c2", "sk-2")]);
    expect(next.bindings).toEqual([binding("cred-c", "c2", "@intx/pkg-c")]);
  });

  test("a revoke and re-add of the same credentialId resolves to the re-add", () => {
    const current = delivery([material("c1", "sk-old")]);
    const next = mergeCredentialDelivery(
      current,
      delivery([material("c1", "sk-new")]),
      ["c1"],
    );
    expect(next.materials).toEqual([material("c1", "sk-new")]);
  });

  test("revoking a credentialId in the same frame that re-adds a binding for it keeps the binding", () => {
    // revoke runs before upsert: revoke drops c1's old binding, then the
    // delivery's binding for c1 is added back.
    const current = delivery(
      [material("c1", "sk-old")],
      [binding("old", "c1", "@intx/pkg-a")],
    );
    const next = mergeCredentialDelivery(
      current,
      delivery(
        [material("c1", "sk-new")],
        [binding("new", "c1", "@intx/pkg-a")],
      ),
      ["c1"],
    );
    expect(next.materials).toEqual([material("c1", "sk-new")]);
    expect(next.bindings).toEqual([binding("new", "c1", "@intx/pkg-a")]);
  });

  test("revoke drops several credentialIds at once", () => {
    const current = delivery(
      [material("c1", "sk-1"), material("c2", "sk-2"), material("c3", "sk-3")],
      [
        binding("cred-a", "c1", "@intx/pkg-a"),
        binding("cred-b", "c2", "@intx/pkg-b"),
        binding("cred-c", "c3", "@intx/pkg-c"),
      ],
    );
    const next = mergeCredentialDelivery(current, delivery([]), ["c1", "c3"]);
    expect(next.materials).toEqual([material("c2", "sk-2")]);
    expect(next.bindings).toEqual([binding("cred-b", "c2", "@intx/pkg-b")]);
  });

  test("revoking an absent credentialId is a no-op", () => {
    const current = delivery([material("c1", "sk-1")]);
    const next = mergeCredentialDelivery(current, delivery([]), ["c2"]);
    expect(next.materials).toEqual([material("c1", "sk-1")]);
  });
});
