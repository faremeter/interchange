export PATH := $(PWD)/node_modules/.bin:$(PWD)/bin:$(PATH)

# Internal bun runs resolve @intx/* to TypeScript source via the intx-src
# exports condition; external consumers fall through to the compiled dist.
BUN := bun --conditions=intx-src

all: lint build build-admin-ui test

build: FORCE
	tsc -b --noEmit --force

build-admin-ui: FORCE
	cd apps/admin-ui && vite build

lint: FORCE
	prettier -c .
	eslint --cache .
	$(BUN) bin/gen-api-docs.ts --check
	$(BUN) bin/check-deps.ts
	$(BUN) bin/check-launchers.ts
	$(BUN) bin/exports-shape.ts
	$(BUN) bin/publish-metadata.ts

test: FORCE
	$(BUN) test packages/ apps/ bin/ tests/agent/ tests/agent-audit-log/ tests/agent-blob-spill/ tests/agent-common/ tests/agent-multi-provider/ tests/agent-quickstart/ tests/agent-resume/ tests/agent-rewind/ tests/agent-rich-tool/ tests/agent-structured-payload/ tests/coding-agent/ tests/hub-agent/lib/ tests/inference-testing/ tests/tool-packaging/ tests/workflow/
	$(BUN) test --timeout 120000 tests/workflow-deploy/multistep-signal.test.ts tests/workflow-deploy/hub-link-reconnect.test.ts tests/workflow-deploy/interrupted-pack-reconnect-recovery.test.ts tests/workflow-deploy/deploy-window-reconnect-recovery.test.ts tests/workflow-deploy/midrun-signal-survives-reconnect.test.ts tests/workflow-deploy/multistep-perstep-reroute-reconnect.test.ts tests/workflow-deploy/crash-restart-reconnect-resumes.test.ts tests/workflow-deploy/single-step-real-agent.test.ts tests/workflow-deploy/map-fan-out-real-agent.test.ts tests/workflow-deploy/instance-reroute-real-agent.test.ts tests/workflow-deploy/instance-failover-real-agent.test.ts tests/workflow-deploy/multistep-signed-send.test.ts tests/workflow-deploy/single-step-message-input.test.ts tests/workflow-deploy/single-step-posix-tool.test.ts tests/workflow-deploy/single-step-grants-bridge.test.ts tests/workflow-deploy/single-step-pinned-ask-tool.test.ts tests/workflow-deploy/single-step-per-run-grants.test.ts tests/workflow-deploy/approval-tracer-roundtrip.test.ts tests/workflow-deploy/mail-trigger-derived-grants-roundtrip.test.ts tests/workflow-deploy/single-step-event-threading.test.ts tests/workflow-deploy/single-step-conversation-durability.test.ts tests/workflow-deploy/per-level-pipeline-real-agents.test.ts tests/workflow-deploy/single-step-full-lifecycle.test.ts tests/workflow-deploy/cross-process-custom-adapter.test.ts tests/workflow-deploy/conversation-state-wal.test.ts tests/workflow-deploy/drain-roundtrip.test.ts tests/workflow-deploy/child-workflow-roundtrip.test.ts tests/workflow-deploy/child-workflow-inherited-grants-roundtrip.test.ts tests/workflow-deploy/unresolvable-director.test.ts tests/workflow-deploy/fifo-mail.test.ts tests/workflow-deploy/mail-edge-cases.test.ts tests/inference/ tests/skill-attachment-flow.test.ts tests/hub-api/ tests/db/

test-load: FORCE
	$(BUN) test --timeout 300000 tests/workflow-deploy/fifo-mail-load.test.ts

verify-tool-load: FORCE
	bun bin/verify-tool-load.ts

format: FORCE
	prettier -w .

docs: FORCE
	$(BUN) bin/gen-api-docs.ts

builtins: FORCE
	$(BUN) run bin/build-builtins.ts

publish-builtins: builtins
	$(BUN) bin/publish-tool-packages.ts --registry workspace-builtins --from dist/builtins

clean:
	rm -f .env-checked .eslintcache
	find . -type f -name tsconfig.tsbuildinfo -a ! -path '*/node_modules/*' | xargs rm -f
	find . -type d -name dist -a ! -path '*/node_modules/*' | xargs rm -rf

.env-checked: bin/check-env
	./bin/check-env
	touch .env-checked

include .env-checked

.PHONY: all build build-admin-ui lint test test-load verify-tool-load format docs clean builtins publish-builtins
FORCE:
