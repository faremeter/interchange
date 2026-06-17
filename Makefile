export PATH := $(PWD)/bin:$(PATH)

all: lint build test

build: FORCE
	bun run check

lint: FORCE
	bun run lint

test: FORCE
	bun run test

test-load: FORCE
	bun run test:load

format: FORCE
	bun run format

docs: FORCE
	bun run docs

builtins: FORCE
	bun run bin/build-builtins.ts

publish-builtins: builtins
	bun bin/publish-tool-packages.ts --registry workspace-builtins --from dist/builtins

clean:
	rm -f .env-checked .eslintcache
	find . -type f -name tsconfig.tsbuildinfo -a ! -path '*/node_modules/*' | xargs rm -f
	find . -type d -name dist -a ! -path '*/node_modules/*' | xargs rm -rf

.env-checked: bin/check-env
	./bin/check-env
	touch .env-checked

include .env-checked

.PHONY: all build lint test test-load format docs clean builtins publish-builtins
FORCE:
