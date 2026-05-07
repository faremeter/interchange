---
name: doc-audit
description: Audit documentation against implementation to find mismatches, stale references, missing coverage, and generated file drift
---

# Doc Audit

Systematic comparison of documentation against implementation. Finds things that are documented but wrong, documented but not implemented, and implemented but not documented. Focuses on docs that claim to describe current state -- aspirational or target-architecture docs are noted but not flagged as problems.

## When to Use

Run this skill periodically or after significant refactoring. It is a read-only audit that produces a report. It does not fix anything -- it identifies what needs fixing and categorizes the work.

## Phase 1: Inventory

Build a complete picture of what documentation exists and what it claims to cover.

### Documentation Discovery

Search for documentation files:

1. Repository root: README, AGENTS.md, CONVENTIONS.md, LAYOUT.md, DEV.md, and any other markdown files
2. `docs/` directory: all markdown files
3. Generated documentation: files with "generated" markers, autogeneration scripts, or entries in `.prettierignore`/`.gitignore` that suggest generated output

For each document, classify it:

| Classification    | Meaning                                      | Audit Treatment                                       |
| ----------------- | -------------------------------------------- | ----------------------------------------------------- |
| **Current-state** | Claims to describe how things work right now | Full audit -- mismatches are bugs                     |
| **Aspirational**  | Describes a target or planned architecture   | Note gaps vs. current state, but don't flag as errors |
| **Generated**     | Produced by a script or build step           | Audit the generation pipeline, not the content        |
| **Process**       | Development workflow, setup instructions     | Verify commands and paths work                        |

Present the inventory to the user before proceeding. Let them correct any misclassifications.

### Code Discovery

Identify the major structural elements of the codebase:

1. **Packages/modules**: list all packages, their names, and a one-line summary of purpose
2. **Entry points**: app wiring, route mounting, plugin registration
3. **Public API surface**: HTTP routes, CLI commands, exported functions, wire protocols
4. **Generated artifacts**: OpenAPI specs, API docs, type definitions, schema files
5. **Build/test pipeline**: what commands exist, what they produce

## Phase 2: Cross-Reference

Work through each document systematically. For each claim a document makes, verify it against the implementation.

### Route and API Auditing

For projects with HTTP APIs:

1. **Mount point composition**: trace each route file from its internal path registration through to its mount point in the app wiring. Verify the composed path matches what documentation claims. Watch for path segment doubling (route registers `/foo` but is mounted at `.../foo`, producing `.../foo/foo`).

2. **Endpoint inventory**: compare documented endpoints against actually registered routes. Check method, path, request body shape, response shape, and status codes.

3. **Generated API docs**: if API documentation is generated from code (OpenAPI, JSDoc, etc.), check whether the generated output matches the checked-in documentation. If there's drift, the generation pipeline or the checked-in file is stale.

4. **Manual annotations in generated files**: search generated documentation for hand-edited content (notes, warnings, explanations added after generation). These will be lost on regeneration and should be migrated into the generation source (route metadata, type descriptions, etc.).

### Package and Module Auditing

1. **Listed vs. actual**: compare package listings in documentation against the filesystem. List all directories in the packages/modules location (e.g., `packages/`, `lib/`, `src/modules/`), then diff against every package mentioned in documentation. Flag both directions: packages that exist but aren't documented, and packages that are documented but don't exist.

2. **Descriptions**: for each documented package, read its entry point (main export file, `index.ts`, `__init__.py`, etc.) and compare the actual exports and behavior against the documented description. A package described as "logging abstraction" that also handles metrics has a stale description.

3. **Dependency claims**: if documentation describes dependency rules or layering constraints (e.g., "runtime packages never import from hub packages"), verify by searching for violating imports. Check at least 3-5 packages across different layers. Use `grep` for import/require statements that cross the documented boundaries.

### Configuration and Setup Auditing

1. **Commands**: verify that documented commands (`bin/` scripts, `bun run` scripts, etc.) actually exist and have the described behavior. Check that flags and arguments mentioned in docs match what the script accepts.

2. **Environment variables**: collect env vars from all documentation sources -- `.env.example` files, README sections, setup guides, deployment docs, and inline comments in config loaders. Then search the codebase for actual env var reads (`process.env.X`, `Bun.env.X`, `os.environ`, `env::var`, `os.Getenv`, etc., depending on the stack). Diff the two lists. Flag documented vars that are never read (stale docs) and read vars that are never documented (missing docs).

3. **Prerequisites**: check that listed prerequisites (runtime versions, tools, services) match what the code requires. Look at lock files, engine fields in `package.json`, CI configs, and Dockerfiles for version constraints that may contradict documentation.

### Feature and Behavior Auditing

1. **Documented features**: for each feature described in current-state docs, verify the implementation exists. To detect stubs, look for: hardcoded return values (e.g., returning an empty array or a fixed string), TODO/FIXME comments near the handler, response bodies that don't query any data source, and status fields derived from unrelated data (e.g., returning "healthy" based on a definition's status rather than actual instance health).

2. **Undocumented features**: scan for significant functionality that isn't mentioned in any documentation. Check route handlers, exported public APIs, CLI commands and subcommands, middleware, and scheduled/background jobs. Not every internal helper needs documentation, but anything a user or API consumer would interact with should appear somewhere.

3. **Behavioral claims**: if docs describe specific behavior (error handling, validation rules, security model, ordering guarantees), read the implementation and verify it matches. Pay attention to edge cases the docs mention explicitly -- these are the most likely to have drifted.

### Inline Documentation Auditing

Source code often carries its own documentation layer: JSDoc comments, docstrings, type-level descriptions, and annotated examples. These drift in the same ways as standalone docs.

1. **Signature drift**: search for functions with JSDoc `@param` or `@returns` annotations and compare the documented parameter names and types against the actual function signature. Parameters that have been renamed, retyped, added, or removed without updating the JSDoc are stale.

2. **Deprecated markers**: search for `@deprecated` annotations or equivalent markers. Verify the deprecated item is actually slated for removal and that a replacement is documented. Flag deprecated items that are still actively called with no migration path.

3. **Type-level descriptions**: if the project uses a schema system that supports descriptions (OpenAPI, ArkType `.describe()`, Zod `.describe()`, JSON Schema `description`, Python dataclass docstrings), check whether types that carry non-obvious semantics have descriptions. Fields like `sessionId`, `creatorId`, or `availability` often need context that isn't obvious from the name alone.

## Phase 3: Report

Organize findings into categories. Each finding should include:

- **What**: the specific mismatch
- **Where**: file and line in both the doc and the code
- **Severity**: how likely this is to mislead someone

### Categories

**Incorrect documentation** (highest priority): docs that actively contradict the implementation. Someone following these docs will do the wrong thing.

- Wrong paths, URLs, or endpoints
- Wrong parameter names or types
- Wrong behavioral descriptions
- Stale references to renamed or moved things

**Missing documentation** (medium priority): significant functionality with no documentation coverage.

- Route handlers with no mention in any doc
- Packages with no entry in package listings
- Configuration options that aren't documented

**Missing implementation** (medium priority): documented features that don't exist in code, excluding aspirational docs.

- Endpoints described in current-state docs but not implemented
- Stub implementations pretending to be real features

**Generated file drift** (medium priority): generated documentation that has been hand-edited or is stale.

- Manual content in generated files
- Generated files that don't match what the generation script produces
- Missing "do not edit" markers on generated files

**Stale aspirational docs** (low priority): target architecture docs that are significantly behind or ahead of current state. Not errors, but worth noting for the maintainer's awareness.

### Output Format

Present findings as a table grouped by category, with the highest-priority items first:

```
## Incorrect Documentation

| Finding | Doc | Code | Notes |
|---|---|---|---|
| Route path doubled by mount composition | README.md:45 | app.ts:80 | Mount at /widgets + route at /widgets = /widgets/widgets |
| Stale endpoint in README | README.md:112 | routes/items.ts:30 | Doc says POST /items, actual path is /items/drafts |

## Missing Documentation

| Finding | Code Location | Notes |
|---|---|---|
| analytics package not in architecture doc | packages/analytics/ | Event tracking and aggregation pipeline |

## Generated File Drift

| Finding | Doc | Script | Notes |
|---|---|---|---|
| Hand-edited notes in generated API reference | docs/API.md:320 | bin/gen-docs.ts | 4 manual Note: paragraphs will be lost on regeneration |

...
```

After presenting the report, ask the user how they want to proceed:

- Which items should be fixed immediately?
- Which items should be filed as issues?
- Which items are acceptable as-is?

## Guidelines

**Don't fix things during the audit.** The audit produces a report. Fixes are separate work, potentially with their own commits, reviews, and issue tracking.

**Verify before reporting.** Don't flag something as a mismatch based on a keyword search. Read the doc claim, read the code, and confirm the discrepancy. False positives erode trust in the audit.

**Respect the doc's intent.** A document describing a target architecture is not "wrong" because the code hasn't caught up. Classify correctly and report accordingly.

**Check composition, not just existence.** Many bugs come from things that individually look correct but compose incorrectly (route mounting, config layering, type re-exports). Trace the full path from definition to use.

**Look for hand edits in generated files.** Search for patterns like `Note:`, `TODO:`, `WARNING:`, or content that doesn't match the generation template's style. These are often the most insidious form of drift because they represent knowledge that will be silently lost.

## Acknowledgment

After reviewing this skill, state: "I have reviewed the doc-audit skill."
