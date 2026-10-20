# AI Readiness Plan

*Drafted 2026-10-19 on branch `experimental-analysis`. Status: phase 1 in progress.*

Goal: prepare this repository so that AI assistants can fix bugs, build features and produce compatibility releases
without violating the two non-negotiable constraints:

1. **Backward compatibility.** Changes stay compatible with prior major versions of the plugin and of Elasticsearch.
   A breaking change stops the work; the human decides whether to proceed, gate it behind a feature switch, or take
   another approach.
2. **Mixed-cluster operation.** Elasticsearch and the plugin are upgraded by rolling restart. Nodes in one cluster
   run different patch and minor versions of both. Only a major upgrade may require a full cluster restart.

## Principles

- **Two axes of analysis.** Modules give orientation. Cross-cutting concerns (wire format, persisted schemas,
  thread-context headers, REST contracts, extension points) hold the invariants that the constraints depend on.
  The cross-cutting inventories are the primary product; module docs are secondary.
- **Short, claim-level, referenced.** Every statement points at a file and a symbol. No narrative that a refactor
  would invalidate. A document that cannot be verified is not written.
- **Human review gate.** AI-written analysis contains errors, and errors in these documents propagate into every
  later task. No document is trusted before a maintainer has reviewed it.
- **Lives in git, split by audience.** A short `CLAUDE.md` per module directory (loaded automatically when working
  there) and deeper pages under `docs/architecture/`. The root `CLAUDE.md` stays an index and a rulebook.
- **Generate what can be generated.** Anything that is a fact about the code (module list, dependency graph,
  routes, actions, config types, settings) is script output checked in CI, not prose.
- **Tests outrank documents.** An invariant that matters is encoded as a failing test wherever possible.
- **Volume is the enemy.** Anything that cannot be verified will drift, and a confident stale document is worse
  for an AI than no document.

## Prerequisites that are human decisions

- **Support matrix.** "Compatible with prior versions" is not decidable until it says compatible *with what*.
  Separate contracts with separate consumers: config documents in the index, REST API, transport messages between
  nodes, sgctl, the Kibana plugin, license keys, audit log record format, Signals watch format. One page listing,
  per contract, the oldest version that must keep working.
- **Halt protocol.** What the AI does when it suspects a breaking change, where it records the question, who answers.
- **Elasticsearch source checkout.** Clone the ES repository as a sibling directory (not inside this repo), with a
  worktree per pinned version (`9.5.5` for `main`, `8.19.23` for `main-es8`, plus the minor being prepared).
  Record the path and layout in the root `CLAUDE.md`. Local grep and `git diff vA..vB` over the packages the plugin
  touches beat GitHub fetches on speed and completeness; the `-sources.jar` files in `~/.m2` cover only the
  `server` artifact.
- **Toolchain alignment.** Local JDK and Maven should match CI (JDK 25 image; local was JDK 23 and Maven 3.6.3 on
  2026-10-19), so "works locally, fails in CI" does not become the dominant failure mode.

## Maintenance mechanisms

These keep the generated and written artefacts current as the code changes. Each is introduced in the phase named
in the last column and is then a standing requirement.

| Mechanism | Applies to | Where it lives | Phase |
|---|---|---|---|
| **Generator scripts.** Derived inventories are produced by scripts. CI regenerates and fails when the committed output differs, like a lockfile. | Module list, dependency graph, REST route catalogue, transport action list, `CType` registry, settings list, branch map | `dev/docs/` scripts, `ci/docs-check.yml` | 2 (scripts), 4 (CI gate) |
| **Docs lint.** A script walks every `path` and `path#Symbol` reference in `docs/` and all `CLAUDE.md` files and fails when the file or symbol no longer exists. References use symbols, not line numbers. | All written documents | `dev/docs/lint.py`, same CI job | 2 |
| **Freshness stamp and drift report.** Every document records the commit it was written against. A script counts commits touching the paths in the document's declared scope since that commit and prints a freshness table. | All written documents | Front matter `scope:` list in each doc; `dev/docs/drift.py` | 2 |
| **Source-to-doc map.** A CODEOWNERS-style file maps source directories and key classes to the documents describing them. The merge request pipeline reads the diff and posts the affected documents as a comment or checklist item. Advisory, not blocking. | All documents | `docs/architecture/DOCMAP`, MR pipeline stage | 4 |
| **Invariant tests.** The strongest invariants become tests that fail on silent change: the set of `Writeable` message classes, registered `CType` names, registered transport actions, REST routes, settings keys, each matched against a committed list that must be updated explicitly. | Wire surface, persisted schemas, REST surface, extension points | `security/src/test/.../inventory/` | 3 |
| **CLAUDE.md rule.** When a change touches an extension point, a serialized message, a persisted schema, a header, or a REST route, the corresponding inventory row and module doc are updated in the same change. | All documents | Root `CLAUDE.md`, "Non-negotiables" | 3 |
| **`docs-sync` skill.** Takes a diff, uses the source-to-doc map, and lists the documents and inventory rows the change affects, so the agent runs it before finishing a task. | All documents | `.claude/skills/docs-sync/` | 4 |
| **Scheduled doc review.** A weekly routine re-verifies the written analysis against current code and opens a merge request with proposed updates. Reviewed like any other MR. | Written analysis, module `CLAUDE.md` files | Claude Code scheduled routine; `.claude/skills/docs-review/` | 4 |
| **Per-branch regeneration.** Written documents travel with the existing backport labels across `main`, `main-es8` and release branches. Derived inventories are regenerated on each branch, never ported, because they differ. | Everything | Backport job, generator scripts | 4 |
| **Detail budget.** A document section that would need rewriting after an ordinary refactor is too detailed and is cut during review. | Written analysis | Review checklist in the module doc template | 2 |

## Phases

### Phase 0: prerequisites (human, short)

- Write the support matrix and the halt protocol.
- Clone Elasticsearch as a sibling; record path and worktree layout in the root `CLAUDE.md`.
- Align local JDK and Maven with CI.

Phase 1 does not depend on phase 0 and runs in parallel.

### Phase 1: cross-cutting inventories (AI, with review) — in progress

Six parallel passes, each one document under `docs/architecture/inventory/`, table-shaped, every claim referenced:

| Document | Covers |
|---|---|
| `build-and-dependencies.md` | Reactor, inter-module dependencies, profiles, plugin zip contents, version derivation, CI jobs |
| `extension-points.md` | ES plugin entry points, `SearchGuardModule` and the registry, implementation matrix, secondary extension interfaces, request pipeline order, settings |
| `wire-surface.md` | Transport actions, serialized messages and their version guards, thread-context headers, user serialization, CCS, transport authentication |
| `persisted-schemas.md` | Indices, `CType` registry, document schemas and unknown-field handling, config loading and propagation, on-disk artefacts, secrets, migrations |
| `rest-surface.md` | Route catalogue, REST entry and wrapping, body handling, authentication frontends, Kibana-facing and sgctl-facing contracts |
| `test-infrastructure.md` | `LocalCluster` modes, legacy harness, per-module test inventory, surefire and CI config, mixed-version test coverage, external integration tests |

Exit criteria: all six reviewed by a maintainer; contradictions between documents resolved; an index page
`docs/architecture/inventory/README.md` lists them with freshness stamps; the table sections that will become
generator output are marked as such.

### Phase 2: per-module analysis (AI, with review per module)

Dependency order: `support`, `ssl`, `security`, `scheduler`, `signals`, then `dlic-auditlog`, `dlic-dlsfls`,
`dlic-fe-multi-tenancy`, `dlic-security`, `dlic-signals`. `security` and `signals` each need more than one session.

Per module, two outputs from a fixed template. Every document, including the phase 1 inventories, opens with a
"Summary for maintainers" section in plain language (what it covers, what you need to know, what to review),
marked as written for humans; the tables below it stay authoritative, and the summary is the review surface:

- `<module>/CLAUDE.md`: one page. Purpose, entry points, extension points implemented, how to run its tests, the
  three things that break when touched carelessly, pointers to the deeper page.
- `docs/architecture/<module>.md`: invariants, data flow, interaction with other modules, open questions.

Maintenance work in this phase:

- Write the generator scripts for the derived inventories identified in phase 1, and replace those table sections
  with generated output.
- Write the docs lint and the drift report; add front matter with `commit:` and `scope:` to every document.
- Apply the detail budget during each module review.

### Phase 3: the rulebook

Synthesize phases 1 and 2 into the compatibility and mixed-cluster checklist: what to check before changing a
message, a config schema, a header, an index mapping, a REST response, a settings key. This becomes the
"Non-negotiables" section of the root `CLAUDE.md`, with the two constraints above verbatim, the support matrix,
the halt protocol, and the rule that inventory rows are updated in the same change.

Maintenance work in this phase:

- Add the invariant tests for serialized messages, `CType` names, transport actions, REST routes and settings keys.

### Phase 4: skills and automation

Project-local skills under `.claude/skills/`:

| Skill | Workflow |
|---|---|
| `compat-release-patch` | New ES patch: retag the existing commit, trigger and watch CI, update download links |
| `compat-release-minor` | New ES minor: `prepare-es-A.B.C` branch, diff ES tags over touched packages, adapt, merge, fork release branch from the previous one, cherry-pick, tag |
| `port-fix` | Cherry-pick a fix from `main` to `main-es8` and the live release branches using the backport labels |
| `write-cluster-test` | A test with `LocalCluster` and `TestSgConfig`, choosing embedded vs external process mode |
| `mixed-cluster-review` | Review a diff against the phase 3 checklist; produce halt-or-proceed with reasons |
| `es-api-diff` | Find what changed in Elasticsearch between two tags for a given package or class, using the local checkout |
| `docs-sync` | List documents and inventory rows affected by a diff |
| `docs-review` | Re-verify written analysis against code; used by the scheduled routine |

Automation:

- `ci/docs-check.yml`: regenerate derived inventories, run docs lint, print the drift report; fail on diff or
  broken reference.
- Merge request stage that posts affected documents from the source-to-doc map.
- A test-runner script that reads surefire reports so retried flaky tests are not masked as passes.
- A license-header check for new files, per module.
- A permission allowlist in `.claude/settings.json` for read-only Maven and git commands, so sessions are not
  interrupted by prompts.
- The weekly scheduled doc review routine.

### Phase 5: pilot on three real tasks

1. A compatibility release for the next ES patch (most mechanical; the ES 9 branch map already documents it).
2. A bug fix that touches a serialized message or a config document.
3. A small feature that adds a REST endpoint.

Record where the AI needed to ask, where it was wrong, which documents it used and which it ignored. Fold the
findings back into the documents, the skills and the checklist. Only after the pilot is the setup considered ready
for routine use.

## Related documents

- `ES9-BRANCH-MAP.md` (repo root): branching and tagging structure for the ES 9 line.
- `.gitlab/issue_templates/new_release_*.md`: the current release process; stale names (`master`, ES 7.17) to be
  fixed in phase 4.
- `docs/architecture/inventory/`: phase 1 output.
