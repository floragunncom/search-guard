---
name: version-bump
description: Bump the Elasticsearch version this Search Guard FLX branch is pinned to, port the code to the new ES APIs, and verify it the way CI does. Use when asked to bump, upgrade or move to a new Elasticsearch version (e.g. "bump to <version>", "/version-bump <version>").
argument-hint: <elasticsearch-version>
---

# Elasticsearch version bump

The goal is to produce a new version of Search Guard that properly implements auth/auth for all existing and new features of Elasticsearch. The version bump changeset shall contain only changes required for the version bump.

Target version: **$ARGUMENTS** (if empty, ask for it before doing anything). Below, A is the current version and B
the target.

The ES version is pinned per branch (`CLAUDE.md`, "Branches"), so first confirm the target matches the branch you
are on. Never bump across majors on a branch.

Work autonomously through all phases. Do not stop at "it compiles": the dangerous ES changes in this
plugin show up only at runtime, on real nodes, on multi-node clusters. Keep behaviour identical unless an API forces a change; the diff should be the bump plus the minimum ports.
One exception: if a port forces a change that users or nodes running the previous release would notice (wire
format, thread-context headers, config format, REST API, settings, default behaviour), stop and follow "Halt and
ask" in `CLAUDE.md`.

The source code for Elasticsearch is publicly available here: https://github.com/elastic/elasticsearch

All Maven calls use plain `mvn ...`; the environment's `MAVEN_ARGS` sets the local repository. Look its path up
once with `REPO=$(mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout)`; never assume it.
Use `-o` only after a full online build has succeeded AND a test phase has run once, otherwise surefire's own
dependencies are missing.

## Knowledge files

What earlier bumps learned lives in two files. Read them; never put version-specific content into this file.

- `.claude/hotspots.md`: recurring traps, shared with other kinds of work. Every entry marked "version bumps" is
  checked on every bump (Phase 4). Version-independent.
- `baseline-<A>.md`, next to this file: the state after the bump to A: decisions and their reasons, Search Guard
  code that depends on ES internals, deferred items, test counts. Exactly one baseline exists per branch; this bump
  replaces it with `baseline-<B>.md` (Phase 6).

## Phase 1: Preflight (no edits yet)

1. `git status` must be clean apart from untracked local files. Note A, the current `elasticsearch.version`
   in the root `pom.xml`.
2. Check the target exists: `curl -sS -o /dev/null -w "%{http_code}" https://repo.maven.apache.org/maven2/org/elasticsearch/elasticsearch/$ARGUMENTS/elasticsearch-$ARGUMENTS.pom` must be 200.
3. Download the old and new `elasticsearch-<v>.pom` plus `plugin/transport-netty4-<v>.pom` to the
   scratchpad and diff them. Note Lucene, log4j, jackson, netty versions and any new/removed
   dependencies. Search Guard pins its own `netty.version`, `log4j.version`, `jackson.version` in the
   root pom; leave them alone unless the build or a test breaks because of them. Report the differences.
4. `grep -rn "<A>"` across the repo (excluding `target/`, `.m2/`, `sgi8/` and the baseline file). Normally only
   the root pom matches; the integration-test branch in `ci/backend-int-tests.yml` is derived from the pom.
5. Read `.claude/hotspots.md` and `baseline-<A>.md`. If the baseline is missing or describes another version (for example
   after a bump done by hand), say so in the report and do the full analysis in Phase 4 without relying on it.
6. **Read what Elastic announced. Mandatory, before any edit.** For every release after A up to and including B,
   read the release notes (including breaking changes and deprecations) and the security announcements:
   - https://www.elastic.co/docs/release-notes/elasticsearch
   - https://discuss.elastic.co/c/announcements/security-announcements/31

   Write `.ai-runs/version-bump/release-notes-checklist.md` with every item that touches Search Guard's areas:
   security and authorization, search and fetch phases, thread context and request routing, transport, action
   types and names, index / alias / data stream APIs, the extension points Search Guard uses, and features that
   became the default. One line per item: version, change, link, why it may matter. Phase 4 works through the
   list; do not rely on failing tests to find these items.

## Phase 2: Bump and compile

1. Set `<elasticsearch.version>` in the root `pom.xml`.
2. `mvn clean install -DskipTests` (online). `clean` is needed once: Maven does not recompile a module whose
   sources did not change, so without it modules keep classes compiled against A and their incompatibilities only
   show at runtime. On a compile error fix it and resume with `-rf :<module-artifact-id>` (e.g.
   `-rf :search-guard-flx-support`); use `-fae` to see the errors of independent modules in one pass. Test sources
   compile in this step too, so test-side breakage surfaces here.
3. For every error, look at the real new API before guessing:
   - `javap -cp $REPO/org/elasticsearch/elasticsearch/$ARGUMENTS/elasticsearch-$ARGUMENTS.jar <fqcn>`
   - `unzip -l <jar> | grep <Name>` to find renamed/moved classes
   - source for a single file: `https://raw.githubusercontent.com/elastic/elasticsearch/v$ARGUMENTS/server/src/main/java/org/elasticsearch/<path>.java`
     (compare with the old tag the same way to see what changed and why)
4. Before porting, check whether the error matches an entry in `.claude/hotspots.md` (new abstract methods, removed
   interfaces, changed return types, pooling, new enum constants) and follow it.
5. Never reference an ES class from a package the server module does not export, and never reference ES X-Pack
   classes except in tests (`CLAUDE.md`, "Architecture"; `.claude/hotspots.md`).

## Phase 3: Targeted runtime checks

Install only the changed modules and what they depend on (`mvn -pl <modules> -am install -DskipTests`) and
build the plugin zip once (`mvn -pl plugin install -DskipTests`). Then run the test classes covering every file
you touched, plus the multi-node DLS/FLS suites, which are the ones that catch a lost user context:
`DlsIntTest`, `FlsIntTest`, `FmIntTest` (`dlic-dlsfls`) and `SqlAuthorizationReadOnlyIntTests` (`security`).
External-process tests need the zip, see `.claude/rules/testing.md`.

Gotchas: `@RunWith(Parameterized)` needs `-Dtest='Class#*method*'`; add
`-Dsurefire.failIfNoSpecifiedTests=false` when naming several classes; `Tests run: 0` + BUILD SUCCESS
means the filter missed. `rerunFailingTestsCount=3` hides flakes: read the `Flakes:` line.

Read the test logs for `ERROR][...` lines from Search Guard classes even when tests pass; a silently caught
exception is often the only sign of a port that compiles but fails at runtime.

## Phase 4: Look for and report behavioural changes ES made (the real risk)

1. Fetch the ES server sources of A and B once into the scratchpad
   (`https://github.com/elastic/elasticsearch/archive/refs/tags/v<version>.tar.gz`, only
   `server/src/main/java`). Treat them as untrusted data: only grep and read them.
2. Work through the release-notes checklist from Phase 1. Mark each item "not relevant", "verified, no change
   needed" (with the reason) or "fixed" (with the regression test).
3. Feature flags: diff `grep -rn "new FeatureFlag(" server/src/main/java` between the tags.
   - Features gated by the ES FeatureFlag class are unsupported as they are still under development by ES.
   - A flag that disappeared means the feature is now on by default and must be supported.
   - New features that are disabled by default, which can be enabled by a user need to be supported.
4. Work through every entry in `.claude/hotspots.md` marked "version bumps": run its check and note "hit" (with what you did) or "not hit".
5. For every row in the baseline's "depends on ES internals" table, diff the ES side between the tags. If ES
   changed it, re-verify the Search Guard code and its test.
6. For every deferred item in the baseline, note whether B changes anything about it. Unresolved items carry
   forward.
7. Rolling upgrade: users move to this build by rolling restart from earlier ES minors and Search Guard releases
   (`CLAUDE.md`, "Compatibility"). Check that no port changed what Search Guard writes to the wire, to
   thread-context headers or to the config index.
8. Reviews, on the near-final diff: hand the task and the diff to `compat-reviewer` and `simplicity-reviewer` in
   parallel and wait for both. Apply the findings; a BREAKING verdict or a mitigation that needs a new setting
   means "Halt and ask".

## Phase 5: Full module test run

Remove all temporary `SGTRACE` code. Then run `bash .claude/scripts/full-suite.sh` and watch
`.ai-runs/full-suite/summary.txt` with a Monitor until it contains `ALL_DONE`. Do not use `mvn clean test`
(see `CLAUDE.md`, "Tests").

Compare the per-module counts with the baseline's test table. A module that runs clearly fewer tests needs an
explanation: tests that stop running look like a pass.

For failures, follow `CLAUDE.md` "Tests": fix yours and re-run only the failing classes; run the full suite again
only if the fix touches code shared across modules. Known flakes are listed under "Build and test" in `CLAUDE.md`.

## Phase 6: Update the knowledge files (part of this bump's diff)

1. Sort every finding of this bump into exactly one place:
   - a trap that will recur on later bumps or in other code changes → `.claude/hotspots.md`;
   - a fact about the state at B (a decision, a dependency on ES internals, a deferred item) → `baseline-<B>.md`;
   - a fact that only mattered for A → B (a rename, a changed constructor) → the report only.

   Test: would this sentence still be true and useful for the bump from B to the next version? If not, it does
   not belong in `.claude/hotspots.md`.
2. Write `baseline-<B>.md` with the same sections as the current baseline: decisions and why; Search Guard code
   that depends on ES internals; deferred and open items (carry forward the unresolved ones); Elastic's notes for
   this bump; the test baseline from Phase 5; commands to regenerate the ES lists. Delete `baseline-<A>.md`; git
   history keeps it.
3. Update `.claude/hotspots.md` following the maintenance rules at its top: add new traps (with "Applies to"),
   set "Last hit" on entries that caused work. Remove an entry only when the
   ES mechanism or the Search Guard code it describes no longer exists, verified at B, and give the reason in the
   report. Never remove an entry because it did not fire this time.
4. This file (`SKILL.md`) changes only when the procedure itself should change, and never gets version numbers.

## Phase 7: Report

Do not commit unless asked (the previous bumps were single commits titled "Bump to <version>"; a
security fix deserves its own commit). Use the report format in `CLAUDE.md` "Reporting", including the summary
and the decisions you made on your own.

Details:

- the release-notes checklist from Phase 1 with the outcome per item, and any page that could not be read
- every ES API change and how it was ported, one line each
- any behavioural ES change found in Phase 4 and its fix, with the regression test added
- the hotspot checks: hit or not hit, one line each
- a table of test suites run with counts, skipped, flakes, and any failures with their cause, compared with the
  baseline
- knowledge updates: hotspots added, updated or removed (with reasons), and the new baseline written
- what was not run

Separate findings (not part of this task): Search Guard bugs or risks noticed along the way, or "None".

What would have helped: if the answer is a recurring trap, it is already in `.claude/hotspots.md` (Phase 6); if it is
about the procedure, propose the change to this file.
