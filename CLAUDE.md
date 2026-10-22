# CLAUDE.md

Guidance for Claude Code in this repository. Part 1 says how we work here and applies to every task. Part 2 is a
map of the codebase. Topic files in `.claude/rules/` and module-level `CLAUDE.md` files load automatically when you
work in their area.

# Part 1: Working rules

## Setup

- If something is missing in the toolchain that is required to execute a certain step, install the tool without asking
- For this project, ensure that the Maven toolchain is installed
- The environment sets `MAVEN_ARGS` (local repository, settings); do not override it. Never assume the path of the
  local Maven repository; look it up with `mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout`.
- Do not read or change files in the `ci` folder or `.gitlab-ci.yml` unless the task is about CI.
  `.claude/scripts/full-suite.sh` already runs the tests the way CI does.

## Scope

- Do what the task asks, nothing more: no drive-by refactoring, renaming, reformatting or "while I'm here" fixes.
  Reviewers need a diff that contains only the task.
- If you notice a bug or risk that is unrelated to the task, do not fix it. Note it and report it at the end under
  "Separate findings". If it blocks the task, stop and ask.
- Tag every line of temporary debug code (prints, trace helpers) with `SGTRACE` and remove it before you report;
  the stop hook checks.
- After finishing a task, review the session and answer the question: What information would have been beneficial
  at the start of the session? Put the answer into the report (see "Reporting") as concrete lines to add to this
  file, to a skill or to `.claude/hotspots.md`. Do not only save it to local memory: memory in a sandbox is lost
  and not shared with the team.
- `.claude/hotspots.md` lists known traps. Before you change or investigate code that an entry marked "code
  changes" names, read that entry and run its check.

## Simplicity

- Change existing code before adding new code. Search for an existing helper before writing one.
- No new interface, abstract class, factory, builder or utility class unless two callers need it today.
- No new settings, config attributes or feature flags unless the task asks for one. If you think compatibility
  needs one, that is a "Halt and ask" case (below), not your decision.
- No defensive code for states that cannot happen (null checks on non-null values, "just in
  case" fallbacks). Validate input at the boundaries: REST requests, config parsing, transport.
- Comments explain *why*, not *what*.
- private methods that only contain a single line are usually creating a bigger chaos in the code than they bring value

## Compatibility

Search Guard is upgraded by rolling restart: during an upgrade, nodes with different Search Guard and
Elasticsearch versions run in one cluster and talk to each other. Our docs promise that users can move from any
Search Guard version to a newer one this way, unless the changelog announces a breaking change. So every change
must stay compatible with all prior minor and patch versions of Search Guard and Elasticsearch, and must work while
old and new nodes share a cluster (and across clusters for cross-cluster search). Only a major upgrade may require
a full cluster restart, and only when mixed mode cannot be made safe.

Check each surface your change touches:

1. Transport wire format (`StreamInput`/`StreamOutput`): old nodes cannot read new fields.
2. Thread-context headers and transients that cross nodes (user, origin, remote address, DLS/FLS data).
3. New transport actions or action names: old nodes do not know them.
4. Config index documents (`CType`): nothing an old node's parser rejects; no stricter validation of valid configs.
5. Internal indices and their mappings (Signals, audit log, multi-tenancy, config vars).
6. REST API shapes, used by the Kibana plugin, `sgctl` and customer scripts.
7. `elasticsearch.yml` settings: names, defaults, removals.
8. Behaviour under an unchanged configuration, beyond the intended fix (authorization, DLS/FLS/masking, audit).

Elasticsearch's `TransportVersion` only changes with the Elasticsearch version, so it cannot gate a change between
two Search Guard releases on the same Elasticsearch version. Reuse the mechanism the codebase already has.

**Halt and ask.** If a change would break any of the above, or you are unsure, stop before writing it and ask
(AskUserQuestion in interactive sessions; in headless runs, end with a "Decision needed" block): what breaks and
for whom, whether to make the change at all, whether to put it behind a feature switch that defaults to the old
behaviour, and one or two alternatives.

Run the `compat-reviewer` subagent once on the near-final diff, and again only after a security-relevant fix.

## Tests

- The test directories contain both unit tests and integration tests.
- We prefer to have a balance between integration tests and unit tests. Integration tests shall exist for each main
  feature, but not every configuration variant needs integration tests. Unit tests can complete this coverage
- Every new or changed execution path must be covered by a test (unit or integration, following the balance
  above). A class that has no tests yet gets its first one.
- A bug fix starts with a test that reproduces the bug and fails before the fix.
- Use plain JUnit for pure logic. When you need a cluster, use `LocalCluster`/`TestSgConfig` like the module's
  existing tests, not the legacy harness in `security-legacy`.
- While working, run the tests of the classes you touched. Tests on external-process clusters need the prebuilt
  plugin zip (`.claude/rules/testing.md`).
- Order: targeted tests and reviews first, apply their findings, then the full suite once. Never start the full
  suite while a review or another test run is still pending.
- Before you report a code change as done, run the full suite with `bash .claude/scripts/full-suite.sh`. It builds
  once, runs every module with the prebuilt plugin zip, continues past failing modules and writes
  `.ai-runs/full-suite/summary.txt`, which ends with `ALL_DONE` (about 15 minutes; watch it with a Monitor).
  Do not use `mvn clean test`: every external-cluster test class then builds the plugin itself, parallel forks
  collide, and the reactor stops at the first failing module.
- After a full run, fix the failures your change caused and re-run only the failing classes. Run the full suite
  again only if the fix touches code shared across modules (e.g. `SearchGuardFilter`, transport, privilege
  evaluation).
- When tests fail, first decide whether your change caused it. Is it a known flake (see "Build and test")? Does
  it also fail without your change? To check, run the class on the base commit in a separate worktree
  (`git worktree add ../sg-base HEAD`, then `mvn -pl <module> -am package -Dtest=<Class>
  -Dsurefire.failIfNoSpecifiedTests=false` there; no `install`, so your local Maven repository stays as it is).
  If the failure is not yours, do not fix it; report it under "Separate findings". If it is yours, fix the cause
  and re-run the failing tests.
- `BUILD SUCCESS` is not proof; see the gotchas under "Build and test".

When you write the final report, a stop hook (`.claude/hooks/stop-check.sh`) checks that no `SGTRACE` code is
left, that production changes come with test changes, and that a full run completed during this work.

## Reporting

Every code change and every analysis ends with a report in this order (a quick question just gets an answer):

1. **Summary**, for a colleague who knows Search Guard but not this code. At most 250 words, no code, no stack
   traces, no file paths, at most two class names. Cover: the problem or request, what changes in behaviour,
   compatibility (rolling-upgrade safe? breaking?), risk, any decision needed (or "none"), and the decisions you
   made on your own that the team should review, such as which privileges a new action requires (or "none").
2. **Details**, for the reviewer: changed files with one line each, a coverage table (every changed production
   method → its test, and whether that test is new, extended or existing), test runs (suite, run, failed, skipped,
   flakes), and what was not run and why.
3. **Separate findings (not part of this task)**: unrelated bugs or risks you noticed, each with location, why it
   matters and a suggested next step. Write "None" if there are none.
4. **What would have helped**: the answer to the question under "Scope", as lines to add to this file or a skill.

## References

Consult these when behaviour, a configuration option or an Elasticsearch change is in question, and cite the
page you used:

- Search Guard documentation: https://docs.search-guard.com/latest/
- Elasticsearch release notes: https://www.elastic.co/docs/release-notes/elasticsearch
- Elasticsearch security announcements: https://discuss.elastic.co/c/announcements/security-announcements/31

# Part 2: Codebase map

## What this is

The repo mixes two licenses, which matters when adding files:

- **Apache 2** — `security`, `security-legacy`, `scheduler`, `signals`, `ssl`, `support`, `plugin`, `dev`, `ci`, root
- **Proprietary** (floragunn "DLIC") — `dlic-security`, `dlic-signals`, `dlic-auditlog`, `dlic-dlsfls`,
  `dlic-fe-multi-tenancy`

Copy the license header from a neighbouring file in the same module; the two headers differ. Files without license headers are not allowed.

## Branches

The ES version is pinned *per branch* (read `elasticsearch.version` in the root `pom.xml`), so "which ES" is a
property of the branch you are on, not a build flag:

- `main` — ES 9.x
- `main-es8` — ES 8.19.x
- release branches `sg-flx-<suite>.x-es-<es>.x`, e.g. `sg-flx-4.1.x-es-9.4.x`, `sg-flx-4.1.x-es-8.19.x`

## Build and test

```bash
mvn install -DskipTests                                  # everything, including the plugin zip
mvn -pl <module>[,<module>] -am install -DskipTests      # only changed modules and what they depend on
mvn -pl <module> test -Dtest='SomeTest'                  # one test class
bash .claude/scripts/full-suite.sh                       # full suite, see "Tests"
```

Gotchas:

- For a `@RunWith(Parameterized.class)` class, `-Dtest='Foo#someMethod'` matches **0 tests**; use a trailing
  wildcard (`-Dtest='FlsIntTest#*multiRole*'`). `Tests run: 0` plus `BUILD SUCCESS` means the filter missed.
  Add `-Dsurefire.failIfNoSpecifiedTests=false` when naming several classes across modules.
- `@RunWith(Suite.class)` classes (e.g. `RoleBasedFieldAuthorizationTest`): the inner class is the real test class.
- `rerunFailingTestsCount=3` counts a test that passes on retry as a pass; only the `Flakes:` line shows it.
- `mvn -o` fails until one online build and one online test run have populated the local repository.
- Known flake: class-level errors in Signals (`ConfigVarRefreshAction ... no such index
  [.searchguard_config_vars]`) are a startup race in the embedded test cluster. Class-level errors are not
  retried. Re-run the class alone; if it passes, report it as this known flake.

## The test cluster

`LocalCluster` runs either JVM-embedded (default, fast, not a full ES) or as external processes (real ES nodes;
needed for LogsDB, data streams, JPMS and plugin-descriptor problems). Details load with test sources
(`.claude/rules/testing.md`).

## Architecture

- The transport protocol is trusted, the HTTP layer is not.
- Requests without a user get full authorization.
- Therefore losing the user on the way between nodes is a privilege escalation. Elasticsearch loses it wherever it
  stashes the thread context and restores only headers it captured earlier (for example the chunked fetch phase),
  because Search Guard keeps user, origin and remote address as transients. Check every such path;
  `.claude/hotspots.md` describes how.
- Authentication happens only on the REST layer.
- Search Guard is a replacement for X-Pack Security. They never run together.
- Elasticsearch Serverless is out of this scope
- In non-test code, only Elasticsearch core classes may be referenced, no plugin classes

## Request Flow

- Search Guard receives a REST Request
- The request is filtered to perform authentication
- Elasticsearch turns this request in one or more Transport requests
- These are filtered by the SearchGuardFilter to perform authorization
- Authentication at this point may not be complete
  - Elasticsearch transport requests may be split into subrequests
  - These subrequests then also are filtered by SearchGuardFilter
  - This is true for requests that extend ActionRequest. Basic transport requests are usually not authorized by Search Guard

## Other modules

- `security-legacy` — artifact `search-guard-flx-legacy-test-framework`: **test sources only**, no `src/main`. The
  older `SingleClusterTest` / `AbstractSGUnitTest` / `DynamicSgConfig` harness, still used by tests that predate
  `LocalCluster`.
- `support` — `com.floragunn.searchsupport`: shared utilities, `Meta` (index metadata abstraction used throughout
  authz), component state, metrics (`MetricsLevel`, `Meter`)
- `scheduler` / `signals` / `dlic-signals` — Quartz-based job scheduling and the Signals alerting feature
- `ssl` — transport and HTTP TLS (`SearchGuardSSLPlugin`, keystore handling)
- `plugin` — assembly only; `plugin/src/main/assemblies/plugin.xml` defines what lands in the zip
- `docker` — not a Maven module; builds images from a *released* plugin zip.
