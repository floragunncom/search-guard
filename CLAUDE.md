# CLAUDE.md

Guidance for Claude Code in this repository. Part 1 says how we work here and applies to every task. Part 2 is a
map of the codebase. Module deep-dives live next to the code (`dlic-dlsfls/CLAUDE.md`, `ci/CLAUDE.md`) and load
when you work there.

# Part 1: Working rules

## Setup

- If something is missing in the toolchain that is required to execute a certain step, install the tool without asking
- For this project, ensure that the Maven toolchain is installed
- Ignore all files in the ci folder and the .gitlab-ci.yml.

## Scope

- Do what the task asks, nothing more: no drive-by refactoring, renaming, reformatting or "while I'm here" fixes.
  Reviewers need a diff that contains only the task.
- If you notice a bug or risk that is unrelated to the task, do not fix it. Note it and report it at the end under
  "Separate findings". If it blocks the task, stop and ask.
- Tag every line of temporary debug code (prints, trace helpers) with `SGTRACE` and remove it before you report;
  the stop hook checks.
- After finishing a task, review the session and answer the question: What information would have been beneficial at the start of the session

## Simplicity

- Change existing code before adding new code. Search for an existing helper before writing one.
- No new interface, abstract class, factory, builder or utility class unless two callers need it today.
- No new settings, config attributes or feature flags unless the task asks for one. If you think compatibility
  needs one, that is a "Halt and ask" case (below), not your decision.
- No defensive code for states that cannot happen (null checks on non-null values, "just in
  case" fallbacks). Validate input at the boundaries: REST requests, config parsing, transport.
- Comments explain *why*, not *what*.
- private methods that only contain a single line are usually creating a bigger chaos in the code than they bring value

## Tests

- The test directories contain both unit tests and integration tests. 
- We prefer to have a balance between integration tests and unit tests. Integration tests shall exist for each main feature, but not every configuration variant needs integration tests. Unit tests can complete this coverage
- A bug fix starts with a test that reproduces the bug and fails before the fix.
- Use plain JUnit for pure logic. When you need a cluster, use `LocalCluster`/`TestSgConfig` like the module's
  existing tests, not the legacy harness in `security-legacy`.
- While working, run the tests of the classes you touched.
- Before you report a code change as done, run the full unit-test suite with mvn test
- When tests fail, first decide whether your change caused it. Is it a known flake (see "Build and test")? Does
  it also fail without your change? To check, run the class on the base commit in a separate worktree
  (`git worktree add ../sg-base HEAD`, then `mvn -pl <module> -am package -Dtest=<Class>
  -Dsurefire.failIfNoSpecifiedTests=false` there; no `install`, so your local Maven repository stays as it is).
  If the failure is not yours, do not fix it; report it under "Separate findings". If it is yours, fix the cause
  and re-run the failing tests.
- `BUILD SUCCESS` is not proof; see the gotchas under "Build and test".

A stop hook (`.claude/hooks/stop-check.sh`) reminds you if production code changed without tests or a full run.

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

For running the full test suite, use:

```bash
mvn clean test
```

## The test cluster

`LocalCluster` runs either JVM-embedded (default, fast, not a full ES) or as external processes (real ES nodes;
needed for LogsDB, data streams, JPMS and plugin-descriptor problems). Details load with test sources
(`.claude/rules/testing.md`).

## Architecture

- The transport protocol is trusted, the HTTP layer is not.
- Requests without a user get full authorization.
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
  - Elasticsearch transport requests may be split into subrequess
  - These subrequests then also are filtered by SearchGuardFilter
  - This is true for requests that extend ActionRequest. Basic transport requests are usually not authorized by Search Guard 

### Other modules

- `security-legacy` — artifact `search-guard-flx-legacy-test-framework`: **test sources only**, no `src/main`. The
  older `SingleClusterTest` / `AbstractSGUnitTest` / `DynamicSgConfig` harness, still used by tests that predate
  `LocalCluster`.
- `support` — `com.floragunn.searchsupport`: shared utilities, `Meta` (index metadata abstraction used throughout
  authz), component state, metrics (`MetricsLevel`, `Meter`)
- `scheduler` / `signals` / `dlic-signals` — Quartz-based job scheduling and the Signals alerting feature
- `ssl` — transport and HTTP TLS (`SearchGuardSSLPlugin`, keystore handling)
- `plugin` — assembly only; `plugin/src/main/assemblies/plugin.xml` defines what lands in the zip
- `docker` — not a Maven module; builds images from a *released* plugin zip. See `ci/CLAUDE.md`.

