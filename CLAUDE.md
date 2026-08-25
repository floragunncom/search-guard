# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Search Guard FLX — a security plugin for Elasticsearch (authentication, authorization, DLS/FLS, audit logging,
multi-tenancy, alerting/Signals). Maven multi-module, `maven.compiler.release=17` — note that is the *target*,
not the toolchain: CI builds on a JDK 25 image (`SG_BUILD_IMAGE` in `.gitlab-ci.yml`). The build produces a
single ES plugin zip at `plugin/target/releases/search-guard-flx-elasticsearch-plugin-*.zip`.

The repo mixes two licenses, which matters when adding files:

- **Apache 2** — `security`, `security-legacy`, `scheduler`, `signals`, `ssl`, `support`, `plugin`, `dev`, `ci`, root
- **Proprietary** (floragunn "DLIC") — `dlic-security`, `dlic-signals`, `dlic-auditlog`, `dlic-dlsfls`,
  `dlic-fe-multi-tenancy`

Copy the license header from a neighbouring file in the same module; the two headers differ.

The version is derived, not literal: `${revision}` = `${sg-suite.version}-es-${elasticsearch.version}`, both set in
the root `pom.xml`. `elasticsearch.version` pins the ES version the whole build compiles and tests against.

## Branches

The ES version is pinned *per branch*, so "which ES" is a property of the branch you are on, not a build flag:

- `main` — ES 9.x (currently 9.4.4)
- `main-es8` — ES 8.19.x
- release branches `sg-flx-<suite>.x-es-<es>.x`, e.g. `sg-flx-4.1.x-es-9.4.x`, `sg-flx-4.1.x-es-8.19.x`

A fix therefore usually needs porting across `main` and `main-es8` (and the live release branches). `ci/backport/backport.py`
plus `ci/backport.yml` automate part of that.

## Build and test

The `enterprise` profile auto-activates whenever the `dlic-security` directory exists, so in this repo the `dlic-*`
modules are part of the reactor by default — no `-Penterprise` needed. Maven *merges* a profile's `<modules>` into
the base list rather than replacing it, so the effective reactor is the union of the two: the `<modules>` block at
the top of the root pom is the community set, and the `enterprise` profile adds the `dlic-*` modules on top. (The
profile's own list happens to omit `ssl`; `ssl` still builds, because it comes from the base list.)

```bash
# Full build, no tests
mvn install -DskipTests

# Build the plugin zip quickly (`quick` is declared in plugin/pom.xml, not the root pom;
# it adds all enterprise modules to the plugin assembly)
mvn install -Dmaven.test.skip.exec=true -Pquick

# Test one module (this is the normal inner loop)
mvn -o -pl dlic-dlsfls test

# One module plus its dependencies, first time or after changing a dependency module
mvn -o -pl dlic-dlsfls -am -DskipTests install && mvn -o -pl dlic-dlsfls test
```

Running a single test:

```bash
mvn -o -pl <module> test -Dtest='SomeTest' -DfailIfNoTests=false
mvn -o -pl <module> test -Dtest='SomeTest$InnerSuiteClass' -DfailIfNoTests=false
mvn -o -pl <module> test -Dtest='SomeTest#someMethod' -DfailIfNoTests=false
```

**Gotcha:** for a `@RunWith(Parameterized.class)` class, `-Dtest='Foo#someMethod'` silently matches **0 tests** —
surefire compares against the decorated name (`someMethod[param]`). Use a trailing wildcard instead:
`-Dtest='FlsIntTest#*multiRole*'`. A run reporting `Tests run: 0` plus `BUILD SUCCESS` means the filter missed, not
that the tests passed.

Several test classes are `@RunWith(Suite.class)` with nested `@Suite.SuiteClasses` (e.g.
`RoleBasedFieldAuthorizationTest`), so the inner class is the real test class.

`mvn -o` (offline) works once the local repo is populated and avoids slow snapshot resolution. Surefire runs
`forkCount=3` with `-Xmx4g` per fork, so a full-module run is memory-hungry.

**Gotcha:** the root pom sets `rerunFailingTestsCount=3`. A test that fails and then passes on a retry is counted
as a **pass** — the run is green and only a `Flakes:` line in the surefire summary hints at it. When working on a
flaky test, read `<module>/target/surefire-reports/*.txt` (or the `<flakyFailure>` elements in the XML) instead of
trusting `BUILD SUCCESS`, and re-run the class a few times before declaring it fixed.

There is no configured linter or formatter goal. `dev/eclipse-code-format-profile.xml` is the shared IDE format
profile; match surrounding style rather than reformatting files.

## The test cluster: two very different modes

`LocalCluster` (in `security/src/test/.../test/helper/cluster/`, shipped to other modules via the
`search-guard-flx-security:tests` artifact) can start a cluster two ways. Read its class javadoc before debugging
cluster-related test failures — it documents the trade-offs in full.

- **JVM-embedded (default locally).** Fast, debuggable, but only loads the SG modules that are on the *test
  module's* dependency list — enterprise modules or Signals may simply be absent. Not a full ES.
- **External process** (`.useExternalProcessCluster()` on the builder, or `-Dsg.tests.use_ep_cluster=true`).
  Downloads a real ES distribution and runs real nodes. Required for features the embedded cluster lacks — LogsDB
  index mode and data streams are the usual reasons a test class forces it. On CI the mode is chosen by
  `ci/run_tests.sh` from `TEST_CLUSTER_TYPE` (`full` → external process, anything else → embedded); that variable
  is not set anywhere in the repo, so unless it is defined as a GitLab project/pipeline variable the unit-test
  jobs run embedded too.

System properties:

- `-Dsg.tests.use_ep_cluster=true` — force external process cluster
- `-Dsg.tests.sg_plugin.file=/path/to/plugin.zip` — use a prebuilt plugin instead of building one per run
- `-Dsg.tests.es_download_cache.dir=/path` — reuse downloaded ES archives

Watch the import: `scheduler/src/test/java/com/floragunn/searchsupport/jobs/LocalCluster.java` is a completely
different, unrelated class of the same name, used only by the scheduler's own tests.

Test configuration is built in Java, not YAML: `TestSgConfig` with nested `Role`, `User`, `Authc`, `DlsFls`
builders. A test declares users/roles as static fields and hands them to the `LocalCluster.Builder`, then drives
the cluster over REST with `GenericRestClient`. Follow the existing pattern in the module you're editing.

## Architecture

### Module system

`SearchGuardPlugin` is the ES entry point. It does **not** reference enterprise code at compile time — enterprise
modules are registered reflectively by class-name string (`SearchGuardPlugin.java`, look for
`enterpriseModulesEnabled` and `moduleRegistry.add(...)`). `SearchGuardModulesRegistry` instantiates each name and
collects the extension points it implements.

Everything pluggable implements `SearchGuardModule` (`security/src/main/java/com/floragunn/searchguard/`), whose
default methods are the extension points: REST handlers, actions, action filters, `SyncAuthorizationFilter`s,
field predicates (FLS), directory reader wrappers (DLS), query cache weight providers, search/indexing operation
listeners, config validators.

Consequences worth internalising:

- Adding an enterprise feature means implementing `SearchGuardModule` in a `dlic-*` module and adding its FQCN to
  the enterprise list in `SearchGuardPlugin` — not adding a Maven dependency from `security`.
- `searchguard.modules.disabled` (and `disableModule()`/`enableModule()` in tests) can switch modules off, so code
  must tolerate a module being absent.

### Request pipeline

`SearchGuardFilter` is the ES `ActionFilter` that runs per request. Order matters: pre-privilege-evaluation
`SyncAuthorizationFilter`s run first, then privilege evaluation (`PrivilegesEvaluator` →
`RoleBasedActionAuthorization`), then the regular `SyncAuthorizationFilter`s (DLS/FLS lives here). Each filter
returns `OK` / `DENIED` / `INTERCEPTED` / `PASS_ON_FAST_LANE`.

`authz/actions/` turns an ES `ActionRequest` into something authorizable: `ActionRequestIntrospector` extracts the
targeted indices into `ResolvedIndices`; `Action`/`Actions` model ES action names and action groups.

### Configuration

Config lives in an ES index, not on disk. `CType` (`configuration/CType.java`) is the registry of config
documents — `roles`, `rolesmapping`, `actiongroups`, `internalusers`, `tenants`, `authc`, `authz`, `blocks`,
`config_vars`, `license_key`, … Each `CType` binds a name to a parser and a model class.
`ConfigurationRepository` loads and hot-reloads them; `SgDynamicConfiguration<T>` is the in-memory typed
representation that authorization code consumes.

Config parsing uses floragunn's own `codova` library (`DocNode`, `ConfigValidationException`) and
`fluent-collections` (`ImmutableList`, `ImmutableSet`, `ImmutableMap`) rather than Jackson/Guava directly. New
config code should follow suit.

### DLS/FLS (`dlic-dlsfls`)

The most intricate part of the codebase, and the one where a small change has wide blast radius.

`RoleBasedAuthorizationBase<SingleRule, JoinedRule>` is the shared skeleton for the three restriction kinds —
`RoleBasedDocumentAuthorization` (DLS), `RoleBasedFieldAuthorization` (FLS), `RoleBasedFieldMasking`. Each
converts a `Role.Index` into a *single-role rule*, then merges the rules of all the user's matching roles into a
*joined rule*.

Two things to hold onto:

- **Restrictions union across roles — the most permissive role wins.** If any of a user's roles matches the index
  with no restriction, the result is unrestricted.
- **Rules are collected from every role matching the index, regardless of the action being performed.**
  `getRestrictionImpl` consults only `context.getMappedRoles()` and index patterns. A role that grants an
  unrelated action still contributes its DLS/FLS rules to the current request.
- Merged rules evaluate each role independently and OR the results. FLS patterns are **order-sensitive within a
  role** (a later pattern overrides an earlier one), so flattening patterns across roles would be incorrect —
  one role's exclusion would clobber another's inclusion.

Rule lookup has a fast "stateful" path (precomputed per concrete index, rebuilt on config/metadata change) and a
"static" fallback for indices the stateful rules don't cover. Both must agree.

Enforcement happens in several places, and a change to one does not affect the others:

- `lucene/DlsFlsDirectoryReaderWrapper` — DLS query application and FLS at the Lucene level
- `lucene/FlsStoredFieldVisitor` + its inner `DocumentFilter` — rewrites `_source` JSON field by field
- `FlsFieldFilter` — mapping / field-caps visibility
- `RoleBasedFieldMasking` — value hashing/masking

FLS has an "include mode" subtlety: an include rule for `a.b.c` must implicitly permit *traversing* `a` and `a.b`
even though neither is itself readable. That is what `objectOnlyPatterns` /
`isObjectAllowedAssumingParentsAreAllowed(...)` exist for, as distinct from `isAllowedAssumingParentsAreAllowed(...)`
(is the field itself readable) and `isAllowedRecursive(...)` (used by everything outside `_source` filtering).

### Other modules

- `security-legacy` — artifact `search-guard-flx-legacy-test-framework`: **test sources only**, no `src/main`. The
  older `SingleClusterTest` / `AbstractSGUnitTest` / `DynamicSgConfig` harness, still used by tests that predate
  `LocalCluster`. Write new tests against `LocalCluster`/`TestSgConfig` instead.
- `support` — `com.floragunn.searchsupport`: shared utilities, `Meta` (index metadata abstraction used throughout
  authz), component state, metrics (`MetricsLevel`, `Meter`)
- `scheduler` / `signals` / `dlic-signals` — Quartz-based job scheduling and the Signals alerting feature
- `ssl` — transport and HTTP TLS (`SearchGuardSSLPlugin`, keystore handling)
- `plugin` — assembly only; `plugin/src/main/assemblies/plugin.xml` defines what lands in the zip
- `dlic-security-legacy` — **orphan**: a `pom.xml` and nothing else, listed in no reactor and referenced by no CI
  job. Don't add code there; it will not be built.
- `docker` — not a Maven module. `Dockerfile` installs a *released* plugin zip from `maven.search-guard.com` into
  the `floragunncom/es-mirror` base image; `build_and_push.sh` drives the multi-arch build. In CI these jobs run
  only when the `DOCKER_ONLY` variable is set, which switches every other job off.

## CI

GitLab CI: `.gitlab-ci.yml` includes `.gitlab-ci-branch-specific.yml` and six job files under `ci/`. The two
tiers you will actually interact with:

- **Backend unit tests** (`ci/backend-unit-tests.yml`) — `ci/run_tests.sh <module> [profile]`, one job per module,
  run as a non-root `es_test` user, with `TEST_CLUSTER_TYPE` selecting embedded vs external-process (see above).
  `SslHostnameVerificationTest` has its own job because it floods the GitLab log limit.
- **Backend integration tests** (`ci/backend-int-tests.yml`) — clone the separate private `sgi8` repo, install the
  freshly built plugin zip into its docker compose setup, and run suites selected by the `TEST_DEFINITIONS` regex
  (`community`, `enterprise_1..3`, `ccs_1..3`, `compliance_1..3`, `compliance_ccs`, `signals_1..2`). A local
  `sgi8/` checkout, if present, is that repo. The branch it clones defaults to `<ES major>.<ES minor>.x` derived
  from `elasticsearch.version` (so `9.4.x` on `main` today) — **not** from the Search Guard version — and is
  overridable via `IT_BRANCH` in `.gitlab-ci-branch-specific.yml`.

The rest of the pipeline, which you rarely need to touch: `ci/frontend-int-tests.yml` (pulls
`search-guard-kibana-plugin` at `SG_KI_BRANCH`, also set in `.gitlab-ci-branch-specific.yml`),
`ci/mt-data-migration-tests.yml`, `ci/security-scan.yml` (Trivy + a CycloneDX SBOM + OWASP Dependency-Check), and
`ci/backport.yml`.

Commit-message switches: `SKIP-UT`, `SKIP-TESTS`, `SKIP-SGI` skip the corresponding jobs.

`ci/resource-diagnostics.sh` is sourced by both tiers and writes `ci-diagnostics/` artifacts. It exists because
test-cluster startup flakiness on shared runners is usually CPU/memory/IO contention rather than a real bug —
check those artifacts before chasing a "node not connected" or "startup timed out" failure as a code defect.
