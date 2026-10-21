---
name: version-bump
description: Bump the Elasticsearch version this Search Guard FLX branch is pinned to, port the code to the new ES APIs, and verify it the way CI does. Use when asked to bump, upgrade or move to a new Elasticsearch version (e.g. "bump to 9.5.5", "version-bump 9.6.1").
argument-hint: <elasticsearch-version>
---

# Elasticsearch version bump

The goal is to produce a new version of Search Guard that properly implements auth/auth for all existing and new features of Elasticsearch. The version bump changeset shall contain only changes required for the version bump.

Target version: **$ARGUMENTS** (if empty, ask for it before doing anything).

The ES version is pinned per branch (`main` = 9.x, `main-es8` = 8.19.x, release branches `sg-flx-*`), so
first confirm the target matches the branch you are on. Never bump across majors on a branch.

Work autonomously through all phases. Do not stop at "it compiles": the dangerous ES changes in this
plugin show up only at runtime, on real nodes, on multi-node clusters. Keep behaviour identical unless an API forces a change; the diff should be the bump plus the minimum ports.

The source code for Elasticsearch is publicly available here: https://github.com/elastic/elasticsearch

All Maven calls use plain `mvn ...`: in the `/work` container the session-start hook exports
`MAVEN_ARGS="-s /work/settings.xml --batch-mode -Dmaven.repo.local=/work/.m2/repository"` (CI uses the
same `.m2/repository` path). Use `-o` only after a full online build has succeeded AND a test phase has
run once, otherwise surefire's own dependencies are missing.

## Phase 1: Preflight (no edits yet)

1. `git status` must be clean apart from untracked local files. Note the current `elasticsearch.version`
   in the root `pom.xml`.
2. Check the target exists: `curl -sS -o /dev/null -w "%{http_code}" https://repo.maven.apache.org/maven2/org/elasticsearch/elasticsearch/$ARGUMENTS/elasticsearch-$ARGUMENTS.pom` must be 200.
3. Download the old and new `elasticsearch-<v>.pom` plus `plugin/transport-netty4-<v>.pom` to the
   scratchpad and diff them. Note Lucene, log4j, jackson, netty versions and any new/removed
   dependencies. Search Guard pins its own `netty.version`, `log4j.version`, `jackson.version` in the
   root pom; leave them alone unless the build or a test breaks because of them. Report the differences.
4. `grep -rn "<old-version>"` across the repo (excluding `target/`, `.m2/`, `sgi8/`). Normally only the
   root pom matches; the integration-test branch in `ci/backend-int-tests.yml` is derived from the pom.

## Phase 2: Bump and compile

1. Set `<elasticsearch.version>` in the root `pom.xml`.
2. `mvn ... install -DskipTests` (online). On a compile error fix it and resume with
   `-rf :<module-artifact-id>` (e.g. `-rf :search-guard-flx-support`). Test sources compile in this
   step too, so test-side breakage surfaces here.
3. For every error, look at the real new API before guessing:
   - `javap -cp /work/.m2/repository/org/elasticsearch/elasticsearch/$ARGUMENTS/elasticsearch-$ARGUMENTS.jar <fqcn>`
   - `unzip -l <jar> | grep <Name>` to find renamed/moved classes
   - source for a single file: `https://raw.githubusercontent.com/elastic/elasticsearch/v$ARGUMENTS/server/src/main/java/org/elasticsearch/<path>.java`
     (compare with the old tag the same way to see what changed and why)
4. **Never reference an ES class from a package the server module does not export** (e.g.
   `org.elasticsearch.search.fetch.chunk`). It compiles and passes embedded tests, but a real node
   dies with `IllegalAccessError`. Check with `grep "exports <package>" server/src/main/java/module-info.java`
   in the ES sources (see Phase 4) before using anything outside the usual action/index/search packages.
5. Never reference any ES X-Pack classes, except for tests (siehe Claude.md Architecture section)

## Phase 3: Targeted runtime checks (embedded clusters)

Run the test classes covering every file you touched.

Before running tests selectively, build the top-level project.

Gotchas: `@RunWith(Parameterized)` needs `-Dtest='Class#*method*'`; add
`-Dsurefire.failIfNoSpecifiedTests=false` when naming several classes; `Tests run: 0` + BUILD SUCCESS
means the filter missed. `rerunFailingTestsCount=3` hides flakes: read the `Flakes:` line.

Read the test logs for `ERROR][...` lines from Search Guard classes even when tests pass; a silently
caught exception (e.g. "Unable to parse current index mappings source") is how the overload-rebinding
bug above showed up.

## Phase 4: Look for and report behavioural changes ES made (the real risk)

* Check the release notes and security notes for behavioural changes
  * https://www.elastic.co/docs/release-notes/elasticsearch
  * https://discuss.elastic.co/c/announcements/security-announcements/31
* New features that are disabled by default, which can be enabled by a user need to be supported. However, features gated by the ES FeatureFlag class are unsupported as they are still under development by ES

## Phase 5: Full module test run

Run

```
mvn clean test
```

## Phase 6: Report

Do not commit unless asked (the previous bumps were single commits titled "Bump to <version>"; a
security fix deserves its own commit). 

Report:

- every ES API change and how it was ported, one line each
- any behavioural ES change found in Phase 4 and its fix, with the regression test added
- a table of test suites run with counts, skipped, flakes, and any failures with their cause
- what was not run

Separate findings (not part of this task): Search Guard bugs or risks noticed along the way, or "None".

