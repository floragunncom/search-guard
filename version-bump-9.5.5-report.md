# Elasticsearch version bump 9.4.3 → 9.5.5

Date: 2026-10-21. Base commit: 314aed9 "Bump to 9.4.3". Nothing is committed; the working tree holds the diff
(28 modified files plus one new test).

## Summary

The branch is bumped from Elasticsearch 9.4.3 to 9.5.5.

Most of the port is mechanical: ES renamed the reindex response classes, removed two interfaces (`RealtimeRequest`,
pooled `SearchHits` helpers), added abstract methods to `XContentGenerator`, and changed how put-mapping and
index-source bytes are exposed. Behaviour is unchanged for all of these, with one deliberate nuance: ES no longer
converts put-mapping bodies to JSON up front, so the audit log and the multi-tenancy mapping interceptor now convert
using the request's content type (a YAML regression test was added).

Three ES behaviour changes needed real work:

1. **FLS and field masking were bypassed on ES 9.5.** ES now streams the fetch phase through a coordination action
   that stashes the thread context and re-applies only captured headers. Search Guard kept the user as a transient,
   so fetch requests reached data nodes without a user and the reader wrapper applied no FLS/FM. The action filter
   now also records user, origin and remote address as headers. This is security-relevant and must ship with the
   bump; it deserves its own commit.
2. The modify-data-stream API can delete backing indices. Search Guard requires `indices:admin/delete` on the
   backing index in addition to modify on the data stream (stricter than X-Pack).
3. ES can create backing indices for past TSDB timestamps on the write path (opt-in setting). The new action is
   registered and added to the create-index action group, mirroring X-Pack.

Rolling upgrade: safe (no serialization, config or setting changes; the user header already travels on every
transport request). Decisions to review: items 2 and 3 above, and the header placement in `SearchGuardFilter`.

## Dependency differences (ES pom diff 9.4.3 → 9.5.5)

- Lucene 10.4.0 → 10.5.1
- log4j 2.26.0 → 2.26.1 (Search Guard pins its own version, left alone)
- netty 4.1.135 → 4.1.138 (Search Guard pins its own version, left alone)
- new dependency `elasticsearch-ip-location-api`

## ES API changes ported

| ES change | Port |
|---|---|
| `BulkByScrollResponse`/`BulkByScrollTask` renamed to `BulkByPaginatedSearch*` | renamed in support, multi-tenancy migration steps, mapper, tests |
| `XContentGenerator` gained `closeAllowIllFormed()` and `writeString(Reader,int)` | implemented in the two stub generators in support |
| `RealtimeRequest` interface removed (PR 145922) | `DlsFlsValve` checks the four concrete request classes (Get, MultiGet, TermVectors, MultiTermVectors) |
| `SearchHits.asUnpooled()`/`unpooled(...)` removed; `SearchResponse` constructor has two new trailing args | `SearchMapper` deep-copies hit source, uses the public `SearchHits` constructor, passes nulls (only used for profiling) |
| `PutMappingRequest.source()` returns `BytesReference` and keeps the original content type | audit log and migration interceptor convert via `request.xContentType()` |
| `SourceToParse.source()` returns `Source` | audit log uses `originalBytes()` |
| `BulkShardRequest` constructor gained `SplitShardCountSummary` | two tests use `SplitShardCountSummary.UNSET` |
| `DataStreamAction.Type.DELETE_BACKING_INDEX` added (PR 151137) | mapped to the existing `DELETE_INDEX` additional dimension |
| New action `indices:admin/data_stream/auto_create_past_tsdb` (PR 152716) | registered as data stream action, added to `SGS_CREATE_INDEX` |

Checked and not affected: `-remote:index` exclusion syntax (Search Guard does not evaluate remote patterns
locally), `RemoteClusterAware.splitIndexName` returning `QualifiedIndexExpression`, form-encoded REST bodies (gated
on X-Pack security), `RestHandler.supportsReadOnlyFormEncodedPostBody()` (default method, no delegating wrapper in
Search Guard), ES|QL datasets and the new `DATASET` index abstraction (X-Pack feature, same situation as views; see
findings), new server action `internal:index/shard/recovery/cancel_recoveries`, ThreadContext, transport and REST
handler interface changes.

## Behavioural changes and regression tests

- **Chunked fetch phase (ES 9.5.0, `search.fetch_phase_chunked_enabled` now default true; FeatureFlag-off in
  9.4.3).** `SearchTransportService.sendExecuteFetch` captures only the thread-context headers, and
  `TransportFetchPhaseCoordinationAction` stashes the context and re-applies them before sending
  `indices:data/read/search[phase/fetch/id]`. Fix in `SearchGuardFilter.apply0`: put `_sg_user_header`,
  `_sg_origin_header` and `_sg_remote_address_header` into the thread context when absent (same headers that
  `SearchGuardInterceptor` adds at send time). Regression coverage is the existing `FlsIntTest`, `FmIntTest` and the
  SQL cursor test in `SqlAuthorizationReadOnlyIntTests`, which fail on 9.5.5 without the fix (verified: 8 + 8 + 1
  failures) and pass on 9.4.3 (verified in a base-commit worktree). Workaround for clusters that cannot upgrade
  Search Guard: disable `search.fetch_phase_chunked_enabled` dynamically.
- **Delete backing index.** Three new tests in `DataStreamAddRemoveIndicesTest` (no permission, modify only → 403,
  modify plus delete → 200 and index gone).
- **Past TSDB index creation.** New external-cluster test `DataStreamPastTsdbIndexCreationIntTest` with the setting
  `data_stream.past_tsdb_index_creation_enabled` on: a write-only user gets a `security_exception` bulk item and no
  new backing index, a user with `SGS_CREATE_INDEX` gets `created` and a second backing index.

## Details

### Changed files

Production:

- `pom.xml` — `elasticsearch.version` 9.4.3 → 9.5.5
- `support/.../indices/IndexCleanupAgent.java` — rename to `BulkByPaginatedSearchResponse`
- `support/.../xcontent/AttributeValueFromXContent.java` — implement the two new `XContentGenerator` methods
- `support/.../xcontent/ObjectTreeXContent.java` — implement the two new `XContentGenerator` methods
- `security/.../authz/actions/ActionRequestIntrospector.java` — `DELETE_BACKING_INDEX` → `DELETE_INDEX` dimension
- `security/.../authz/actions/Actions.java` — register `indices:admin/data_stream/auto_create_past_tsdb`
- `security/src/main/resources/static_config/static_action_groups.yml` — add the action to `SGS_CREATE_INDEX`
- `security/.../filter/SearchGuardFilter.java` — put user/origin/remote-address headers into the thread context
- `dlic-dlsfls/.../DlsFlsValve.java` — replace `RealtimeRequest` with four concrete request classes
- `dlic-auditlog/.../impl/AbstractAuditLog.java` — `source().originalBytes()`, content-type-aware put-mapping source
- `dlic-auditlog/.../impl/RequestResolver.java` — pass put-mapping source with its content type
- `dlic-fe-multi-tenancy/.../FrontendDataMigrationInterceptor.java` — content-type-aware put-mapping conversion
- `dlic-fe-multi-tenancy/.../steps/CopyDataToGlobalIndexStep.java`, `CreateBackupOfGlobalIndexStep.java`,
  `DeleteGlobalIndexContentStep.java`, `StepRepository.java` — renames to `BulkByPaginatedSearch*`
- `dlic-fe-multi-tenancy/.../request/mapper/SearchMapper.java` — unscope response without pooled `SearchHits` helpers
- `dlic-fe-multi-tenancy/.../request/mapper/UpdateByQueryMapper.java` — rename to `BulkByPaginatedSearchResponse`

Tests:

- `security/.../ActionRequestIntrospectorTest.java` — `BulkShardRequest` constructor
- `security/.../int_tests/DataStreamAddRemoveIndicesTest.java` — extended with three delete-backing-index tests
- `security/.../int_tests/DataStreamPastTsdbIndexCreationIntTest.java` — new
- `signals/.../TransportExecuteWatchActionTest.java` — public `SearchHits` constructor
- `signals/.../ProxyApiTest.java` — rename
- `dlic-auditlog/.../AuditlogTest.java` — extended with a YAML put-mapping case
- `dlic-auditlog/.../IgnoreRequestBodiesTest.java` — `BulkShardRequest` constructor
- `dlic-fe-multi-tenancy/.../CopyDataToGlobalIndexStepTest.java`, `CreateBackupOfGlobalIndexStepTest.java`,
  `DeleteGlobalIndexContentStepTest.java`, `TenantScopedActionListenerWrapperTest.java` — renames

### Coverage

| Changed method | Test | Status |
|---|---|---|
| `SearchGuardFilter.apply0` header block | FlsIntTest, FmIntTest, SqlAuthorizationReadOnlyIntTests | existing (fail before fix) |
| `ActionRequestIntrospector` DELETE_BACKING_INDEX | DataStreamAddRemoveIndicesTest | extended |
| `Actions` / action group entry | DataStreamPastTsdbIndexCreationIntTest, ActionGroupsTest | new / existing |
| `DlsFlsValve.apply` realtime | DlsIntTest, DlsReadOnlyIntTests | existing |
| `SearchMapper.unscopeResponse` | MultiTenancyRequestMappingTest | existing |
| `FrontendDataMigrationInterceptor` put mapping | MultiTenancyMigrationTest | existing |
| `AbstractAuditLog` put mapping / index source | AuditlogTest (YAML case), AuditlogIntegrationTest | extended / existing |
| `RequestResolver` put mapping | AuditlogIntegrationTest | existing |
| `IndexCleanupAgent`, generators | support module suite, XContent converter tests | existing |
| migration steps, `UpdateByQueryMapper` | step tests, MigrationStepsTest | existing |

### Test runs (final code)

Full suite: `mvn clean test -Dsg.tests.sg_plugin.file=<prebuilt zip>`, started 16:08:40Z, finished 16:18:25Z.

| Suite | Run | Failed | Skipped | Flakes |
|---|---|---|---|---|
| support | 23 | 0 | 0 | 0 |
| ssl | 25 | 0 | 0 | 0 |
| security | 8680 | 0 | 7 | 0 |
| security-legacy | 0 (test framework only) | 0 | 0 | 0 |
| scheduler | 22 | 0 | 3 | 0 |
| dlic-dlsfls | 2527 | 0 | 0 | 0 |
| dlic-security | 126 | 0 | 1 | 0 |
| dlic-fe-multi-tenancy | 561 | 0 | 2 | 0 |
| signals | 293 | 2 class errors (RestApiTest, RemoteClusterWatchTest: `IndexNotFoundException [.searchguard_config_vars]` during embedded cluster start under 3 forks; both green standalone, 86/0) | 3 | 0 |
| dlic-auditlog | 129 | 0 | 4 | 0 |
| dlic-signals | 7 | 0 | 0 | 0 |

dlic-auditlog, dlic-signals and plugin were run with `-rf :dlic-search-guard-flx-auditlog` after the Signals class
errors: BUILD SUCCESS.

Targeted runs during the work (all green): DataStreamAddRemoveIndicesTest 9, DataStreamPastTsdbIndexCreationIntTest
2, FlsIntTest 28, FmIntTest 105, DlsIntTest 30, DlsReadOnlyIntTests 210, SqlAuthorizationReadOnlyIntTests 159,
AuthTokenIntegrationTest 18, AuthTokenDlsIntTest 2, AuditlogIntegrationTest 23, MultiTenancyRequestMappingTest 81,
MultiTenancyMigrationTest 35, AuditlogTest 21, IgnoreRequestBodiesTest 5, RoleBasedActionAuthorizationTests 2555,
ActionRequestIntrospectorTest 9, ActionGroupsTest 3, TransportExecuteWatchActionTest 9, ProxyApiTest 15,
multi-tenancy step tests 224. ActionCompletenessIntTest is skipped by design.

One flake seen in the first full run (before the fetch-phase fix):
`DataStreamFailureStoreAuthorizationReadWriteIntTests.updateByQuery_indexPattern` (cleanup timing), green on rerun.

### Not run

- Plain `mvn clean test` without `-Dsg.tests.sg_plugin.file`: every external-cluster test class runs its own
  in-process `mvn install` and the three surefire forks race (ten class errors with "mvn install failed" in the
  first attempt). See separate findings.
- CI pipeline and `ci/` folder (ignored per project rules).
- Multi-node rolling-upgrade clusters (mixed 9.4.3/9.5.5).
- A multi-node test with `hosts`-mapped roles exercising the fetch phase (origin/remote-address header forwarding).

### Security fixes brought in by 9.5.5

ESA-2026-185 (CVE-2026-102404, ES|QL memory DoS), ESA-2026-189 (CVE-2026-102408, ES|QL CHUNK regex),
ESA-2026-190 (CVE-2026-102409, recursion DoS), ESA-2026-195 (CVE-2026-103005, connector description),
ESA-2026-198 (CVE-2026-103008, recursion DoS), ESA-2026-199 (CVE-2026-103009, High, RCS 2.0 CCS authorization
bypass on fulfilling clusters; X-Pack specific).

Sources: https://discuss.elastic.co/c/announcements/security-announcements/31 and
https://www.elastic.co/docs/release-notes/elasticsearch

ES 9.5.0 breaking changes reviewed and found not Search Guard relevant: CCS remote exclusions are order-dependent
(#143338), TSDB `look_ahead_time` default 30 → 9 minutes (#145552), ILM allocate removes `auto_expand_replicas`
(#155279).

## Separate findings (not part of this task)

- **ES|QL datasets** (new in 9.5, actions `indices:admin/esql/dataset/*`, `indices:data/read/esql/resolve_datasets`,
  `IndexAbstraction.Type.DATASET`) have no Search Guard handling, same as ES views and ES|QL generally. Next step:
  decide whether dataset privileges need action groups.
- **New X-Pack actions treated as unknown**: OTLP logs/traces actions (new in 9.5) resolve to all indices; reindex
  management privileges `cluster:admin/reindex/*` and `cluster:monitor/reindex/*` use generic cluster handling.
  Next step: decide whether explicit action groups are wanted.
- **`SearchMapper` in `dlic-fe-multi-tenancy`** copies only top-level hits; aggregation top-hits and completion
  suggestions still reference the original pooled hits (pre-existing, tenant-scoped Kibana searches only).
- **Test infrastructure**: external-cluster tests (`SgPluginPackage`) build the plugin in-process per fork; with
  three forks this races under `mvn clean test`. Suggest building the zip once in the surefire configuration and
  passing `sg.tests.sg_plugin.file`.
- **Audit log** entries on the coordinating node now include the serialized `_sg_user_header` in the raw
  transport-request header dump (data-node entries already did). Next step: strip `_sg_*` keys there if unwanted.
- **Signals** `RestApiTest` and `RemoteClusterWatchTest` fail at class level under parallel forks because
  `ConfigVarRefreshAction` runs before `.searchguard_config_vars` exists during cluster start. Timing race, not
  caused by this change.

## What information would have been beneficial at the start of the session

- The real Maven local repository path in this sandbox (`/home/node/.cache/maven`, not `/work/.m2` as the skill
  says). Saved as memory.
- That the full suite needs a prebuilt plugin zip via `-Dsg.tests.sg_plugin.file=<absolute path>` because of the
  nested-build race. Saved as memory.
- That ES 9.5 relies on thread-context headers (not transients) for security context propagation in the chunked
  fetch phase, so any Search Guard state that must reach data nodes has to be a header.
- That nested external-cluster test classes must be run via their outer class (`-Dtest=<OuterClass>`), otherwise the
  class rule is not initialised and the run fails with an NPE.
