# Module analysis: `dlic-dlsfls`

*Phase 2 pilot, written 2026-10-20 on branch `experimental-analysis` at commit `b339ec334`. Scope: `dlic-dlsfls/`,
plus the places in `security/` that wire the module in. References are `path#Symbol`; a line number is given only
where no symbol exists. Status: draft, not yet reviewed by a maintainer.*

## Summary for maintainers

*This section is written for people. The tables below are the authoritative part and are what an AI assistant
reads. Review means: confirm the claims here, then spot-check the rows they point to. Re-read this section when
`Role.Index`, `DlsFlsValve`, the `lucene/` package or the header allow-list in `SearchGuardInterceptor` change.*

**What this covers.** The enterprise module that enforces document-level security (DLS), field-level security (FLS)
and field masking (FM). It reads the `dls`, `fls` and `masked_fields` entries of index permissions in `roles`, plus
its own small config document `authz_dlsfls`, and enforces them in eight places spread over the coordinating node
and the data nodes.

**What you need to know.**

- Enforcement is recomputed on every data node from two inputs: the user carried in the `_sg_user_header`
  transport header and that node's local copy of the `roles` configuration. The computed restriction itself never
  travels between nodes. Only four small flags do: DLS mode, "filter-level DLS already done", "this is a suggest
  request" and the term-lookup document whitelist.
- Consequence for rolling upgrades: a change to how rules are evaluated (the FLS parent-object handling of
  2025-09 and the multi-role fix of 2026-08 are examples) produces different per-shard results in a mixed cluster
  rather than errors. The coordinator merges them silently.
- Consequence for safety: if a thread opens a shard reader without a user in the thread context, the reader is
  returned unfiltered. That is by design for internal requests, and it is the failure mode that the ES 9.5.2
  chunked fetch phase and the ES 9.5 batched query phase both triggered. Any Elasticsearch change to how search
  phases move between threads or nodes has to be checked against this path first.
- The module fails closed on configuration problems in a way that affects everyone: any validation error in
  `roles` or `rolesmapping` makes every request that reaches the DLS valve fail with HTTP 500 on that node.
- Field masking is the only part gated by a license feature in this module. Without the `COMPLIANCE` feature,
  masking rules are silently not applied at the reader level.
- Deleting or renaming a config attribute of `authz_dlsfls` is a breaking change: unknown attributes are rejected,
  which is why the dead `use_impl` key is still accepted.
- The config type `authz_dlsfls` shares its numeric identifier 10011 with `sessions`. This is inherited from phase
  1 and is a decision for the maintainers, see D-5 below.

**What to review.**

- The invariants table in section 3, especially rows I-1 to I-4 (default-closed behaviour) and I-8 (missing user
  means unfiltered reader).
- The mixed-cluster table in section 6, row M-3 (config attribute added in a newer version) which is marked
  UNVERIFIED and is the one hazard I could not settle from code alone.
- The open questions in section 8. Q-2 and Q-3 block the support matrix for this module.

## 1. Scope and method

| | |
|---|---|
| Sources read in full | all 23 files under `dlic-dlsfls/src/main/java` (4,342 lines), `dlic-dlsfls/pom.xml` |
| Sources read in part | `security/.../SearchGuardPlugin.java` (`onIndexModule`, `getActionFilters`, `getFieldFilter`, enterprise module list), `security/.../filter/SearchGuardFilter.java` (filter chain, user header for search), `security/.../transport/SearchGuardInterceptor.java` (header allow-list, remote cluster stripping, batched suggest gate), `security/.../transport/SearchGuardRequestHandler.java` (receiving side), `security/.../authz/indices/SearchGuardDirectoryReaderWrapper.java`, `security/.../authz/config/Role.java` (`Role.Index`, `FlsPattern`, `FieldMaskingExpression`), `security/.../authz/DocumentWhitelist.java`, `security/.../authc/AuthInfoService.java`, `security/.../support/ConfigConstants.java` |
| Tests | class list and `@Test` counts only; test bodies not read except where cited |
| Not read | `Blake2bDigest.java` beyond its API (a Bouncy Castle derived hash implementation), `support/.../Meta`, codova `Template` rendering |
| Git history | `git log -- dlic-dlsfls` (94 commits since 2021), `git log -S` for header introduction dates |

## 2. Purpose and entry points

| Item | Fact | Reference |
|---|---|---|
| Module class | `DlsFlsModule`, registered by fully-qualified name in the enterprise list; no compile-time reference from `security` | `dlic-dlsfls/.../DlsFlsModule.java#DlsFlsModule`, `security/.../SearchGuardPlugin.java` enterprise module list |
| Capabilities advertised | `dls`, `fls`, `field_masking` | `DlsFlsModule.java#getCapabilities` |
| Maven | `dlic-search-guard-flx-dlsfls`, depends on `security` and `support` at `provided` scope; `ssl` and the `tests` classifiers of `security` and `support` at `test` scope | `dlic-dlsfls/pom.xml` |
| License | proprietary (DLIC) header on most files; `DlsFlsConfigApi`, `FlsFieldFilter`, `FlsQueryCacheWeightProvider`, `RoleBasedDocumentAuthorization`, `RoleBasedFieldAuthorization`, `RoleBasedFieldMasking` carry an Apache 2 header; several files include OpenSearch-derived code with attribution | file headers |
| Plugin zip | included in both the default and the `quick` assembly | `plugin/pom.xml` (two `dlic-search-guard-flx-dlsfls` entries) |
| Settings | one: `searchguard.dls_fls.provide_thread_context_authz_hash`, default `false`, node-scope, static | `DlsFlsModule.java#PROVIDE_THREAD_CONTEXT_AUTHZ_HASH` |

### 2.1 Extension points implemented

| `SearchGuardModule` method | Implementation | Where it runs |
|---|---|---|
| `createComponents` | builds `DlsFlsBaseContext`, `DlsFlsValve`, `DlsFlsSearchOperationListener`, `FlsFieldFilter`, `FlsQueryCacheWeightProvider`, the reader wrapper factory; subscribes to config changes, license changes and cluster state | every node |
| `getSyncAuthorizationFilters` | `DlsFlsValve` (post-privilege-evaluation) | node executing the top-level action |
| `getDirectoryReaderWrappersForNormalOperations` | `DlsFlsDirectoryReaderWrapper` per `IndexService` | data node, per shard reader open |
| `getSearchOperationListeners` | `DlsFlsSearchOperationListener` | data node, query phase |
| `getFieldFilters` | `FlsFieldFilter` | node answering mapping and field-caps requests |
| `getQueryCacheWeightProviders` | `FlsQueryCacheWeightProvider` | data node |
| `getRestHandlers`, `getActions` | `DlsFlsConfigApi` (`GET`/`PUT`/`PATCH /_searchguard/config/authz_dlsfls`, actions `cluster:admin:searchguard:config/authz_dlsfls/{get,put,patch}`) | any node |
| `getSettings` | the one setting above | |

Wiring on the `security` side: `SearchGuardPlugin.java#onIndexModule` installs `SearchGuardDirectoryReaderWrapper`
with the module's wrapper in the "normal operations" list, forces a query cache provider when any weight provider
exists, and adds the search operation listener. `SearchGuardPlugin.java#getFieldFilter` ANDs all module field
predicates. `SearchGuardPlugin.java#getActionFilters` passes the sync filters to `SearchGuardFilter`.

## 3. Invariants

Claims that code elsewhere depends on. Each one should become a test in phase 3 where it is not one already.

| ID | Invariant | Where it is enforced | Where it is relied upon |
|---|---|---|---|
| I-1 | A user with no mapped roles is fully restricted (`MATCH_NONE`, `DENY_ALL`, `MASK_ALL`). | `RoleBasedAuthorizationBase.java#getRestrictionImpl` first branch | all enforcement points |
| I-2 | An index unknown to the node's metadata is treated as restricted. | `RoleBasedAuthorizationBase.java#hasRestrictions` (`hasNonExistingObjects`, `!index.exists()`) | `DlsFlsValve` |
| I-3 | Restrictions union across roles: one matching role without a rule makes the index unrestricted; otherwise the rules of all matching roles are joined. | `RoleBasedAuthorizationBase.java#getRestrictionImpl`, `FlsRule.merge`, `FieldMaskingRule.MultiRole#get` | root `CLAUDE.md` |
| I-4 | The action being performed is not consulted when collecting DLS/FLS/FM rules; only mapped roles and index patterns are. | `RoleBasedAuthorizationBase.java#getRestrictionImpl` | root `CLAUDE.md` |
| I-5 | The stateful rule path (per concrete index, alias, data stream) and the static fallback path must give the same answer; the stateful path is used only when `StatefulRules#covers` the object. | `RoleBasedAuthorizationBase.java#getRestrictionImpl` (`statefulRules = null` fallback) | correctness after index creation before the async rebuild lands |
| I-6 | FLS patterns within one role are order-sensitive (later pattern wins); an include-only list gets an implicit `~*` first, an exclude-only list an implicit `*` first. | `RoleBasedFieldAuthorization.java#FlsRule.SingleRole` constructor | all FLS enforcement points |
| I-7 | An include rule for `a.b.c` implicitly allows traversing objects `a` and `a.b`; those implicit object rules are dropped again where an explicit exclusion exists. | `RoleBasedFieldAuthorization.java#FlsRule.SingleRole#getObjectOnlyPatterns`, `Role.java#FlsPattern#getParentObjectPatterns` | `FlsStoredFieldVisitor.DocumentFilter#copy` |
| I-8 | No user in the thread context means no filtering: the reader wrapper returns the raw reader and the field filter returns `ACCEPT_ALL`. | `DlsFlsDirectoryReaderWrapper.java#apply`, `FlsFieldFilter.java#apply` | internal requests; **hazard** for any ES path that drops the user, see M-5 |
| I-9 | Admin-certificate users and `_sg_conf_request` requests bypass the module's reader wrapper entirely. | `SearchGuardDirectoryReaderWrapper.java#isAdminAuthenticatedOrInternalRequest` | config index access |
| I-10 | `.keyword` suffixes are stripped before FLS and FM pattern matching. | `FlsRule#stripKeywordSuffix`, `FieldMaskingRule#stripKeywordSuffix`, `FlsFieldFilter#removeSuffix` | keyword sub-fields follow their parent |
| I-11 | Built-in metadata fields plus `_primary_term` are always visible regardless of FLS. | `DlsFlsDirectoryReader.java` `META_FIELDS`, `FlsStoredFieldVisitor.java` `META_FIELDS` | ES internals |
| I-12 | Any validation error in `roles` or `rolesmapping` makes the valve throw HTTP 500 for every request. | `DlsFlsValve.java#blockAccessInCaseOfRoleOrMappingsConfigurationErrors` | fail-closed policy; tested by `InvalidRolesAndMappingConfigurationTest` |
| I-13 | Field masking is applied only when the license has `Feature.COMPLIANCE`. | `DlsFlsActionContext.java#hasFieldMasking`, `DlsFlsModule.java` license subscription | reader-level FM |
| I-14 | The default masking hash is Blake2b-256 keyed with `field_anonymization.salt` and `.personalisation`; defaults are the hard-coded constants in `DlsFlsConfig.FieldMasking`. | `RoleBasedFieldMasking.java#Field#blake2bHash` | masked values must be identical on every node and across restarts |
| I-15 | `now` in a DLS query is rejected unless `dls.allow_now` is set. | `DlsFlsDirectoryReaderWrapper.java#nowSupplier` | cache safety |
| I-16 | A DLS query containing a terms-lookup query forces filter-level mode in `ADAPTIVE` mode. | `DlsFlsValve.java#apply`, `DlsRestriction.java#containsTermLookupQuery` | `DlsFilterLevelActionHandler` |
| I-17 | Date-math index patterns with a DLS/FLS/FM rule are silently unsupported (only patterns without a rule are collected). | `RoleBasedAuthorizationBase.java#StaticRules` constructor, comment "Date math for rules is not supported" | nobody; see Q-8 |

## 4. Data flow

### 4.1 Configuration intake

| Step | Fact | Reference |
|---|---|---|
| Config document | `authz_dlsfls`, CType ord **10011** (collides with `sessions`), storage OPTIONAL, arity SINGLE, entry id `default` | `DlsFlsConfig.java#TYPE`; phase 1 `persisted-schemas.md` |
| Attributes | `field_anonymization{salt (32 hex chars = 16 bytes), personalisation (padded/cut to 16 bytes), prefix}`, `debug`, `metrics`, `use_impl` (consumed and ignored since the legacy implementation was removed 2024-11), `dls.allow_now`, `dls.mode` (`ADAPTIVE` default, `LUCENE_LEVEL`, `FILTER_LEVEL`), `dls.force_min_doc_count_to_1` | `DlsFlsConfig.java#parse` |
| Unknown attributes | rejected (`checkForUnusedAttributes`) | `DlsFlsConfig.java#parse` |
| Role-side schema | `Role.Index`: `dls` (templated query string), `fls` (list of patterns, `~` or `!` prefix excludes), `masked_fields` (list of `<pattern>[::<MessageDigest algo>]` or `<pattern>::/regex/::replacement[::...]`) | `security/.../Role.java#Index`, `#FlsPattern`, `#FieldMaskingExpression` |
| Rebuild trigger | every config change rebuilds `DlsFlsProcessedConfig` (three `RoleBased*` objects over `roles` and current `Meta`); the previous instance's async updater is cancelled | `DlsFlsModule.java#createComponents` (`subscribeOnChange`), `DlsFlsProcessedConfig.java#createFrom` |
| Rebuild failure | exception → `DlsFlsProcessedConfig.DEFAULT` (authorization objects `null`) and component state FAILED; the valve then throws on the first request | `DlsFlsProcessedConfig.java#createFrom` catch block |
| Cluster state changes | `updateIndicesAsync` rebuilds the stateful rules on the generic thread pool, gated by metadata version, with a 100 ms pause after ten consecutive rebuilds | `DlsFlsProcessedConfig.java#updateIndicesAsync` |
| Roles with parse problems | caught per role, role ignored, component state PARTIALLY_INITIALIZED | `RoleBasedAuthorizationBase.java#StaticRules`, `#StatefulRules.Index` |

### 4.2 Request path, coordinating node

`SearchGuardFilter` runs `DlsFlsValve` after privilege evaluation succeeded. The valve:

| Step | Behaviour | Reference |
|---|---|---|
| 1 | If `_sg_filter_level_dls_done` is set, return OK (this is the inner search issued by the filter-level handler). | `DlsFlsValve.java#apply` |
| 2 | Throw 500 if roles/rolesmapping have validation errors (I-12). | |
| 3 | If the request carries a `SpecialPrivilegesEvaluationContext` with its own roles (auth tokens with frozen privileges), build throw-away `RoleBased*` objects over those roles without metadata, i.e. static path only. | `DlsFlsValve.java#apply`; providers: `security/.../InternalAuthTokenProvider`, `dlic-security/.../AuthTokenService` |
| 4 | Compute `hasRestrictions` for DLS, FLS, FM over the resolved local indices. Transform preview/put is denied if any applies. | |
| 5 | No restrictions: write `_sg_dls_fls_authz: 0` (if enabled) and return OK. | `ThreadContextAuthzHashProvider.java#noRestrictions` |
| 6 | Suggest in a search request: put `_sg_issuggest: true` header (needed because the ES 9.5 batched query phase hides the `SearchRequest` from the data node). | `DlsFlsValve.java#apply` |
| 7 | Decide filter-level vs Lucene-level DLS by `dls.mode`, the `_sg_dls_mode` header, and I-16. In `ADAPTIVE`, the decision is pinned in `_sg_dls_mode` for the rest of the request. | `DlsFlsValve.java#setDlsModeHeader` |
| 8 | Write `_sg_dls_fls_authz: sha256(...)` (if enabled). | `ThreadContextAuthzHashProvider.java#restrictions` |
| 9 | Request surgery: `realtime(false)` for get/mget/termvectors; `request_cache=false` for search; deny `min_doc_count: 0` terms aggregations unless `dls.force_min_doc_count_to_1`; with FM force `execution_hint: map` on terms, significant_terms, diversified_sampler and `direct` on cardinality; deny `profile: true` with DLS; deny suggest with filter-level DLS. | `DlsFlsValve.java#apply` |
| 10 | Filter-level DLS with a non-empty restriction: hand off to `DlsFilterLevelActionHandler`, which returns `INTERCEPTED`. | |

`DlsFilterLevelActionHandler`:

| Step | Behaviour | Reference |
|---|---|---|
| Pass-through | `cluster:*`, `searchguard:cluster:*`, template actions, scroll, search/msearch templates, `MultiSearchRequest` return OK untouched (comment: "DLS will be handled on a lower level"). | `DlsFilterLevelActionHandler.java#handle(Action, ...)` |
| Query construction | One `bool` with `minimum_should_match: 1`; for each local index a `should` clause of its DLS queries, scoped with a `term` on `_index` when more than one index or a wildcard is involved; unrestricted and remote indices get a plain `_index` term so they pass. Terms-lookup queries add their lookup documents to `_sg_doc_whitelist`. | `#createQueryExtension` |
| Search | original query ANDed in, re-issued via the node client inside a stored context carrying `_sg_filter_level_dls_done` and the whitelist; response passed to the original listener. | `#handle(SearchRequest, ...)` |
| Get / multi-get | rewritten into a search by `_id` (and `_index`), hits converted back into `GetResult`s; metadata vs document fields split using the local `MapperService`. | `#handle(GetRequest, ...)`, `#searchHitToGetResult` |
| CCS shard resolution | `ClusterSearchShardsRequest` and `SearchShardsRequest` fail with "Filter-level DLS via cross cluster search is not available for scrolling and minimize_roundtrips=true". | `#handle(SearchShardsRequest, ...)` |

### 4.3 Request path, data node

| Point | Behaviour | Reference |
|---|---|---|
| Reader wrapper | For every shard reader open: get the user from the thread context (I-8), evaluate DLS (skipped if `_sg_filter_level_dls_done`), FLS and FM for this index; if a document whitelist for this index is present, lift FLS and FM; build a `ConstantScoreQuery` from the DLS restriction with a fresh `SearchExecutionContext`; wrap in `DlsFlsDirectoryReader`. | `DlsFlsDirectoryReaderWrapper.java#apply` |
| Leaf reader, FLS | Filters `FieldInfos`, `terms`, every doc-values accessor including `DocValuesSkipper`, points, norms, term vectors, and the `_field_names` terms. | `DlsFlsDirectoryReader.java#FilterLeafReader` |
| Leaf reader, FM | Wraps binary, sorted and sorted-set doc values and their `TermsEnum` so `lookupOrd`/`term` return the hash; `next()` deliberately not masked. | `DlsFlsDirectoryReader.java#MaskedTermsEnum` |
| Leaf reader, DLS | Live-doc bits computed from the DLS query **only** when the action is not a search (`indices:data/read/search*`) or is a suggest; the action name comes from the `_sg_action_name` transient set by `SearchGuardRequestHandler`. | `DlsFlsDirectoryReader.java#applyDlsHere`, `DlsGetEvaluator.java` |
| Search query phase, DLS | For search actions the DLS query is ANDed into the parsed query (`ConstantScore`), then `preProcess()`; skipped for internal requests, `FILTER_LEVEL` mode, done flag, suggest, admin, and for terms-lookup queries in `ADAPTIVE` mode (scroll loses the done flag, comment in code). | `DlsFlsSearchOperationListener.java#onPreQueryPhase` |
| Stored fields and `_source` | `FlsStoredFieldVisitor` drops disallowed stored fields and rewrites `_source` token by token with `DocumentFilter`; masked string values are hashed; a `MaskedFieldsConsumer` delegate (audit log) is told which fields were masked. | `FlsStoredFieldVisitor.java`, `support/.../dfm/MaskedFieldsConsumer.java` |
| Mapping and field caps | `FlsFieldPredicate` over `FlsRule#isAllowedRecursive`; cache key prefix `sg_fls_<ruleHash>`. | `FlsFieldFilter.java#FlsFieldPredicate` |
| Query cache | Returns the weight (i.e. marks the query as user-dependent) when FLS or FM applies to the index for this user. | `FlsQueryCacheWeightProvider.java#apply`, `SearchGuardPlugin.java#onIndexModule` |

### 4.4 Thread-context headers and transients the module depends on

| Name | Kind | Written by | Read by | Forwarded between nodes | Since |
|---|---|---|---|---|---|
| `_sg_user_header` | header | `security` (interceptor; `SearchGuardFilter` for search requests since ES 9.5.2) | `security` → transient `_sg_user` → `AuthInfoService#peekCurrentUser` | yes | long-standing |
| `_sg_dls_mode` | header | `DlsFlsValve` | `DlsFlsValve` | yes (stripped for remote clusters) | 2021-07 |
| `_sg_filter_level_dls_done` | header | `DlsFilterLevelActionHandler` | valve, reader wrapper, search listener | yes (stripped for remote clusters) | 2021-07 |
| `_sg_doc_whitelist` | header | `DlsFilterLevelActionHandler` via `DocumentWhitelist#applyTo` | reader wrapper, `PrivilegesEvaluator` | yes (stripped for remote clusters) | 2021-07 |
| `_sg_issuggest` | header or transient | `DlsFlsValve` (header), `SearchGuardRequestHandler` (header, from `ShardSearchRequest`) | `DlsFlsDirectoryReader#isSuggest` | yes | 2026-09-17, first in `sg-flx-4.2.0-es-9.5.2` |
| `_sg_dls_fls_authz` | header | `ThreadContextAuthzHashProvider` (opt-in) | nothing inside this repository | yes | 2025-05 |
| `_sg_conf_request` | header | `security` | `DlsFlsBaseContext#isInteralRequest`, base reader wrapper | yes | long-standing |
| `_sg_action_name` | transient | `SearchGuardRequestHandler` | `DlsFlsDirectoryReader#getRuntimeActionName` | no (transient) | |
| `_sg_dls_query`, `_sg_fls_fields`, `_sg_masked_fields` | header | nothing in `main` code | only the interceptor's CCS response relay | yes | legacy; phase 1 open question |

The allow-list that forwards these lives in `security/.../SearchGuardInterceptor.java#sendRequestDecorate`. A new
header introduced in this module is a change to `security` as well.

## 5. Interaction with other modules

| Module | Direction | Nature |
|---|---|---|
| `security` | depends on | `SearchGuardModule` contract, `Role` schema, `CType`, `ConfigurationRepository`, `PrivilegesEvaluationContext`, `SpecialPrivilegesEvaluationContext`, `DocumentWhitelist`, `QueryBuilderTraverser`, `ConfigConstants` header names, the interceptor allow-list, `SearchGuardDirectoryReaderWrapper` ordering (normal-operation wrappers before all-operation wrappers; admin users skip normal-operation wrappers) |
| `support` | depends on | `Meta` index metadata abstraction, `ComponentState` and metrics, `MaskedFieldsConsumer`, `searchsupport.queries.Query` |
| `dlic-auditlog` | consumer | `ComplianceAwareStoredFieldVisitor` implements `MaskedFieldsConsumer` and receives masked-field callbacks from `FlsStoredFieldVisitor`; the audit log's own reader wrapper runs in the all-operations list after this module's | 
| `dlic-security` | test-scope dependency on this module; runtime: `AuthTokenService` supplies a `SpecialPrivilegesEvaluationContext` with frozen roles that this module evaluates without stateful rules | |
| Elasticsearch | depends on | `IndexModule#setReaderWrapper`, `SearchOperationListener#onPreQueryPhase`, `FieldPredicate`, `QueryCache` provider, `SearchExecutionContext`, `TermsQueryBuilder#termsLookup`, aggregation builders' `executionHint`; the batched query phase (`NodeQueryRequest`) and chunked fetch phase (ES 9.5.2) are the two most recent ES-side changes that required code in `security` to keep this module correct |

## 6. Mixed-cluster and compatibility relevance

| ID | Situation | Behaviour today | Verified? |
|---|---|---|---|
| M-1 | Nodes run different plugin versions with different rule-evaluation semantics (FLS parents, multi-role merge, keyword handling). | Each data node applies its own semantics to its shards; the coordinator merges. No error, no warning. Results differ per shard until the upgrade completes. | yes, by construction (section 4.3) |
| M-2 | A data node has the module disabled or absent (community build, `searchguard.modules.disabled`). | No reader wrapper, no search listener on that node: shards served unfiltered even though the coordinator computed restrictions. | phase 1 README item 8; not re-tested here |
| M-3 | A newer node writes `authz_dlsfls` with an attribute older nodes do not know. | Older nodes reject the document on parse. What they then use (previous document, or `DlsFlsConfig.DEFAULT` with the hard-coded masking salt and `ADAPTIVE` mode) depends on `ConfigurationRepository`, not on this module. If `DEFAULT`, masked values differ between nodes and `dls.mode` differs. | **UNVERIFIED**; Q-2 |
| M-4 | A newer node writes a role using a `fls`/`masked_fields` syntax older nodes cannot parse. | The `Role` parser throws; on the old nodes the roles document carries validation errors, and by I-12 every request through the valve fails with HTTP 500 on those nodes until the document is fixed or the nodes are upgraded. | valve part verified; whether a single bad role marks the whole document is a `security` question (phase 1 persisted-schemas: nested unknown keys throw) |
| M-5 | Elasticsearch moves a search phase to a thread or node where the SG user transient is absent. | Reader is unfiltered (I-8). Two shipped examples: chunked fetch phase (fixed by putting `_sg_user_header` on the context in `SearchGuardFilter`) and batched query phase for suggest (fixed by `_sg_issuggest` plus a fail-closed gate in the interceptor for target nodes before ES 9.5.0). | yes, both in code with explanatory comments |
| M-6 | Batched suggest request to a node that does not understand `_sg_issuggest`. | The gate keys on the **ES** version of the target node (`Version.V_9_5_0`), not on the plugin version. That is sufficient today because no plugin older than 4.2.0 was ever built for ES 9.5 (tags: `sg-flx-4.2.0-es-9.5.2` is the first 9.5 tag). It would stop being sufficient if a 4.1.x build for ES 9.5 were ever produced. | yes (tags listed 2026-10-20) |
| M-7 | A new coordination header is added by this module. | Old data nodes ignore it; the feature it carries is silently off on them. The interceptor allow-list must be extended in `security`, and the same-change rule from the plan applies. | yes |
| M-8 | Cross-cluster search. | DLS/FLS headers are stripped for remote connections; the remote cluster authenticates the user header and applies its own roles. Filter-level DLS refuses the shard-resolution requests (section 4.2). | code read; not tested here |
| M-9 | `authz_dlsfls` attribute removal. | Breaking: unknown attributes are rejected on parse. `use_impl` is kept for this reason. | yes |
| M-10 | CType ord 10011 collision with `sessions`. | Inherited; affects `ConfigVersion` serialisation in auth tokens, not this module's logic. | phase 1 |

Halt triggers for an AI working in this module, derived from the above: any change to `Role.Index` parsing, to
`FlsRule` or `FieldMaskingRule` semantics, to `DlsFlsConfig#parse`, to a header name or its forwarding, to
`applyDlsHere`, or to the no-user behaviour in `DlsFlsDirectoryReaderWrapper#apply`.

## 7. Tests

| Fact | Value | Reference |
|---|---|---|
| Test classes / `@Test` methods | 23 / 190 | `dlic-dlsfls/src/test/java` |
| Pure unit tests (no cluster) | `Blake2bDigestTest`, `DlsFlsProcessedConfigTest` (Mockito), `FieldMaskingRuleTest`, `FlsFieldPredicateTest`, `RoleBasedDocumentAuthorizationTest` (26), `RoleBasedFieldAuthorizationTest` (suite, 19), `RoleBasedFieldAuthorizationSingleRoleTest`, `RoleBasedFieldMaskingTest` (suite, parameterized), `ThreadContextAuthzHashProviderTest`, `DocumentFilterTest` | |
| Embedded `LocalCluster` | `DlsMinDocCountTest`, `DlsWriteIntTest`, `FlsKeywordTest`, `InvalidRolesAndMappingConfigurationTest`, `InvalidRolesAndMappingConfigurationWithDisabledDlsTest`, `ThreadContextAuthzHashProviderIntTest`, `DlsReadOnlyIntTests`, `DlsRemoteAddressRoleMappingIntTest` | |
| Forced external-process cluster | `DlsIntTest`, `DlsTest`, `FlsIntTest`, `FmIntTest` (two clusters, one with `search.fetch_phase.chunked` enabled), `FmLogsDbIntTest`; reason: LogsDB index mode needs a native library | `.useExternalProcessCluster()` in each |
| Parameterized classes (use `-Dtest='X#*name*'`) | `DlsTest`, `FlsIntTest`, `FmIntTest`, `RoleBasedFieldMaskingTest` inner class | |
| Surefire overrides | `rerunFailingTestsCount=0` (flakes are failures here), `forkCount=3`, `-Xmx3072m`, JaCoCo always on | `dlic-dlsfls/pom.xml` |
| CI job | `test_dlic_dlsfls` via `ci/run_tests.sh dlic-dlsfls` | `ci/backend-unit-tests.yml` |
| Mixed-version coverage | none | |
| Test config | built with `TestSgConfig.DlsFls` (`debug`, `metrics`, `dls.allow_now`, `dls.mode`, `field_anonymization.prefix`) and `TestSgConfig.Role` `dls`/`fls`/`maskedFields`; YAML resources only for action groups, blocks, tenants | `security/src/test/.../TestSgConfig.java#DlsFls` |

Invariants without a dedicated test, candidates for phase 3: I-5 (stateful equals static), I-8 (no user means
unfiltered reader, as a guard test rather than a feature test), I-13 (FM off without licence), M-7 (header
allow-list contains every header this module writes).

## 8. Open questions for human review

Fact questions get an answer in the slot. Decisions go to `docs/architecture/decisions.md`. Defects get an issue
link.

1. Is DLS or FLS (as opposed to field masking) gated by a license feature anywhere outside this module, e.g. in
   `SearchGuardModulesRegistry` or the license repository? `ComponentState#requiresEnterpriseLicense` only reports
   state.
   - **Answer:** _pending_
2. (M-3, blocking for the support matrix) When a node fails to parse `authz_dlsfls`, does it keep the last good
   document or fall back to `DlsFlsConfig.DEFAULT`? If the latter, a config written by a newer node changes the
   masking salt on older nodes.
   - **Answer:** _pending_
3. (M-4, blocking) Does one unparseable role mark the whole `roles` document as having validation errors, and is the
   resulting HTTP 500 on every request the intended behaviour during a rolling upgrade?
   - **Answer:** _pending_
4. Is `_sg_dls_fls_authz` consumed by any external component (Kibana plugin, a customer plugin)? Nothing in this
   repository reads it. This decides whether its format is a contract.
   - **Answer:** _pending_
5. Can `DlsFlsDirectoryReader#getRuntimeActionName` ever see a `null` transient (a reader opened outside a
   transport handler, e.g. by an ES-internal maintenance task)? It would throw `NullPointerException` in
   `applyDlsHere`.
   - **Answer:** _pending_
6. Are the legacy `_sg_dls_query`, `_sg_fls_fields`, `_sg_masked_fields` response headers still produced by any
   remote cluster version in support (same as phase 1 wire-surface question)?
   - **Answer:** _pending_
7. Which supported client paths hit the filter-level CCS refusal (`SearchShardsRequest`)? Is the error message
   ("not available for scrolling and minimize_roundtrips=true") still accurate for ES 9.5?
   - **Answer:** _pending_
8. I-17: is "date math index patterns with a DLS/FLS/FM rule are ignored" documented for users, or is it a defect?
   - **Answer:** _pending_
9. `use_impl`: is there a date after which configs containing it can be considered gone, so the key can be
   rejected again?
   - **Answer:** _pending_
10. M-6: is "no plugin older than 4.2.0 on ES 9.5" a rule the release process guarantees, so the ES-version gate
    can stay, or should the gate also check the plugin version of the target node?
    - **Answer:** _pending_

## 9. Proposed decisions

- **D-5 (new, for `decisions.md`):** resolve the 10011 ord collision for `authz_dlsfls` or `sessions`, with a
  migration note for `ConfigVersion` consumers. Raised by this analysis; same facts as phase 1.
- **D-6 (new):** policy for rule-semantics changes (M-1): accept silent per-shard divergence during rolling
  upgrades, or require such changes to be gated on a cluster-wide minimum plugin version.

## 10. Known limits of this pass

- Test bodies were not read; the counts and cluster modes come from greps.
- `ConfigurationRepository` behaviour on invalid documents (Q-2, Q-3) was not traced; it belongs to the `security`
  module analysis.
- Performance characteristics (stateful rebuild cost on large clusters, `DlsGetEvaluator` bitset per reader open)
  were not assessed.
- Compared with the root `CLAUDE.md` section on DLS/FLS, this document adds the header contract, the no-user
  path, the filter-level handler, the config schema, the license gate and the mixed-cluster table; it does not
  contradict anything there.
