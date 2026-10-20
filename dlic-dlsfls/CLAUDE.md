# dlic-dlsfls

Document-level security (DLS), field-level security (FLS) and field masking (FM). Proprietary (DLIC) module; copy
the license header from a neighbouring file. Deeper analysis: `docs/architecture/dlic-dlsfls.md`. The root
`CLAUDE.md` section "DLS/FLS" still applies.

## Entry points

- `DlsFlsModule` is the `SearchGuardModule`, registered by class name in `SearchGuardPlugin` (no Maven dependency
  from `security`). It wires: `DlsFlsValve` (post-privilege `SyncAuthorizationFilter`, coordinating node),
  `DlsFlsDirectoryReaderWrapper` (per shard reader open, data node), `DlsFlsSearchOperationListener` (query phase,
  data node), `FlsFieldFilter` (mappings, field caps), `FlsQueryCacheWeightProvider`, and the REST API
  `/_searchguard/config/authz_dlsfls`.
- Rules come from `roles` (`Role.Index` in `security`: `dls`, `fls`, `masked_fields`) and are compiled by
  `RoleBasedDocumentAuthorization`, `RoleBasedFieldAuthorization`, `RoleBasedFieldMasking` on top of
  `RoleBasedAuthorizationBase`. `DlsFlsProcessedConfig` holds the compiled state and is rebuilt on every config
  change; stateful (per concrete index) rules are rebuilt on cluster state changes.
- Module config `authz_dlsfls` (`DlsFlsConfig`): masking salt/personalisation/prefix, `dls.mode`
  (`ADAPTIVE` default, `LUCENE_LEVEL`, `FILTER_LEVEL`), `dls.allow_now`, `dls.force_min_doc_count_to_1`,
  `debug`, `metrics`. Unknown attributes are rejected, so removing one is a breaking change (`use_impl` is kept
  dead for that reason).
- `filter/DlsFilterLevelActionHandler` rewrites search/get/mget into a filtered search when DLS runs at filter
  level (always for terms-lookup queries); the rest of the request is marked with `_sg_filter_level_dls_done`.

## Running tests

```bash
mvn -o -pl dlic-dlsfls test
mvn -o -pl dlic-dlsfls test -Dtest='FlsIntTest#*multiRole*' -DfailIfNoTests=false   # parameterized: wildcard
```

- `DlsIntTest`, `DlsTest`, `FlsIntTest`, `FmIntTest`, `FmLogsDbIntTest` force the external-process cluster
  (LogsDB needs a native library). They download an ES distribution; set `-Dsg.tests.es_download_cache.dir`.
- `rerunFailingTestsCount=0` here, unlike the rest of the build: a flake fails the run.
- Test config is built with `TestSgConfig.DlsFls` and `TestSgConfig.Role` builders, not YAML.
- There is no mixed-version test. A change that alters rule semantics has no automated check for M-1 below.

## Three things that break when touched carelessly

1. **FLS and FM are enforced in several independent places and must agree.** Reader level
   (`lucene/DlsFlsDirectoryReader`: field infos, terms, doc values, points, norms, term vectors, `_field_names`),
   `_source` rewrite (`lucene/FlsStoredFieldVisitor.DocumentFilter`), mappings (`FlsFieldFilter`), masking of
   doc values and terms (`MaskedTermsEnum`). A rule change in `RoleBasedFieldAuthorization.FlsRule` must be
   checked at every one of them. The include-mode subtlety (`isObjectAllowedAssumingParentsAreAllowed`) exists
   only for the `_source` path.
2. **No user in the thread context means no filtering.** `DlsFlsDirectoryReaderWrapper#apply` returns the raw
   reader and `FlsFieldFilter` returns `ACCEPT_ALL` when `AuthInfoService#peekCurrentUser` is null. Any ES
   change that moves search work to another thread or node (batched query phase, chunked fetch phase) can drop
   the user. Read the comments in `SearchGuardFilter` (user header for search requests) and
   `SearchGuardInterceptor#isBatchedSuggestRequestToUnsupportedNode` before touching that area.
3. **Coordination between nodes is four string headers, forwarded by an allow-list in `security`.**
   `_sg_dls_mode`, `_sg_filter_level_dls_done`, `_sg_doc_whitelist`, `_sg_issuggest` (the last since 4.2.0).
   A new header needs `SearchGuardInterceptor#sendRequestDecorate` updated and is silently ignored by older nodes.
   Everything else (the actual DLS query, FLS rule, FM rule) is recomputed per data node from the user header and
   the node's local `roles`. A semantics change therefore gives per-shard divergence in a mixed cluster, not an
   error (M-1 in the deeper document).

## Halt before implementing

Stop and ask when a change touches `Role.Index` parsing, `DlsFlsConfig#parse`, `FlsRule` or `FieldMaskingRule`
semantics, a header name or its forwarding, `DlsFlsDirectoryReader#applyDlsHere`, or the no-user path. These are
the compatibility and mixed-cluster boundaries of this module.

## Behaviour worth knowing

- Default closed: no mapped roles, unknown index, or no matching rule means fully restricted. One matching role
  without a rule makes the index unrestricted (union across roles).
- Any validation error in `roles` or `rolesmapping` makes every request through `DlsFlsValve` fail with HTTP 500
  on that node (`blockAccessInCaseOfRoleOrMappingsConfigurationErrors`).
- Field masking only applies with license feature `COMPLIANCE` (`DlsFlsActionContext#hasFieldMasking`); the
  default hash is Blake2b-256 keyed by the configured salt and personalisation, so all nodes must share the config.
- The valve denies: transform with any restriction, `profile: true` with DLS, `min_doc_count: 0` terms aggregations
  with DLS (unless forced to 1), suggest with filter-level DLS. It forces `request_cache=false`, non-realtime
  get, and `map`/`direct` execution hints with FM.
- `authz_dlsfls` shares CType ord 10011 with `sessions` (phase 1 finding, decision pending).
