# Baseline after the bump to Elasticsearch 9.5.5

State of this branch after the bump 9.4.3 -> 9.5.5 (October 2026). The next bump replaces this file. No baseline for
9.4.3 existed; the 9.4.3 state was reconstructed from the sources and the single squashed commit "Bump to 9.4.3".

## Decisions and why

- `delete_backing_index` (new operation of the modify data streams API) requires `indices:admin/data_stream/modify`
  on the data stream and on the backing index (like `remove_backing_index`) plus `indices:admin/delete` on the
  backing index. Reason: the operation deletes the index; the alias API's `remove_index` already requires the delete
  privilege. X-Pack only checks the modify privilege. Test: `DataStreamAddRemoveIndicesTest.deleteBackingIndex_*`.
- `SearchGuardFilter` puts user, origin and remote address into thread-context headers (`_sg_user_header`,
  `_sg_origin_header`, `_sg_remote_address_header`, same format the transport interceptor uses) before an action
  proceeds. Reason: the chunked fetch phase (default on since 9.5) stashes the thread context and restores only
  headers; without the headers the fetch runs without a user and the DLS/FLS reader wrapper does nothing. The header
  is written once per request from the `SG_USER` transient (not from a substituted special-context user).
- Suggest searches are marked with the header `_sg_suggest` by `DlsFlsValve` and forwarded by the interceptor.
  Reason: with the batched query phase (default on since 9.5) the data node builds the shard search requests itself,
  so `SearchGuardRequestHandler` cannot set the `_sg_issuggest` transient from a `ShardSearchRequest`; the transient
  is still set for the per-shard path (shards on the coordinating node, old data nodes).
- No new well-known actions were added for the new ES action names (see "Elastic's notes"); they fall under the
  generic rules (`cluster:` -> cluster privilege, `internal:` -> only from server nodes, unknown index-like requests
  -> privilege needed on `*`). `ActionCompletenessIntTest` is `@Ignore`d, so new actions are not flagged by tests.
- Search Guard's own dependency pins (`netty.version` 4.2.10.Final, `log4j.version` 2.25.4, `jackson.version`
  2.18.6) were left alone although ES moved to netty 4.1.138, log4j 2.26.1, jackson 2.21.x.

## Search Guard code that depends on ES internals

| Search Guard | ES internal | why it matters | test |
|---|---|---|---|
| `SearchGuardFilter` (headers), `SearchGuardInterceptor.ensureCorrectHeaders` | `TransportFetchPhaseCoordinationAction` restores only `ThreadContext.getHeaders()` captured in `SearchTransportService.sendExecuteFetch`; `search.fetch_phase_chunked_enabled` default true | lost user = full authorization on the data node | `FmIntTest`, `FlsIntTest`, `DlsIntTest` (external, 5 shards, 2 data nodes) |
| `SearchGuardRequestHandler` (`_sg_issuggest` transient), `DlsFlsValve` (`_sg_suggest` header), `DlsFlsDirectoryReader.isSuggest` | `SearchQueryThenFetchAsyncAction.NodeQueryRequest` / `[query][n]`; `search.batched_query_phase` default true; shards on the coordinator are not batched; a node with a single shard gets a plain `ShardSearchRequest` | reader-level DLS for suggesters | `DlsIntTest.search_suggest_completion` (6 shards, 20 docs per department) |
| `DlsFlsDirectoryReaderWrapper` | `IndexShard.acquireSearcherSupplier` applies the reader wrapper on every `acquireSearcher` (query and fetch phase) | wrapper reads the user from the thread context each time | DLS/FLS suites |
| `ActionRequestIntrospector` (`ModifyDataStreamsAction.Request`) | `DataStreamAction.Type` enum (`ADD_BACKING_INDEX`, `REMOVE_BACKING_INDEX`, `DELETE_BACKING_INDEX`), `Request.indices()` returns the backing indices | switch must cover all constants | `DataStreamAddRemoveIndicesTest` |
| `DlsFlsValve` realtime handling | `GetRequest`, `MultiGetRequest`, `TermVectorsRequest`, `MultiTermVectorsRequest.realtime(boolean)` (the `RealtimeRequest` interface is gone) | realtime reads bypass the reader wrapper | `DlsIntTest.get/mget/termvectors/mtermvectors` |
| `SearchMapper` (multi-tenancy) | `SearchHits.replaceHit` (takes ownership, releases the old hit), `SearchHit` pooling, `SearchHit.getSourceRef` backed by pooled buffers | wrong ref counting corrupts responses | `MultiTenancyRequestMappingTest.searchRequest_*` |
| `ObjectTreeXContent.Generator`, `AttributeValueFromXContent.Generator` | `XContentGenerator` interface (new: `writeString(Reader,int)`, `closeAllowIllFormed()`) | compile | `ObjectTreeXContentTest` |
| `AbstractAuditLog.logDocumentWritten`, `serializeRequestContent(PutMappingRequest)`, `RequestResolver` | `Engine.Index.source()` -> `SourceToParse.Source` (`originalBytes()`, `xContentType()`); `PutMappingRequest.source()` -> `BytesReference` + `xContentType()` | sources keep the request's content type | `ComplianceAuditlogTest.testWriteHistory` (JSON and YAML), `AuditlogTest.testPutMappingRequest_*` |
| `FrontendDataMigrationInterceptor` | `PutMappingRequest.source()`/`xContentType()` | mapping extension for multi-tenancy | `MultiTenancyRequestMappingTest` put-mapping tests |
| `SearchGuardSSLNettyHttpServerTransport.SSLHttpChannelHandler` | `Netty4HttpServerTransport.HttpChannelHandler.initChannel` builds the decoder/flow-control pipeline (request smuggling fix in 9.4.5) | the TLS handler is prepended, the rest is ES's | HTTP/TLS tests |
| `ActionRequestIntrospector` (`unknownActionRequest`) | requests that are neither `IndicesRequest` nor known composites | new request types (OTLP, reindex resume, ES\|QL datasets) need privileges on `*` | - |
| `support`, `dlic-fe-multi-tenancy` reindex callers | `BulkByPaginatedSearchResponse`, `BulkByPaginatedSearchTask` (renamed from `BulkByScroll*`) | compile | data migration step tests, `ProxyApiTest` |

## Deferred and open items

- Reindex task relocation on graceful node shutdown (new in 9.5): `indices:data/write/reindex/resume` with
  `ResumeBulkByPaginatedSearchRequest` (a `CompositeIndicesRequest` wrapping the original request, `getDelegate()`)
  is unknown to the introspector, so the relocated task is denied unless the user has the action on `*`. Effect: the
  reindex fails on shutdown as it did before 9.5; no escalation. Supporting it means introspecting the delegate like
  `ReindexRequest` and deciding which privilege the resume action needs.
- ES|QL data federation (`esql.federation.enabled`, experimental, off by default): dataset actions and external reads
  not evaluated.
- Columnar index modes (`columnar`, `logsdb_columnar`, tech preview): DLS/FLS/field masking untested; the test data
  uses `text` fields, which columnar mode rejects, so the DLS/FLS suites have no columnar variant.
- Kibana 9.5 index patterns (`.evaluation-*`, `significant_events-*`, third-party agent, XM Cyber, PingDirectory
  indices in X-Pack's `kibana_system`) are not in `SGS_KIBANA_SERVER`. Not part of the ES bump.
- Mixed cluster during a rolling upgrade: a 9.5.5 coordinator batches query requests to 9.4.x data nodes (they
  support the transport version and register the `[query][n]` handler); on those nodes the old Search Guard does not
  know the `_sg_suggest` header, so suggesters can return DLS-restricted documents from shards on old nodes until
  they are upgraded (also for CCS against remote clusters on 9.1-9.4 with `ccs_minimize_roundtrips=false`). The new
  node cannot gate this. Operational mitigation (upgrade note, team decision pending): set the dynamic ES cluster
  setting `search.batched_query_phase: false` while DLS-restricted users can use suggesters and old nodes remain.
- `ActionCompletenessIntTest` is `@Ignore`d ("to facilitate backporting"); the manual action-name diff (see commands
  below) replaces it.
- OTLP logs/traces endpoints are on by default now; they are authorized coarsely (privilege on `*`), like metrics
  before.

## Elastic's notes for this bump

Checklist with outcomes: `.ai-runs/version-bump/release-notes-checklist.md` of the bump session (not committed);
the items that needed code are listed above. Feature flags removed between the tags (features now on by default):
`chunked_fetch_phase_enabled`, `batched_query_phase`, `extended_doc_values_options`, `ars_formula_adjustment`,
`ignored_source_as_doc_values`, `time_series_temporality`, `reindex_pit_search`, `reindex_resilience`. New flags
(unsupported): `field_info_caching_directory`, `numeric_range_collect_pushdown`, `slice_indexing`,
`shard_results_skip_shard_search_request`. New action names (server, modules, x-pack):
`cluster:admin/ip_location/downloads/consumers/update`, `indices:admin/data_stream/auto_create_past_tsdb`,
`internal:index/shard/recovery/cancel_recoveries`, `indices:data/write/reindex/resume`,
`cluster:admin/encryption/reset`, `indices:admin/esql/dataset/*`, `cluster:admin/esql/dataset/authorize_datasource`,
`indices:data/write/otlp/logs`, `indices:data/write/otlp/traces`, `indices:admin/reshard*` (was `reshard/split*`).
New server exports: `org.elasticsearch.eirf`, `org.elasticsearch.lucene.search`, `org.elasticsearch.sourcebatch`;
removed: `org.elasticsearch.index.codec.vectors.reflect`. Security announcements with fixes in 9.4.4-9.5.5: 38, none
needed a Search Guard change (HTTP pipeline, data stream and simulate-bulk authorization, wildcard matching verified).

## Test baseline (full suite, `bash .claude/scripts/full-suite.sh`, external-process clusters with the prebuilt zip)

| module | run | failures | errors | skipped | flakes | note |
|---|---|---|---|---|---|---|
| support | 27 | 0 | 0 | 0 | 0 | |
| ssl | 25 | 0 | 0 | 0 | 0 | |
| security | 4266 | 0 | 0 | 7 | 0 | plus 4422 tests of the 11 external-process classes rerun alone (see note) |
| scheduler | 22 | 0 | 0 | 3 | 0 | |
| signals | 369 | 0 | 0 | 4 | 1 | `SeverityMappingTest` class error = known `ConfigVarRefreshAction` flake, passes alone; `SignalsTenantTest.shouldInitWatchWhichUsesTruststoreDuringStartupTime` flaky (passed on rerun) |
| dlic-security | 108 | 0 | 0 | 1 | 0 | 2 external-process classes rerun alone (20 tests) |
| dlic-auditlog | 128 | 0 | 0 | 4 | 0 | |
| dlic-dlsfls | 2357 | 0 | 0 | 0 | 0 | 5 external-process classes rerun alone (179 tests) |
| dlic-fe-multi-tenancy | 561 | 0 | 0 | 2 | 0 | |
| dlic-signals | 7 | 0 | 0 | 0 | 0 | |

Note: in the full run every external-process class errored at class level because `full-suite.sh` picked a stale
9.4.3 zip left in `plugin/target/releases` from the pre-bump build (the plugin cannot be installed into ES 9.5.5).
The "run" numbers above are the full run's numbers (the errored classes count as 1 each there); the classes were
rerun with the 9.5.5 zip and all passed. Delete old zips from `plugin/target/releases` before the full run.

## Commands to regenerate the ES lists

```bash
# sources of both tags into a scratch directory (server, modules, x-pack)
curl -sSL https://github.com/elastic/elasticsearch/archive/refs/tags/v<version>.tar.gz | tar -xz --wildcards \
  'elasticsearch-<version>/server/src/main/java/*' 'elasticsearch-<version>/modules/*/src/main/java/*' \
  'elasticsearch-<version>/x-pack/plugin/*/src/main/java/*'
# feature flags
grep -rhn "new FeatureFlag(" <tag>/server/src/main/java | sort
# action names
grep -rhoE '"(indices|cluster|internal):[A-Za-z0-9_/\[\]\*\.-]+"' <tag>/server <tag>/modules <tag>/x-pack --include=*.java | sort -u
# stashed contexts that restore headers only
cd <tag> && grep -rl "stashContext()" server/src/main/java | xargs grep -l "getHeaders()"
# server exports
grep -E "exports|opens" <tag>/server/src/main/java/module-info.java | sort
```
