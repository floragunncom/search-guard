# Hotspots

Recurring traps in Search Guard: places where a change compiles and passes the usual tests but breaks at runtime,
on real nodes, or silently weakens security. Entries describe patterns, never facts about one version or one bump.

Who reads it:

- **Version bumps** check every entry marked "version bumps" (version-bump skill, Phase 4).
- **Code changes and analyses** (bug fixes, features, root-cause analysis) read the entries marked "code changes"
  whose "Search Guard" line names code they touch or investigate.
- **The compat-reviewer** checks the entries that match the diff it reviews.

Format per entry: what goes wrong and how it shows, how to check, where in Search Guard, and which kind of work it
applies to. "Added" and "Last hit" record when the entry was created and when it last caused work; they are
history, not instructions.

Maintenance: add an entry when work finds a trap that will recur; update "Last hit" when an entry caused work.
The version-bump skill edits this file as part of the bump's diff; any other task proposes changes in its report
instead of editing the file. Remove an entry only when the ES mechanism or the Search Guard code it describes no
longer exists, verified, and give the reason in the report. Never remove an entry just because it did not fire.
Keep entries short; merge similar ones when the file grows past about 25 entries.

## Lost user context when ES stashes the thread context

- **Goes wrong:** ES stashes the thread context and restores only headers it captured earlier. Search Guard keeps
  user, origin and remote address as transients, so the next hop runs without a user. Requests without a user get
  full authorization, and the DLS/FLS reader wrapper applies nothing. Shows as FLS or field-masking values in clear
  on multi-shard searches and missing compliance read logs; single-shard and embedded tests still pass.
- **Check:** `grep -rl "stashContext()" server/src/main/java | xargs grep -l "getHeaders()"` in the ES sources of
  both tags and compare; review every new entry. Run the multi-node suites (`DlsIntTest`, `FlsIntTest`,
  `FmIntTest`, `SqlAuthorizationReadOnlyIntTests`) on external-process clusters.
- **Search Guard:** `SearchGuardFilter` puts user, origin and remote address into the thread context as headers.
  Serialize the `SG_USER` transient, not a substituted special-context user (auth tokens), or token users gain
  their base roles on other nodes.
- **Applies to:** version bumps, code changes (any new hop, stash or stored context)
- Added: bump to 9.5.5 · Last hit: bump to 9.5.5

## A removed feature flag means the feature is now on by default

- **Goes wrong:** a feature that ES gated behind a `FeatureFlag` becomes default when the flag disappears, and can
  change request routing or execution paths that Search Guard hooks into.
- **Check:** diff `grep -rn "new FeatureFlag(" server/src/main/java` between the tags; every removed flag goes on the
  release-notes checklist.
- **Search Guard:** whatever the feature touches; the fetch-phase entry above started this way.
- **Applies to:** version bumps
- Added: bump to 9.5.5 · Last hit: bump to 9.5.5

## ES classes from packages the server module does not export

- **Goes wrong:** code compiles and passes embedded tests, but a real node dies with `IllegalAccessError`.
- **Check:** `grep "exports <package>" server/src/main/java/module-info.java` before using any ES class outside the
  usual action/index/search packages; external-process tests are the only ones that catch it.
- **Search Guard:** any new reference to ES internals, typically in transport or filter code.
- **Applies to:** version bumps, code changes (any new reference to ES classes)
- Added: bump to 9.5.5 · Last hit: bump to 9.5.5

## Return types that change to `BytesReference` or another `ToXContent`

- **Goes wrong:** a call to an overloaded helper silently binds to the `ToXContent` overload, compiles, and fails at
  runtime. The exception is often caught and only logged.
- **Check:** after any return-type change, grep every caller of overloaded helpers
  (`Utils.convertJsonToxToStructuredMap` and similar); read `ERROR` lines from Search Guard classes in test logs even
  when tests pass.
- **Search Guard:** audit log (`AbstractAuditLog`, `RequestResolver`), multi-tenancy interceptors.
- **Applies to:** version bumps, code changes (calls to overloaded helpers)
- Added: bump to 9.5.5 · Last hit: bump to 9.5.5

## Request bodies are not always JSON

- **Goes wrong:** ES may keep a request body in its original content type (YAML, CBOR, SMILE). Code that treats it
  as a JSON string writes binary or YAML into audit fields or fails to parse it.
- **Check:** every place that reads a request source; convert with the request's `xContentType()`, never with
  `utf8ToString()`. Test with a non-JSON body.
- **Search Guard:** audit log, multi-tenancy mapping interceptor.
- **Applies to:** version bumps, code changes (any code that reads a request source)
- Added: bump to 9.5.5 · Last hit: bump to 9.5.5

## New action types, enum constants and transport actions

- **Goes wrong:** a new action or a new constant in a `switch` either fails to compile or, worse, is authorized
  with the wrong privilege. Which privilege it requires is a security decision.
- **Check:** compare the ES action names and the enums Search Guard switches over between the tags; compare with
  X-Pack's mapping (read only, never reference X-Pack classes in production code).
- **Search Guard:** `ActionRequestIntrospector`, `Actions`, `static_action_groups.yml`. Add a test and list the
  privilege choice under "decisions" in the report and the baseline.
- **Applies to:** version bumps, code changes (features that add actions)
- Added: bump to 9.5.5 · Last hit: bump to 9.5.5

## Removed marker interfaces

- **Goes wrong:** Search Guard checks `instanceof <interface>`; ES removes the interface but keeps the behaviour on
  the classes.
- **Check:** list the former implementors with `javap` on the old jar; replace with explicit checks on exactly those
  classes and verify each still has the method.
- **Search Guard:** e.g. `DlsFlsValve` (realtime handling).
- **Applies to:** version bumps
- Added: bump to 9.5.5 · Last hit: bump to 9.5.5

## Pooling and reference counting

- **Goes wrong:** ES removes "unpooled" helpers or changes ref counting; copies share pooled buffers, which are
  released while Search Guard still uses them, or references leak.
- **Check:** mirror the old copy semantics (deep-copy `RefCounted` sources) and release exactly the references you
  created; look at nested hits (aggregation `top_hits`, completion suggestions) too.
- **Search Guard:** `SearchMapper` (multi-tenancy).
- **Applies to:** version bumps, code changes (any code that copies or wraps search hits)
- Added: bump to 9.5.5 · Last hit: bump to 9.5.5

## Shard requests built on the data node

- **Goes wrong:** with the batched query phase, the coordinator sends one `NodeQueryRequest` per data node
  (`indices:data/read/search[query][n]`) and the data node builds the `ShardSearchRequest`s itself, without any
  transport hop or action filter. Code in `SearchGuardRequestHandler` that derives state from a received
  `ShardSearchRequest` (e.g. the suggest marker for reader-level DLS) never runs for those shards; shards on the
  coordinating node itself still take the old per-shard path, so single-node and small tests pass.
- **Check:** anything the data node needs to know about a search must be a thread-context *header* put on the
  coordinator (the interceptor forwards only the headers listed in `SearchGuardInterceptor`); test on a
  multi-node external cluster with more shards than data nodes.
- **Search Guard:** `SearchGuardRequestHandler`, `DlsFlsValve` (`_sg_suggest` header), `DlsFlsDirectoryReader`.
- **Applies to:** version bumps, code changes (anything that inspects shard-level search requests)
- Added: bump to 9.5.5 · Last hit: bump to 9.5.5

## Interfaces Search Guard implements gain methods or parameters

- **Goes wrong:** new abstract methods or constructor parameters; the quick fix drops new data or invents semantics.
- **Check:** implement new methods with the same semantics as ES's own implementation and reuse ES helpers; carry new
  optional data through (e.g. resume info) instead of dropping it; tests may pass neutral values.
- **Search Guard:** `support/.../xcontent/` generators, response mappers in multi-tenancy.
- **Applies to:** version bumps
- Added: bump to 9.5.5 · Last hit: bump to 9.5.5
