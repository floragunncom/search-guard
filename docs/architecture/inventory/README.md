# Cross-cutting inventories (phase 1)

Phase 1 of `docs/architecture/ai-readiness-plan.md`. Six documents, each written in one pass by a separate agent on
2026-10-19 against branch `experimental-analysis`, commit `374449897`. Every row carries a `path:line` reference.
Items marked UNVERIFIED in a document were inferred, not read. None of these documents has been reviewed by a
maintainer yet.

## Documents

| Document | Covers | Lines | Written at | Review status |
|---|---|---|---|---|
| [build-and-dependencies.md](build-and-dependencies.md) | Reactor, inter-module dependencies, profiles, plugin zip, version derivation, CI jobs | 240 | 374449897 | unreviewed |
| [extension-points.md](extension-points.md) | ES plugin entry points, `SearchGuardModule` and registry, implementation matrix, secondary registries, request pipeline order, settings | 330 | 374449897 | unreviewed |
| [wire-surface.md](wire-surface.md) | Transport actions, serialized messages and version guards, thread-context headers, interceptor behaviour, user serialization, CCS, transport TLS | 276 | 374449897 | unreviewed |
| [persisted-schemas.md](persisted-schemas.md) | Indices, `CType` registry, document schemas and unknown-field handling, config propagation, on-disk artefacts, secrets, migrations | 208 | 374449897 | unreviewed |
| [rest-surface.md](rest-surface.md) | Route catalogue (66 handlers, 177 routes), dispatcher chain, body handling, HTTP auth frontends, Kibana-facing and sgctl-facing contracts | 291 | 374449897 | unreviewed |
| [test-infrastructure.md](test-infrastructure.md) | `LocalCluster` modes, legacy harness, per-module test inventory, surefire and CI, mixed-version coverage, external integration tests | 307 | 374449897 | unreviewed |

Reading order for the compatibility rulebook (phase 3): wire-surface, persisted-schemas, extension-points, then
the rest.

Each document opens with a "Summary for maintainers" written in plain language: what it covers, the three to six
facts that change how you work, and what a human should check. The tables below it are the authoritative record.
**Reviewing a document means confirming the claims in its summary and the rows it points to**, not reading every
table row; the cross-check below already did the mechanical verification.

## Findings that matter most for rolling upgrades

Verified by a second reader (the coordinating session) against the code, not only by the writing agent:

1. **The authenticated user crosses nodes as Java serialization.** `User` implements `Serializable` with a fixed
   `serialVersionUID`; `Base64Helper` uses `ObjectOutputStream` and `ObjectInputStream`; the result travels in
   `_sg_user_header` on every authenticated request to every data node. Any field, type, or package change on
   `User` or its attribute classes is a wire-format change with no version negotiation.
2. **There is exactly one node-version gate in the plugin**, `SearchGuardInterceptor.java:241` (ES 9.5.0, batched
   suggest with DLS). Every plugin-defined `Writeable` is positional with no `TransportVersion` check; enums go
   over the wire by ordinal.
3. **`CType` ord 10011 is declared twice**, by `sessions` (`SessionServiceConfig.java:47`) and `authz_dlsfls`
   (`DlsFlsConfig.java:36`). Registration is last-wins in a static map (`CType.java:118`), and `ConfigVersion`
   in `dlic-security` writes the ord with `writeVInt` and resolves it with `CType.getByOrd` on receipt. Reachability
   in practice (which config types auth-token snapshots carry) is not yet traced. Defect candidate.

Reported by the writing agents and not yet independently verified:

4. **No mixed-version, rolling-restart or wire-compatibility test exists.** The external-process cluster hard-wires
   `Version.CURRENT`, one ES download per cluster, one plugin zip; there is a node stop but no restart. The only
   compatibility test deserializes a single ES 8.18 auth-token blob. test-infrastructure.md sketches the shortest
   path to a mixed-version test.
5. **The interceptor header allow-list is the de-facto inter-node protocol** (`SearchGuardInterceptor.java:151-169`).
   A header introduced on a new node is dropped by an old coordinating node; a header a new node requires is
   missing on requests from old nodes.
6. **Config documents carry no version or format field.** Unknown attributes are logged and tolerated on load but
   rejected on API write; unknown keys inside role permission elements throw; `internalusers` preserves unknown
   fields. Signals watches are strict on load, so a watch with a newer attribute silently stops running on older
   nodes.
7. **All plugin settings are node-scope, none dynamic.** Consistent behaviour depends on identical
   `elasticsearch.yml` on every node; nothing can be reconciled at runtime.
8. **A disabled or absent module silently drops its transport actions and index-level enforcement on that node.**
   A data node without `dlic-dlsfls` serves unfiltered shards even if the coordinating node computed restrictions.
9. **One plugin zip per exact ES patch.** The ES version is baked into the plugin descriptor; ES, Lucene and log4j
   are provided scope; there is no build-level compatibility layer.
10. **REST authorization happens only per transport action in `SearchGuardFilter`.** Handlers that answer locally
    without dispatching an action (authinfo, kibanainfo, sslinfo, whoami, session GET, the `_has_privileges` mock)
    are never privilege-checked.

Security-relevant, verified by the second reader:

- `EncryptionKeys.java:59-60` ships a hard-coded default AES key used whenever no `active` key is configured under
  `searchguard.config_vars.encryption_keys.*`. Whether generated JWT signing keys are the only thing it protects
  is as reported by persisted-schemas.md and not re-traced.
- `EncryptionKeys.java:92-93` checks `settings.hasValue("key_file")` but then reads `settings.get("file")`, so the
  documented `key_file` option yields a null path. Defect candidate.

Reported and not yet verified: Signals internal auth keys and account passwords stored in clear text
(persisted-schemas.md, section 6).

## Documentation drift already visible

Three of the six agents independently flagged that the root `CLAUDE.md` says `main` is on ES 9.4.4 while
`pom.xml` pins 9.5.5. `THIRD-PARTY.txt` still lists ES 9.5.2. The `CLAUDE.md` description of the Docker jobs does
not match `.gitlab-ci.yml`. These are exactly the kind of staleness the maintenance mechanisms in the plan exist
to catch.

## Sections that will become generator output

Per the plan, table-shaped facts about the code move to scripts in phase 2, with a CI diff gate in phase 4. The
candidates, by document:

| Document | Section | Source of truth |
|---|---|---|
| build-and-dependencies | module inventory, dependency graph, profiles | `pom.xml` files |
| extension-points | implementation matrix, settings list, registered transport actions | `SearchGuardModule` implementations, `getSettings`, `getActions` |
| wire-surface | transport action list, `Writeable` list with field order, header constants | `ActionType` registrations, `writeTo` methods, `ConfigConstants` |
| persisted-schemas | `CType` registry, index name list | `CType` declarations, index name constants |
| rest-surface | route catalogue | `routes()` of every `RestHandler` |
| test-infrastructure | per-module test inventory | `src/test` trees |

Everything else in the six documents is written analysis and stays subject to the docs lint, freshness stamp and
review gate.

## Open questions for maintainers

Each document ends with its own list; 52 questions in total (build 11, extension points 10, wire surface 7,
persisted schemas 9, REST surface 8, test infrastructure 7). Every question has an `Answer: _pending_` slot.

How to answer:

- **A fact about the code or the environment**: write it into the slot, with your initials and the date. In
  phase 2 the fact is folded into the document body and the question removed.
- **A decision**: add an entry to [`../decisions.md`](../decisions.md) and put `see D-<n>` into the slot.
  Four draft entries exist already (support matrix, halt protocol, `User` serialization, config API tier).
- **A defect**: open a GitLab issue and put the link into the slot.

The questions that need an answer before phase 3 can write the rulebook:

- Is `User` meant to stay Java-serialized, or should `_sg_user_header` move to a versioned, self-describing
  encoding? (wire-surface 1)
- Is the `CType` ord still load-bearing for config-history and auth-token snapshots? (persisted-schemas 1)
- Which deployed versions still send the legacy CCS DLS/FLS response headers, if any? (wire-surface 2)
- Which of the two config API tiers is the supported contract for sgctl and the Kibana UI? (rest-surface 1)
- Which endpoints does the current sgctl actually call? This repo cannot answer it. (rest-surface 7)
- Is `TEST_CLUSTER_TYPE` set as a GitLab variable, so that CI runs external-process clusters at all?
  (test-infrastructure 2)
- Does `sgi8` contain any rolling-upgrade or mixed-version test definition? (test-infrastructure 3)

## Cross-check (2026-10-19)

A seventh agent read all six documents against each other and against the code. Twelve spot checks, two per
document, all held, including the three defect candidates above. Six contradictions were found; each was resolved
by reading the code, and the losing document was corrected in place (edits are marked "after cross-check").

| # | Topic | Wrong claim | Correct, per code | Fixed in |
|---|---|---|---|---|
| 1 | Enterprise module list | build-and-dependencies omitted `SessionModule` | `SearchGuardPlugin.java:353-355` registers it between Scheduler and Signals | build-and-dependencies §4 |
| 2 | Serialized user header name | extension-points said `_sg_user`, `_sg_origin`, `_sg_remote_address` | Those are node-local transients; the headers are `_sg_user_header`, `_sg_origin_header`, `_sg_remote_address_header` (`ConfigConstants.java:28,59,68`) | extension-points §5d, mixed-cluster row |
| 3 | Transport interceptor in `ssl_only` mode | wire-surface said the `ssl` module registers `SearchGuardSSLTransportInterceptor` | `SearchGuardPlugin.getTransportInterceptors` never calls `super` (`:635-665`), so no interceptor is registered at all in that mode | wire-surface §4.4 |
| 4 | FEMT transport actions | wire-surface and extension-points missed `config/multitenancy/activate` and `femt:user/available_tenants/get` | Both registered via `FeMultiTenancyConfigApi.java:48-54` | wire-surface §1 (rows added, types still UNVERIFIED), extension-points mixed-cluster row |
| 5 | `security` REST handler count | rest-surface said 29 | Code gives 28; totals corrected to 65 handlers, route total marked UNVERIFIED | rest-surface §1 |
| 6 | Line numbers for `getActions()` and the mock handler | wire-surface `:496`, rest-surface `:448` | `:498` and `:449` | rest-surface note; wire-surface reference left as is (cosmetic) |

Canonical homes for topics that appear in several documents, so later edits go to one place:

| Topic | Canonical home |
|---|---|
| Thread-context headers and transients | wire-surface §4 |
| `SearchGuardFilter` step order, fast exits, admin bypass | extension-points §5d |
| Admin certificate check | rest-surface §2 (HTTP), wire-surface §7 (transport) |
| `CType` registry, ord, storage, arity | persisted-schemas §2 |
| Transport action catalogue and counts | wire-surface §1 |
| Module registration order, enterprise list, disabled modules | extension-points §2b |
| Plugin descriptor and entitlement policy | build-and-dependencies §4 |
| Signals enterprise handlers loaded reflectively | extension-points §4 |
| CI unit-test jobs and surefire configuration | test-infrastructure §4 |
| Default-init `sgconfig/` YAMLs, JWT signing keys, FEMT 8.8.0 migration, `ProtectedConfigIndexService` | persisted-schemas |
| `X-Search-Guard-Version` and `SearchGuardVersion` | rest-surface §4 |

### Gaps to fill in phase 2

Topics none of the six documents covered, found by the cross-check:

- Scheduler nodes actions (`executing_triggers/check`, `scheduler/config/update`) have no named caller. Callers are
  `IndexJobStateStore.java:1618` and `SignalsTenant.java:394-405`, `TransportDeleteWatchAction.java:74`,
  `TransportDeActivateWatchAction.java:88`. Both are fan-outs and belong in the wire-surface blast-radius list.
- Audit sink types are not listed: `internal_elasticsearch`, `external_elasticsearch`, `webhook`, `debug`, `noop`,
  `log4j` (`SinkProvider.java:124-147`).
- Source of the Search Guard version string: `SearchGuardVersion.java:33,43-60` reads the Maven `pom.properties`
  and cuts at `-es-`; on CI snapshots without `-es-` the whole revision is used.
- License gating is not inventoried; `SearchGuardLicense.Feature` has a single constant `COMPLIANCE`, written by
  ordinal (`SearchGuardLicense.java:102,330-332`).
- The Signals painless whitelist is `signals/src/main/resources/com/floragunn/signals/script/SignalsPainlessClassWhitelist.txt`.
- `dlic-signals` account documents (`jira`, `pagerduty`) are missing from persisted-schemas §3.
- The set of "well-known actions needing special processing" (`ExtendedActionHandlingService`) and the static
  `Actions.java` catalogue of ES action names are listed nowhere; both matter when an ES minor renames actions.
- Every document covers `main` at ES 9.5.5 only. Known divergence points for `main-es8`: `entitlement-policy.yaml`
  vs `plugin-security.policy`, and `BaseNodesRequest` serialization.

Verdict per document after the fixes: persisted-schemas and test-infrastructure ready for maintainer review as
written; build-and-dependencies, extension-points, wire-surface and rest-surface ready after the corrections above,
which have been applied.

## Known limits of this pass

- Elasticsearch base-class behaviour was checked with `javap` against ES 9.4.4 from the local Maven cache; the
  pinned 9.5.5 jar was not present locally. Marked UNVERIFIED where it matters.
- `main-es8` was not inspected. Nodes-action serialization may differ there (wire-surface, open question 7).
- `sgi8` is not checked out locally; its content is known only from CI job names.
- No builds or tests were run.
