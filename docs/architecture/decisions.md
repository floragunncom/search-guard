# Decision log

Decisions that constrain how this codebase may change. One entry per decision, newest at the bottom. An entry
records what was decided, why, and what it constrains, so that a later reader (human or AI) does not have to
rediscover the reasoning. A decision that is reversed gets a new entry; the old one is marked superseded, never
deleted.

Facts about the code do not belong here; they go into the inventory documents under `inventory/`. Defects go to
GitLab issues. This file is for choices.

Format:

```
## D-<n>: <short title>
- **Date:** YYYY-MM-DD
- **Status:** accepted | superseded by D-<m>
- **Decision:** one or two sentences
- **Why:** one or two sentences
- **Constrains:** what may not change without revisiting this entry; which inventory or module documents cite it
- **Raised by:** inventory document and question number, issue link, or discussion
```

---

## D-1: Support matrix

- **Date:** _pending_
- **Status:** draft
- **Decision:** For each contract below, the oldest version that must keep working with the current `main`.

| Contract | Consumer | Oldest supported version | Notes |
|---|---|---|---|
| Config documents in the `searchguard` index | all nodes, sgctl | _pending_ | |
| Transport messages and thread-context headers between nodes | nodes of other plugin versions during rolling upgrade | _pending_ | patch and minor upgrades must work in mixed mode; major upgrades may require full restart |
| REST API, legacy `/_searchguard/api/*` tier | sgctl, Kibana plugin, user scripts | _pending_ | |
| REST API, `/_searchguard/config/*` tier | sgctl, Kibana plugin | _pending_ | |
| Kibana-facing endpoints (authinfo, session, frontend config, multi-tenancy) | Search Guard Kibana plugin | _pending_ | |
| License key format | license issuer, all nodes | _pending_ | |
| Audit log record format | external log consumers | _pending_ | `audit_format_version` is 4 |
| Signals watch, account and state documents | all nodes, Signals UI | _pending_ | |
| Auth token and session token format | all nodes, token holders | _pending_ | |
| Elasticsearch versions | the build | _pending_ | one zip per exact ES patch |

- **Why:** "Backward compatible" is not decidable until it says compatible with what.
- **Constrains:** every change reviewed against the phase 3 checklist.
- **Raised by:** `ai-readiness-plan.md`, prerequisites.

## D-2: Halt protocol for suspected breaking changes

- **Date:** _pending_
- **Status:** draft
- **Decision:** When an AI-driven change would alter any contract in D-1 beyond its supported range, the work
  stops before implementation. The agent records the contract, the exact change, the affected versions, and at
  least two alternatives (feature switch, additive change, deferral) in _pending: where_, and waits for a decision
  by _pending: whom_.
- **Why:** The two non-negotiable constraints in the plan.
- **Constrains:** every skill and every root `CLAUDE.md` rule that touches implementation.
- **Raised by:** `ai-readiness-plan.md`, prerequisites.

## D-3: Serialization of `User` between nodes

- **Date:** _pending_
- **Status:** draft
- **Decision:** _pending_ (keep Java serialization with the fixed `serialVersionUID` and treat every field change
  as a wire change, or move `_sg_user_header` to a versioned self-describing encoding at a defined release)
- **Why:** _pending_
- **Constrains:** `security/src/main/java/com/floragunn/searchguard/user/User.java` and every class reachable
  from its fields; `Base64Helper`; the `SearchGuardInterceptor` allow-list.
- **Raised by:** `inventory/wire-surface.md`, open question 1.

## D-4: Supported config API tier

- **Date:** _pending_
- **Status:** draft
- **Decision:** _pending_ (which of the legacy `/_searchguard/api/*` tier and the `/_searchguard/config/*` tier
  is the frozen contract for sgctl and the Kibana configuration UI, and whether the other is deprecated)
- **Why:** _pending_
- **Constrains:** the REST route catalogue in `inventory/rest-surface.md` §1.3 and §1.1; sgctl releases.
- **Raised by:** `inventory/rest-surface.md`, open questions 1 and 7.

## D-5: CType ord 10011 collision (`authz_dlsfls` and `sessions`)

- **Date:** _pending_
- **Status:** draft
- **Decision:** _pending_ (renumber one of the two types with a migration note for `ConfigVersion` consumers, or
  declare the collision harmless and document why)
- **Why:** _pending_
- **Constrains:** `dlic-dlsfls/.../DlsFlsConfig.java#TYPE`, `security/.../SessionServiceConfig.java`,
  `dlic-security/.../sgconf/history/ConfigVersion.java`; auth-token snapshots that carry config versions.
- **Raised by:** `inventory/persisted-schemas.md` open question 1; `dlic-dlsfls.md` section 9.

## D-6: Rule-semantics changes in DLS/FLS/FM during rolling upgrades

- **Date:** _pending_
- **Status:** draft
- **Decision:** _pending_ (accept that a change to how roles are compiled into DLS/FLS/FM rules yields per-shard
  divergence while nodes run different plugin versions, or require such changes to be gated on a cluster-wide
  minimum plugin version, or require a full cluster restart for them)
- **Why:** Restrictions are recomputed on every data node from the user header and the node's local `roles`; nothing
  about the computed rule travels between nodes (`dlic-dlsfls.md` M-1).
- **Constrains:** `RoleBasedAuthorizationBase`, `RoleBasedFieldAuthorization.FlsRule`,
  `RoleBasedFieldMasking.FieldMaskingRule`, `Role.Index` parsing; the halt list in `dlic-dlsfls/CLAUDE.md`.
- **Raised by:** `dlic-dlsfls.md` section 6, row M-1.
