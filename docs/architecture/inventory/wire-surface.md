# Wire surface between nodes

*Inventory pass, generated 2026-10-19 on branch experimental-analysis at commit 374449897. Every claim carries a file reference. Items marked UNVERIFIED were inferred, not read.*

## Summary for maintainers

*Written for human readers. The tables below are the authoritative record; this section is a guide to them and must be re-read whenever the interceptor, the request handler, the `User` class or any `writeTo` method changes.*

**What this covers.** Everything the plugin sends between nodes over the Elasticsearch transport layer: which actions exist, how their messages are encoded, which headers travel with a request, how nodes authenticate each other, and what happens when a request goes to a remote cluster.

**What you need to know.**

- The logged-in user travels to every node as a Java-serialized object inside a request header. Any change to the `User` class, or to any class it contains, changes the wire format. Nothing negotiates versions, so a mismatch between nodes either fails the request or silently drops data such as roles.
- Exactly one place in the plugin looks at the other node's version before sending. Every other message is written field by field in a fixed order with no version check, and enum values go over the wire as numbers. Adding, removing or reordering a field, or inserting an enum constant anywhere but at the end, breaks a mixed-version cluster.
- About twenty actions fan out to every node: config reload, cache flush, session and auth-token pushes, Signals updates. These are the paths where an old and a new node must agree on message layout during a rolling upgrade.
- An allow-list in the interceptor decides which headers leave a node. A header introduced on a new node is silently dropped by an old coordinating node, and a header a new node requires is missing on requests from old nodes.
- Nodes trust each other by TLS certificate, matched against a DN list or a certificate OID. Once a peer is recognised as a node, the user header it sends is accepted without further checks.
- Remote clusters receive the user object and use it for authorization on their side. A legacy exchange of DLS/FLS headers in responses still exists on the receiving side, but nothing in this repository sends it.

**What to review.**

- Section 2, the statement that no serialized message carries a version check, and the field order of the fan-out messages listed in "Mixed-cluster and compatibility relevance". These rows drive the phase 3 checklist.
- Section 4.1 and 4.4, which headers cross nodes and the allow-list.
- Section 5, the `User` row. Whether Java serialization stays is decision D-3 in `../decisions.md` (open question 1).
- Section 6, whether any deployed version still sends the legacy cross-cluster headers (open question 2).
- Base-class behaviour was checked against ES 9.4.4, not the pinned 9.5.5.

## Scope and method

Covers everything the Search Guard FLX plugin puts on the Elasticsearch transport layer: registered transport actions, `Writeable` request/response/node messages, version checks, `ThreadContext` headers and transients, the transport interceptor and request handler, user/auth-state serialization, cross-cluster handling and inter-node authentication. Method: `grep`/`sed` over `*/src/main` of all Maven modules (`sgi8/`, `audit/`, `target/` ignored), plus `javap` against the ES 9.4.4 jar in `~/.m2` to confirm base-class behaviour (the pom pins `elasticsearch.version` 9.5.5 at `pom.xml:49`; 9.5.x jar was not present locally, so base-class facts are from 9.4.4 and marked as such). Test code is out of scope. All paths are relative to the repo root.

Encoding legend used below: **nodes** = `TransportNodesAction` (fans out a per-node `NodeRequest` to every node; the top-level `BaseNodesRequest` is local-only because `BaseNodesRequest.writeTo` is `final` and calls `TransportAction.localOnly()` in ES 9.4.4, verified by `javap`); **handled** = `HandledTransportAction` with hand-written `writeTo` (positional binary); **generic** = `com.floragunn.searchsupport.action.Action.Handler` (`support/src/main/java/com/floragunn/searchsupport/action/Action.java:241-253`) whose request/response are a self-describing envelope: 2 version bytes, a generic metadata map, a type byte and a SMILE body (`Action.java:111-131`, `182-188`, `272-283`).

## 1. Transport actions

Registration points: `security/src/main/java/com/floragunn/searchguard/SearchGuardPlugin.java:457-499` (core, plus `moduleRegistry.getActions()` at `:496`), `security/.../authc/session/backend/SessionModule.java:73-79`, `dlic-security/.../authtoken/AuthTokenModule.java:85-93`, `dlic-dlsfls/.../DlsFlsModule.java:180-182`, `dlic-fe-multi-tenancy/.../FeMultiTenancyModule.java:223-226`, `scheduler/.../jobs/actions/SchedulerActions.java:11-14`, `signals/src/main/java/com/floragunn/signals/SignalsModule.java:150-187`. `dlic-auditlog` and `dlic-signals` register no ES actions (`dlic-signals` only adds Signals action-handler factories, `dlic-signals/.../SignalsEnterpriseFeatures.java:12`). No `getNamedWriteables` registrations exist anywhere (grep returned nothing).

| Action name | ActionType (file:line) | Request / Response | Module | Scope | Mechanism |
|---|---|---|---|---|---|
| `cluster:admin/searchguard/config/update` | `security/.../action/configupdate/ConfigUpdateAction.java:25` | `ConfigUpdateRequest` / `ConfigUpdateResponse`; per node `TransportConfigUpdateAction.NodeConfigUpdateRequest` / `ConfigUpdateNodeResponse` | security | cluster-wide | nodes (`TransportConfigUpdateAction.java:48`) |
| `cluster:admin/searchguard/whoami` | `security/.../action/whoami/WhoAmIAction.java:25` | `WhoAmIRequest` / `WhoAmIResponse` | security | local only (`WhoAmIResponse.writeTo` calls `localOnly()` `:45`; `WhoAmIRequest(StreamInput)` is a no-op `:28`) | handled (`TransportWhoAmIAction.java:38`) |
| `cluster:admin/searchguard/components/state` | `security/.../modules/api/GetComponentStateAction.java:79` | `Request`/`Response`; `NodeRequest`/`NodeResponse` | security | cluster-wide | nodes (`:459`) |
| `cluster:admin:searchguard:get_license_info` | `security/.../license/SearchGuardLicenseInfoAction.java:41` | `EmptyRequest` / `Response` | security | local | generic (`:121`) |
| `cluster:admin/searchguard/license/info` (legacy) | `security/.../license/legacy/LicenseInfoAction.java:33` | `LicenseInfoRequest`/`LicenseInfoResponse`; `NodeLicenseRequest`/`LicenseInfoNodeResponse` | security | cluster-wide | nodes (`TransportLicenseInfoAction.java:44`) |
| `cluster:admin:searchguard:config/bulk/get`, `.../bulk/update` | `security/.../configuration/api/BulkConfigApi.java:77,193` | `EmptyRequest`/`Response`; `Request`/`StandardResponse` | security | local | generic (`:104,222`) |
| `cluster:admin:searchguard:config/delete_by_type` | `security/.../configuration/api/GenericTypeLevelConfigApi.java:43` | `IdRequest`/`StandardResponse` | security | local | generic (`:50`) |
| `cluster:admin:searchguard:config/migrate_index` | `security/.../configuration/api/MigrateConfigIndexApi.java:35` | `EmptyRequest`/`StandardResponse` | security | local | generic (`:46`) |
| `cluster:admin:searchguard:config_vars/refresh` | `security/.../configuration/variables/ConfigVarRefreshAction.java:51` | `Request`/`Response`; `NodeRequest`/`NodeResponse` | security | cluster-wide | nodes (`:113`) |
| `cluster:admin:searchguard:config/vars/get`, `/put`, `/delete`, `/get/all`, `/put/all` | `security/.../configuration/variables/ConfigVarApi.java:54,97,184,213,249` | `IdRequest`/`Request`/`EmptyRequest` with `StandardResponse` | security | local | generic (`:60,157,190,219,290`) |
| `cluster:admin:searchguard:config/internal_users/get`, `/put`, `/delete`, `/patch` | `security/.../authc/internal_users_db/InternalUsersConfigApi.java:62,103,178,218` | `IdRequest`/`PutAction.Request`/`PatchAction.Request` with `StandardResponse` | security | local | generic (`:68,140,184,256`) |
| `cluster:admin:searchguard:auth/frontend/config/get` | `security/.../authc/session/GetActivatedFrontendConfigAction.java:47` | `Request`/`Response` | security | local | generic (`:157`) |
| `cluster:admin:searchguard:config/authc/get`, `/put`, `/patch`, `/delete` | `security/.../authc/rest/RestAuthcConfigApi.java:47,65,81,96` | `EmptyRequest`/`Request`/`PatchAction.Request` with `TypeLevelConfigApi` responses | security | local | generic (`:53,71,87,103`) |
| `cluster:admin:searchguard:cache/delete` | `security/.../authc/rest/AuthcCacheApi.java:79` | `Request`/`Response`; `NodeRequest`/`NodeResponse` | security | cluster-wide | nodes (`:157`) |
| `cluster:admin:searchguard:config/authz/get`, `/put`, `/patch` | `security/.../authz/config/AuthorizationConfigApi.java:50,68,84` | `TypeLevelConfigApi` request/response types | security | local | generic (`:56,74,90`) |
| `cluster:admin:searchguard:config/license_key/get`, `/put`, `/patch` | `security/.../license/SearchGuardLicenseKeyApi.java:48,65,81` | `TypeLevelConfigApi` types | security | local | generic (`:54,71,87`) |
| `cluster:admin:searchguard:config/authc_frontend/_all/{get,put,patch}`, `/_id/{get,put,patch}` | `security/.../authc/session/FrontendAuthcConfigApi.java:45,63,79,107,125,141` | `TypeLevelConfigApi` / `DocumentLevelConfigApi` types | security | local | generic (`:51,69,85,113,131,147`) |
| `cluster:admin/searchguard/capabilities/cluster_wide/get` | `security/.../SearchGuardCapabilities.java:320` | `Request`/`Response`; `NodeRequest`/`NodeResponse` | security | cluster-wide | nodes (`:435`) |
| `cluster:admin:searchguard:internal/indices/create` | `security/.../configuration/ProtectedConfigIndexService.java:673` | `Request`/`Response`; `NodeRequest`/`NodeResponse` | security | cluster-wide | nodes (`:784`) |
| `cluster:admin:searchguard:session/_own/get/extended`, `/_own/delete`, `session/create` | `security/.../authc/session/backend/SessionApi.java:63,149,216` | `EmptyRequest`/`Response`; `RestRequestHeadersRequest`/`StandardResponse`; `RestRequestHeadersRequest`/`StartSessionResponse` | security | local | generic (`:119,155,222`) |
| `cluster:admin/searchguard/session_token/update/push` | `security/.../authc/session/backend/PushSessionTokenUpdateAction.java:47` | `Request`/`Response`; `NodeRequest`/`NodeResponse` | security | cluster-wide | nodes (`:183`) |
| `cluster:admin:searchguard:config/sessions/get`, `/put`, `/patch` | `security/.../authc/session/backend/SessionServiceConfigApi.java:47,65,81` | `TypeLevelConfigApi` types | security | local | generic (`:53,71,87`) |
| `cluster:admin:searchguard:authtoken/_own/create` | `dlic-security/.../authtoken/api/CreateAuthTokenAction.java:22` | `CreateAuthTokenRequest`/`CreateAuthTokenResponse` | dlic-security | local in practice | handled (`TransportCreateAuthTokenAction.java:31`) |
| `cluster:admin/searchguard/auth_token/update/push` | `dlic-security/.../authtoken/update/PushAuthTokenUpdateAction.java:23` | `PushAuthTokenUpdateRequest`/`PushAuthTokenUpdateResponse`; `NodeRequest`/`PushAuthTokenUpdateNodeResponse` | dlic-security | cluster-wide | nodes (`TransportPushAuthTokenUpdateAction.java:37`) |
| `cluster:admin:searchguard:authtoken/_own/get`, `/_own/revoke`, `/_own/search`, `authtoken/info` | `dlic-security/.../authtoken/api/{GetAuthTokenAction,RevokeAuthTokenAction,SearchAuthTokensAction,AuthTokenInfoAction}.java:22` | matching `*Request`/`*Response` | dlic-security | local in practice | handled (`AbstractTransportAuthTokenAction.java:32-33`, `TransportAuthTokenInfoAction.java:29`) |
| `cluster:admin:searchguard:config/auth_token_service/get`, `/put`, `/patch` | `dlic-security/.../authtoken/AuthTokenServiceConfigApi.java:49,67,83` | `TypeLevelConfigApi` types | dlic-security | local | generic (`:55,73,89`) |
| `cluster:admin:searchguard:config/authz_dlsfls/get`, `/put`, `/patch` | `dlic-dlsfls/.../DlsFlsConfigApi.java:47,65,81` | `TypeLevelConfigApi` types | dlic-dlsfls | local | generic (`:53,71,87`) |
| `cluster:admin:searchguard:config/fe_multi_tenancy/get`, `/put`, `/patch` | `dlic-fe-multi-tenancy/.../FeMultiTenancyConfigApi.java:58,76,92` | `TypeLevelConfigApi` types | dlic-fe-multi-tenancy | local | generic (`:64,82,98`) |
| `cluster:admin:searchguard:config/multitenancy/activate` | `dlic-fe-multi-tenancy/.../MultitenancyActivationAction.java:27` (registered via `FeMultiTenancyConfigApi.java:48-54`, `FeMultiTenancyModule.java:223-226`) | request/response types not yet inventoried (added after cross-check; see README gap list) | dlic-fe-multi-tenancy | local | UNVERIFIED |
| `cluster:admin:searchguard:femt:user/available_tenants/get` | `dlic-fe-multi-tenancy/.../GetAvailableTenantsAction.java:21` (registered via `FeMultiTenancyConfigApi.java:48-54`) | request/response types not yet inventoried (added after cross-check) | dlic-fe-multi-tenancy | local | UNVERIFIED |
| `cluster:admin:searchguard:config/multitenancy/frontend_data_migration/8_8_0/get`, `/start` | `dlic-fe-multi-tenancy/.../datamigration880/rest/GetDataMigrationStateAction.java:40`, `StartDataMigrationAction.java:43` | `EmptyRequest`/`StandardResponse`; `StartDataMigrationRequest`/`StandardResponse` | dlic-fe-multi-tenancy | local | generic (`:47`, `:79`) |
| `cluster:admin/searchsupport/scheduler/executing_triggers/check` | `scheduler/.../jobs/actions/CheckForExecutingTriggerAction.java:8` | `CheckForExecutingTriggerRequest`/`Response`; `NodeRequest`/`NodeResponse` | scheduler | cluster-wide | nodes (`TransportCheckForExecutingTriggerAction.java:30`) |
| `cluster:admin/searchsupport/scheduler/config/update` | `scheduler/.../jobs/actions/SchedulerConfigUpdateAction.java:30` | `SchedulerConfigUpdateRequest`/`Response`; `NodeRequest`/`NodeResponse` | scheduler | cluster-wide | nodes (`TransportSchedulerConfigUpdateAction.java:42`) |
| `cluster:admin:searchguard:tenant:signals:watch/ack` | `signals/.../actions/watch/ack/AckWatchAction.java:8` | `AckWatchRequest`/`AckWatchResponse`; `NodeRequest`/`NodeResponse` | signals | cluster-wide | nodes (`TransportAckWatchAction.java:51`) |
| `cluster:admin:searchguard:tenant:signals:watch/ack_and_get` | `signals/.../watch/ackandget/AckAndGetWatchAction.java:8` | `AckAndGetWatchRequest`/`AckAndGetWatchResponse`; `NodeRequest`/`NodeResponse` | signals | cluster-wide | nodes (`TransportAckAndGetWatchAction.java:65`) |
| `cluster:admin:searchguard:signals:destination/update` | `signals/.../account/config_update/DestinationConfigUpdateAction.java:13` | `DestinationConfigUpdateRequest`/`Response`; `NodeRequest`/`NodeResponse` | signals | cluster-wide | nodes (`TransportDestinationConfigUpdateAction.java:26`) |
| `cluster:admin:searchguard:signals:settings/update` | `signals/.../settings/update/SettingsUpdateAction.java:13` | `SettingsUpdateRequest`/`Response`; `NodeRequest`/`NodeResponse` | signals | cluster-wide | nodes (`TransportSettingsUpdateAction.java:26`) |
| `cluster:admin:searchguard:signals:truststores/update` | `signals/.../truststore/rest/TransportTruststoreUpdatedAction.java:168` | `TruststoreUpdatedRequest`/`Response`; `NodeRequest`/`NodeResponse` | signals | cluster-wide | nodes (`:50`) |
| `cluster:admin:searchguard:signals:proxies/update` | `signals/.../proxy/rest/TransportProxyUpdatedAction.java:165` | `ProxyUpdatedRequest`/`Response`; `NodeRequest`/`NodeResponse` | signals | cluster-wide | nodes (`:47`) |
| `cluster:admin:searchguard:tenant:signals:watch/{get,put,delete,search,activate_deactivate,execute}`, `watch:state/{get,search}` | `signals/.../actions/watch/*/…Action.java:8` | matching `*WatchRequest`/`*WatchResponse` | signals | local in practice | handled (`TransportGetWatchAction.java:28`, `TransportPutWatchAction.java:30`, `TransportDeleteWatchAction.java:32`, `TransportSearchWatchAction.java:31`, `TransportDeActivateWatchAction.java:33`, `TransportExecuteWatchAction.java:58`, `TransportGetWatchStateAction.java:30`, `TransportSearchWatchStateAction.java:30`) |
| `cluster:admin:searchguard:signals:account/{get,put,delete,search}` and `tenant:signals:account/{get,put,delete,search}` | `signals/.../actions/account/*/…Action.java:8` and `Tenant*Action.java:32` | `*AccountRequest`/`*AccountResponse` (tenant variants reuse the same classes) | signals | local in practice | handled (`TransportGetAccountAction.java:24`, `TransportPutAccountAction.java:33`, `TransportDeleteAccountAction.java:30`, `TransportSearchAccountAction.java:28`; tenant subclasses at `TenantGetAccountAction.java:38` etc.) |
| `cluster:admin:searchguard:signals:settings/get`, `/put`; `signals:admin/start_stop`; `tenant:signals:tenant/start_stop` | `signals/.../settings/get/GetSettingsAction.java:8`, `settings/put/PutSettingsAction.java:8`, `admin/start_stop/StartStopAction.java:8`, `tenant/start_stop/StartStopTenantAction.java:8` | matching request/response | signals | local in practice | handled (`TransportGetSettingsAction.java:30`, `TransportPutSettingsAction.java:21`, `TransportStartStopAction.java:17`, `TransportStartStopTenantAction.java:20`) |
| `cluster:admin:searchguard:signals:truststores/{createorreplace,findone,findall,delete}` | `signals/.../truststore/rest/CreateOrReplaceTruststoreAction.java:47`, `FindOneTruststoreAction.java:41`, `FindAllTruststoresAction.java:41`, `DeleteTruststoreAction.java:40` | `CreateOrReplaceTruststoreRequest`/`IdRequest`/`EmptyRequest` with `StandardResponse` | signals | local | generic (`:60`, `:53`, `:54`, `:53`) |
| `cluster:admin:searchguard:signals:proxies/{createorreplace,delete,findall,findone}` | `signals/.../proxy/rest/ProxyApi.java:61,149,192,229` | `CreateOrReplaceProxyRequest`/`IdRequest`/`EmptyRequest` with `StandardResponse` | signals | local | generic (`:68,156,199,236`) |
| `cluster:admin:searchguard:signals:summary/load` | `signals/.../actions/summary/LoadOperatorSummaryAction.java:9` | `LoadOperatorSummaryRequest`/`StandardResponse` | signals | local | generic (`LoadOperatorSummaryHandler.java:43`) |

"Local in practice": every `HandledTransportAction` registers a transport handler, so another node *could* send the message; in this repo all of them are invoked through the local `NodeClient` from REST handlers (ES 9 has no transport client). UNVERIFIED that no external tooling sends them node-to-node.

## 2. Serialized messages

Every class in `src/main` with a `writeTo(StreamOutput)` (72 files, found by grep). **No class in this list contains any version check**: there is no `TransportVersion`, `Version.onOrAfter`, `getTransportVersion()` or minimum-version constant in any `writeTo` or `StreamInput` constructor. The only "version-shaped" bytes are the two literal `0` bytes of the generic envelope (`Action.java:112-113,183-184`), which are read but never evaluated (`Action.java:272-273`, `UnparsedMessage` at `:337-358`), and the literal `out.writeInt(1)` in `AuthTokenInfoResponse.java:47` which the reader skips (`:40`). Fields are listed in write order.

| Class (writeTo at) | Module | Fields written, in order |
|---|---|---|
| `Action.Request` (`support/.../action/Action.java:111`) | support | byte 0, byte 0, `writeGenericMap(getMetaData())` (`if-match`,`if-none-match`; null if empty), type byte (`SMILE`=1 or `EMPTY`=0), `writeByteArray(SMILE of toBasicObject())` |
| `Action.Response` (`Action.java:182`) | support | byte 0, byte 0, `writeGenericMap({status, etag})`, byte 1, `writeByteArray(toSmile())` |
| All `generic` requests/responses in section 1 (`StandardRequests.EmptyRequest/IdRequest`, `StandardResponse`, `TypeLevelConfigApi.*`, `DocumentLevelConfigApi.*`, `BulkConfigApi.*`, `ConfigVarApi.*`, `SessionApi.*`, `GetActivatedFrontendConfigAction.*`, `SearchGuardLicenseInfoAction.Response`, `CreateOrReplaceTruststoreRequest`, `CreateOrReplaceProxyRequest`, `LoadOperatorSummaryRequest`, `StartDataMigrationRequest`) | support/security/signals/dlic-* | inherit the envelope above; none override `writeTo` or `getMetaData()` (only definition is `Action.java:155`) |
| `AuthToken` (`dlic-security/.../authtoken/AuthToken.java:181`) | dlic-security | `id` S, `userName` S, `tokenName` optS, `creationTime` Instant, `expiryTime` optInstant, `revokedAt` optInstant, `requestedPrivileges` (nested), `base` (nested) |
| `AuthTokenPrivilegeBase` (`AuthTokenPrivilegeBase.java:170`) | dlic-security | `backendRoles` StringCollection, `searchGuardRoles` StringCollection, `attributes` GenericMap, `configVersions` optWriteable(`ConfigVersionSet`) |
| `RequestedPrivileges` (`RequestedPrivileges.java:200`) | dlic-security | `clusterPermissions` StrColl, `indexPermissions` Coll, `tenantPermissions` Coll, `excludedClusterPermissions` StrColl, `excludedIndexPermissions` Coll, `roles` optStrColl, `aliasPermissions` Coll, `dataStreamPermissions` Coll |
| `RequestedPrivileges.{Index,Tenant,Alias,DataStream,ExcludedIndex}Permissions` (`:237,334,431,528,621`) | dlic-security | pattern sources StrColl, `allowedActions`/`actions` StrColl |
| `ConfigVersion` (`sgconf/history/ConfigVersion.java:133`) | dlic-security | `configurationType.getOrd()` VInt, `version` long |
| `ConfigVersionSet` (`ConfigVersionSet.java:208`) | dlic-security | Coll of `ConfigVersion` |
| `AuthTokenInfoResponse` (`api/AuthTokenInfoResponse.java:46`) | dlic-security | int 1, `enabled` bool, `initialized` bool |
| `CreateAuthTokenRequest` (`api/CreateAuthTokenRequest.java:83`) | dlic-security | super (parent task), `tokenName` S, `audience` optS, `expiresAfter` optS (formatted), `requestedPrivileges` |
| `CreateAuthTokenResponse` (`:46`) | dlic-security | `authToken`, `jwt` S |
| `GetAuthTokenRequest` (`:44`), `RevokeAuthTokenRequest` (`:44`) | dlic-security | super, `authTokenId` S |
| `GetAuthTokenResponse` (`:57`) | dlic-security | `restStatus` enum, `error` optS, bool hasToken, [`authToken`] |
| `RevokeAuthTokenResponse` (`:49`) | dlic-security | `info` optS, `restStatus` enum, `error` optS |
| `SearchAuthTokensRequest` (`:53`) | dlic-security | super, `scroll` optTimeValue, `from` int, `size` int, `searchSourceBuilder` optWriteable |
| `SearchAuthTokensResponse` (`:45`) | dlic-security | `searchResponse` (ES `SearchResponse`) |
| `TransportPushAuthTokenUpdateAction.NodeRequest` (`update/TransportPushAuthTokenUpdateAction.java:72`) | dlic-security | super, `updatedToken` (`AuthToken`), `updateType` enum, `newHash` long |
| `PushAuthTokenUpdateNodeResponse` (`:47`) | dlic-security | super (node), `message` optS |
| `AuthTokenInfoRequest`, `PushAuthTokenUpdateRequest`, `PushAuthTokenUpdateResponse` | dlic-security | no own `writeTo`: inherit `ActionRequest` (parent task id) / `BaseNodesRequest` (local-only) / `BaseNodesResponse` |
| `TransportCheckForExecutingTriggerAction.NodeRequest` (`scheduler/.../TransportCheckForExecutingTriggerAction.java:107`) / `NodeResponse` (`:142`) | scheduler | super, `schedulerName` S, `triggerKeys` StrColl / super, `executingTriggers` StrColl, `message` optS |
| `TransportSchedulerConfigUpdateAction.NodeRequest` (`:111`) / `NodeResponse` (`:148`) | scheduler | super, `schedulerName` S / super, `status` enum, `message` optS |
| `SearchGuardCapabilities.Capabilities` (`security/.../SearchGuardCapabilities.java:216`) | security | `searchGuardVersion` optS, `capabilities` Coll<S>, `uiCapabilities` Coll<S>, `publicCapabilities` Coll<S> |
| `SearchGuardCapabilities.NodeRequest` (`:398`) / `NodeResponse` (`:418`) | security | super only / super, `capabilities` |
| `ConfigUpdateNodeResponse` (`action/configupdate/ConfigUpdateNodeResponse.java:60`) | security | super, `updatedConfigTypes` String[], `message` optS |
| `TransportConfigUpdateAction.NodeConfigUpdateRequest` (`:84`) | security | super, `configTypes` String[] |
| `WhoAmIResponse` (`action/whoami/WhoAmIResponse.java:44`) | security | `TransportAction.localOnly()` (never serialized) |
| `AuthcCacheApi.NodeRequest` (ctor `:96`, no writeTo) / `NodeResponse` (`:152`) | security | super only / super only |
| `PushSessionTokenUpdateAction.NodeRequest` (`authc/session/backend/PushSessionTokenUpdateAction.java:140`) / `NodeResponse` (`:171`) | security | super, `updatedToken` (`SessionToken`), `updateType` enum, `newHash` long / super, `message` optS |
| `SessionPrivileges` (`SessionPrivileges.java:117`) | security | `backendRoles` StrColl, `searchGuardRoles` StrColl, `attributes` GenericMap |
| `SessionToken` (`SessionToken.java:169`) | security | `id` S, `userName` S, `tokenName` optS, `creationTime` Instant, `expiryTime` optInstant, `dynamicExpiryTime` optInstant, `revokedAt` optInstant, `base` (`SessionPrivileges`) |
| `ProtectedConfigIndexService.NodeRequest` (`:751`) / `NodeResponse` (`:771`) | security | super only / super, `result` GenericMap |
| `ConfigVarRefreshAction.NodeRequest` (`:161`) / `NodeResponse` (`:192`) | security | super only / super, `status` enum, `message` optS |
| `SearchGuardLicense` (`license/SearchGuardLicense.java:85`) | security | `uid` S, `type` enum, `issueDate` S, `expiryDate` S, `issuedTo` S, `issuer` S, `startDate` S, `majorVersion` optVInt, `clusterName` S, `allowedNodeCount` int, `msgs` StrColl, `expiresInDays` long, `isExpired` bool, `valid` bool, `action` S, `prodUsage` S, `features` enum[] |
| `LicenseInfoNodeResponse` (`license/legacy/LicenseInfoNodeResponse.java:63`) | security | super, `license` optWriteable, `modules` Coll<`ModuleInfo`> |
| `LicenseInfoResponse.ModuleInfo` (`LicenseInfoResponse.java:293`) | security | `moduleType` enum, `classname` S, `classpath` S, `version` S, `buildTime` S, `gitsha1` S |
| `TransportLicenseInfoAction.NodeLicenseRequest` (`:72`) | security | super only |
| `GetComponentStateAction.NodeRequest` (`:132`) / `NodeResponse` (`:447`) | security | super, `moduleId` optS, `verbose` bool / super, `message` optS, `detailJson` optS, `states` as SMILE byte[] |
| `TransportDestinationConfigUpdateAction.NodeRequest` (`:75`) / `NodeResponse` (`:111`) | signals | super only / super, `status` enum, `message` optS |
| `TransportSettingsUpdateAction.NodeRequest` (`:84`) / `NodeResponse` (`:115`) | signals | super only / super, `status` enum, `message` optS |
| `TransportAckWatchAction.NodeRequest` (`:166`) / `NodeResponse` (`:205`) | signals | super, `watchId` S, `ack` bool, `actionId` optS / super, `status` enum, `message` optS |
| `TransportAckAndGetWatchAction.NodeRequest` (`:199`) / `NodeResponse` (`:274`) | signals | super, `watchId` S, `ack` bool, `actionId` optS / super, `status` enum, `message` optS, `acknowledgements` array (each: `acknowledgeTime` Instant, `acknowledgeByUser` S, `actionId` S — `Acknowledgement.java:17-22`), `unackedActionIds` String array |
| `TransportTruststoreUpdatedAction.NodeRequest` (`:115`) / `NodeResponse` (`:159`) | signals | super, `truststoreId` S, `operationType` S / super, `success` bool, `message` optS |
| `TransportProxyUpdatedAction.NodeRequest` (`:112`) / `NodeResponse` (`:156`) | signals | super, `proxyId` S, `operationType` S / super, `success` bool, `message` optS |
| `DeleteAccountRequest` (`:28`), `GetAccountRequest` (`:32`) | signals | super, `accountId` S, `accountType` S |
| `PutAccountRequest` (`:40`) | signals | super, `accountId` S, `accountType` S, `body` BytesReference, `bodyContentType` enum |
| `SearchAccountRequest` (`:38`), `SearchWatchRequest` (`:38`), `SearchWatchStateRequest` (`:38`) | signals | super, `scroll` optTimeValue, `from` int, `size` int, `searchSourceBuilder` optWriteable |
| `SearchAccountResponse` (`:33`), `SearchWatchResponse` (`:33`), `SearchWatchStateResponse` (`:33`) | signals | ES `SearchResponse` |
| `DeleteAccountResponse` (`:40`), `DeleteWatchResponse` (`:41`), `DeActivateWatchResponse` (`:41`) | signals | `id` S, `version` VLong, `result` enum, `restStatus` enum, `message` optS |
| `PutAccountResponse` (`:44`), `PutWatchResponse` (`:48`) | signals | `id` S, `version` VLong, `result` enum, `restStatus` enum, `message` optS, `detailJsonDocument` optS |
| `GetAccountResponse` (`:62`) | signals | `id` S, `exists` bool, `version` long, `seqNo` long, `primaryTerm` long, `source` optBytesRef |
| `GetWatchResponse` (`:71`) | signals | `tenant` optS, `id` S, `exists` bool, `version` long, `seqNo` long, `primaryTerm` long, `source` optBytesRef |
| `StartStopRequest` (`:29`), `StartStopTenantRequest` (`:29`) | signals | super, `activate` bool |
| `StartStopResponse` (`:19`), `StartStopTenantResponse` (`:19`) | signals | nothing (empty body) |
| `GetSettingsRequest` (`:32`) / `GetSettingsResponse` (`:32`) | signals | super, `key` S, `jsonRequested` bool / `status` enum, `result` optS, `contentType` optS |
| `PutSettingsRequest` (`:35`) / `PutSettingsResponse` (`:40`) | signals | super, `key` S, `value` optS, `valueJson` bool / `result` enum, `restStatus` enum, `message` optS, `detailJsonDocument` optS |
| `DeActivateWatchRequest` (`:31`) | signals | super, `watchId` S, `activate` bool |
| `DeleteWatchRequest` (`:29`), `GetWatchRequest` (`:29`) | signals | super, `watchId` S |
| `ExecuteWatchRequest` (`:48`) | signals | super, `watchId` optS, `watchJson` optS, `recordExecution` bool, `simulationMode` enum, `goTo` optS, `inputJson` optS, `includeAllRuntimeAttributesInResponse` bool |
| `ExecuteWatchResponse` (`:42`) | signals | `tenant` optS, `id` S, `status` enum, `result` optBytesRef |
| `PutWatchRequest` (`:37`) | signals | super, `watchId` S, `body` BytesReference, `bodyContentType` enum |
| `GetWatchStateRequest` (`:32`) / `GetWatchStateResponse` (`:35`) | signals | super, `watchIds` StrColl / `restStatus` enum, `watchToStatusMap` Map<S,BytesReference> |
| Top-level `BaseNodesResponse` subclasses (`ConfigUpdateResponse`, `LicenseInfoResponse`, `GetComponentStateAction.Response`, `AuthcCacheApi.Response`, `ConfigVarRefreshAction.Response`, `ProtectedConfigIndexService.Response`, `SearchGuardCapabilities.Response`, `PushSessionTokenUpdateAction.Response`, `PushAuthTokenUpdateResponse`, `AckWatchResponse`, `AckAndGetWatchResponse`, `DestinationConfigUpdateResponse`, `SettingsUpdateResponse`, `TruststoreUpdatedResponse`, `ProxyUpdatedResponse`, `CheckForExecutingTriggerResponse`, `SchedulerConfigUpdateResponse`) | various | inherit ES `BaseNodesResponse.writeTo`; only define `readNodesFrom` (e.g. `ConfigUpdateResponse.java:32`, `LicenseInfoResponse.java:53`) |

Enum fields (`writeEnum`) are ordinal-encoded by ES: reordering or inserting enum constants in `RestStatus`-like custom enums (`DocWriteResponse.Result`, Signals `Status` enums, `UpdateType`, `SimulationMode`, `SearchGuardLicense.Type/Feature`, `CType` ordinal in `ConfigVersion.java:134`) silently changes meaning on the wire.

## 3. Version checks in main code

| Location | Construct | What it guards |
|---|---|---|
| `security/.../transport/SearchGuardInterceptor.java:110,128-134,239-242` | `connection.getNode().getVersionInformation().nodeVersion()` compared with `Version.V_9_5_0` via `before()` | Fails closed a batched query-phase suggest request (`SearchQueryThenFetchAsyncAction.NODE_SEARCH_ACTION_NAME`) carrying `_sg_issuggest` when the target node runs ES < 9.5.0, because such a node would not see a `ShardSearchRequest` and would skip DLS on completion suggestions. The **only** wire-level, per-target-node version gate in the plugin. |
| `security/.../SearchGuardPlugin.java:678`, `ssl/.../SearchGuardSSLPlugin.java:235` | `TransportVersion.current()` passed to `SearchGuardSSLNettyTransport` (`ssl/.../transport/SearchGuardSSLNettyTransport.java:60`) | Not a check; ES handshake negotiation is left entirely to ES. |
| `support/.../action/Action.java:112-113,183-184` (write), `:272-273,337-358` (read) | two literal version bytes `0,0` | Carried on every generic message, never evaluated. A future reader could branch on them. |
| `dlic-security/.../authtoken/api/AuthTokenInfoResponse.java:40,47` | literal `int 1` | Read and discarded. |
| `security/.../SearchGuardCapabilities.java:83,217` with aggregation at `:131-135,151` | `SearchGuardVersion.getVersion()` string exchanged per node | Cluster-wide capability set and comma-joined version list; informational, feeds `capabilities` API, not serialization. |
| `security/.../license/SearchGuardLicense.java:93,113` | `majorVersion` optVInt | License compatibility, not wire. |
| `security/.../MultiTenancyChecker.java:62` | `IndexVersions.V_8_8_0` on `getCreationVersion()` | Startup check of frontend index creation version; not wire. |
| `support/.../meta/MetaImpl.java:530` | `Version.CURRENT` as `index.version.created` for synthetic metadata | Not wire. |
| `dlic-auditlog/.../impl/AuditMessage.java:133` | `localNode.getVersion()` | Audit field only. |
| none | `TransportVersions`, `minimumCompatibilityVersion`, `getTransportVersion()`, `onOrAfter` | No occurrences in any `src/main` tree. |

## 4. Thread context headers and transients

Request headers are `String`-valued and serialized by ES on every outgoing transport request **only if the Search Guard interceptor keeps them** (see "Interceptor" below); transients never leave the node. Writers/readers abbreviated: `SGC` = `security/.../support/SearchGuardContext.java`, `SGF` = `security/.../filter/SearchGuardFilter.java`, `SGI` = `security/.../transport/SearchGuardInterceptor.java`, `SGRH` = `security/.../transport/SearchGuardRequestHandler.java`, `CC` = `security/.../support/ConfigConstants.java`, `AAL` = `dlic-auditlog/.../impl/AbstractAuditLog.java`.

### 4.1 Request headers (serialized)

| Constant (`CC` line) | Value | Written by | Read by | Crosses nodes | Encoding |
|---|---|---|---|---|---|
| `SG_USER_HEADER` (`:68`) | `_sg_user_header` | `SGC:45,63`; `SGF:205` (search requests, for the chunked fetch path) | `SGC:40,51`; `AAL:1109-1110` | yes (`SGI:155`) | base64 of **Java-serialized `User`** (`security/.../support/Base64Helper.java:44-58`) |
| `SG_REMOTE_ADDRESS_HEADER` (`:59`) | `_sg_remote_address_header` | `SGC:76,96` (from REST auth `RestAuthenticationProcessor.java:166`, `SessionService.java:367`, `SGRH:168,182`) | `SGC:71,84`; `AAL:1100-1102` | yes (`SGI:154`) | base64 Java-serialized `InetSocketAddress` |
| `SG_ORIGIN_HEADER` (`:28`) | `_sg_origin_header` | `SGC:110,128` (`SGRH:158` sets `TRANSPORT`, `SGF:158` reads) | `SGC:105`; `AAL:1091-1092` | yes (`SGI:153`) | plain string (`Origin` enum name) |
| `SG_CONF_REQUEST_HEADER` (`:53`) | `_sg_conf_request` | `security/.../support/PrivilegedConfigContext.java:58` | `SGF:170`, `security/.../authz/indices/SearchGuardDirectoryReaderWrapper.java:92`, `dlic-dlsfls/.../DlsFlsBaseContext.java:77`, `dlic-auditlog/.../sink/InternalESSink.java:64` (all via `HeaderHelper.getSafeFromHeader`, `security/.../support/HeaderHelper.java:41-59`, which only honours it on inter-cluster/trusted/direct requests) | yes (`SGI:152`) | `"true"` |
| `SG_DLS_QUERY_HEADER` (`:30`), `SG_FLS_FIELDS_HEADER` (`:38`), `SG_MASKED_FIELD_HEADER` (`:40`) | `_sg_dls_query`, `_sg_fls_fields`, `_sg_masked_fields` | only `SGI:200-206` (re-injected from CCS transients when talking to a remote cluster) | no reader in `src/main`; read as *response* headers at `SGI:313-315` | yes (`SGI:156-158`), stripped for remote search-shards/search (`SGI:180-186`) | opaque strings (legacy CCS DLS/FLS exchange) |
| `SG_DLS_FILTER_LEVEL_QUERY_HEADER` (`:32`) | `_sg_dls_filter_level_query` | none | none | allow-listed (`SGI:162`) | dead constant |
| `SG_FILTER_LEVEL_DLS_DONE` (`:45`) | `_sg_filter_level_dls_done` | `dlic-dlsfls/.../filter/DlsFilterLevelActionHandler.java:142` | `:79`, `dlic-dlsfls/.../DlsFlsValve.java:110`, `DlsFlsBaseContext.java:60,68` | yes (`SGI:160`) | `request.toString()` |
| `SG_DLS_MODE_HEADER` (`:35`) | `_sg_dls_mode` | `DlsFlsValve.java:308` | `DlsFlsValve.java:303-305,313` | yes (`SGI:161`) | `DlsFlsConfig.Mode` enum name |
| `SG_DOC_WHITELST_HEADER` (`:42`) | `_sg_doc_whitelist` | `security/.../authz/DocumentWhitelist.java:60` | `DocumentWhitelist.java:31`, `security/.../authz/PrivilegesEvaluator.java:723` | yes (`SGI:159`) | `DocumentWhitelist.toString()` text format |
| `SG_AUTHZ_HASH_THREAD_CONTEXT_HEADER` (`:65`) = `ThreadContextAuthzHashProvider.THREAD_CONTEXT_HEADER` | `_sg_dls_fls_authz` | `dlic-dlsfls/.../ThreadContextAuthzHashProvider.java:45,52` (only if `PROVIDE_THREAD_CONTEXT_AUTHZ_HASH`, `DlsFlsModule.java:60`) | no reader in this repo | yes (`SGI:163`) | `"0"` or SHA-256 hex of the restriction description |
| `SG_IS_SUGGEST_HEADER` (`:63`) | `_sg_issuggest` | `SGRH:112`, `DlsFlsValve.java:162` | `SGI:128`, `SGRH:111`, `dlic-dlsfls/.../lucene/DlsFlsDirectoryReader.java:496-497` | yes (`SGI:164`) | `"true"` |
| (literal) | `_sg_source_field_context` | `SGF:398,403` (compliance read-history) | `dlic-auditlog/.../access_log/read/FieldReadCallback.java:50` | yes except on `SearchRequest`/`GetRequest` (`SGI:165`) | base64 Java-serialized `SourceFieldsContext` (`security/.../support/SourceFieldsContext.java:26-36`) |
| `SG_INITIAL_ACTION_CLASS_HEADER` (`:61`) | `_sg_initial_action_class_header` | `SGRH:216,220` | `SGRH:93`, `AAL:596` | yes (`SGI:167`, prefix match) | request class simple name |
| (literal) | `_sg_remotecn` | `SGI:149` on **every** outgoing request | `SGRH:233` | yes | local cluster name |
| (literal prefix) | `_sg_trace…` | `SGI:217`, `SGRH:121,193,204`, `SGF:230` (trace logging only) | none | yes (`SGI:166`) | debug text |
| `DiagnosticContext.ACTION_STACK_HEADER` (`support/.../diag/DiagnosticContext.java:51`) | `x_action_stack` | `SGI:211`, `security/.../support/PrivilegedConfigClient.java:58` | `DiagnosticContext.java:180` | yes (explicit put) | text |
| custom headers matching `searchguard.allow_custom_headers` | user defined | callers | callers | yes (`SGI:168,244-283`); `_sg_` prefix always refused (`:245`) | opaque |
| `SG_USER_NAME` (`:69`) | `_sg_user_name` | `SGF:343` | none in repo | **no** (not allow-listed) | user name |
| `InternalAuthTokenProvider.TOKEN_HEADER` / `AUDIENCE_HEADER` (`security/.../internalauthtoken/InternalAuthTokenProvider.java:61-62`) | `_sg_internal_auth_token`, `_sg_internal_auth_token_audience` | `scheduler/.../execution/AuthorizingJobDecorator.java:34-35`; cleared by `PrivilegedConfigClient.java:54-55`, `signals/.../watch/result/WatchLogIndexWriter.java:63-64` | `InternalAuthTokenProvider.java:141-142` | **no** (not allow-listed) | JWT: HS512-signed, optionally JWE `A256KW`/`A256CBC-HS512` (`:64,117-127`); `sg_roles` claim embeds the user's role definitions (`:114,163`) |
| `MultiTenancyAuthorizationFilter.SG_FILTER_LEVEL_FEMT_DONE` (`dlic-fe-multi-tenancy/.../MultiTenancyAuthorizationFilter.java:69`) | `_sg_filter_level_femt_done` | femt request handlers (`request/handler/BulkRequestHandler.java:44` etc.), `FrontendDataMigrationInterceptor.java:131,152,191` | `MultiTenancyAuthorizationFilter.java:126` | **no** | `request.toString()` |

Incoming REST requests carrying any `_sg_` header are rejected (`security/.../authc/rest/AuthenticatingRestFilter.java:289-298`); incoming transport requests carrying `_sg_ssl_` headers are rejected (`ssl/.../transport/SearchGuardSSLRequestHandler.java:82-88`). Other `_sg_` *request* headers arriving over transport are **not** stripped; `HeaderHelper.getSafeFromHeader` (`HeaderHelper.java:54`) is the only guard and only for the few headers read through it.

### 4.2 Transients (never serialized)

| Constant | Value | Written by | Read by |
|---|---|---|---|
| `SG_USER` (`CC:67`) | `_sg_user` | `SGC:52,66` | `SGF:166`, every transport action (e.g. `signals/.../TransportPutWatchAction.java:52`), `SearchGuardDirectoryReaderWrapper.java:86`, `AAL:1108`, `SearchGuardPlugin.java:573,586` |
| `SG_REMOTE_ADDRESS` (`CC:56`) | `_sg_remote_address` | `SGC:85,100` | `security/.../authc/AuthInfoService.java:69`, `dlic-security/.../AuthTokenService.java:855`, `AAL:1099` |
| `SG_ORIGIN` (`CC:27`) | `_sg_origin` | `SGC:116,131` | `SGF:279`, `SearchGuardPlugin.java:566`, `AAL:1089`, `DiagnosticContext.java:138` |
| `SG_CHANNEL_TYPE` (`CC:25`) | `_sg_channel_type` | `SGRH:105` | `HeaderHelper.java:36-37`, `DiagnosticContext.java:139` |
| `SG_ACTION_NAME` (`CC:75`) | `_sg_action_name` | `SGRH:106` | `DlsFlsDirectoryReader.java:492` |
| `SG_XFF_DONE` (`CC:70`) | `_sg_xff_done` | `RestAuthenticationProcessor.java:163`, `SessionService.java:364` | none found |
| `SSO_LOGOUT_URL` (`CC:72`) | `_sg_sso_logout_url` | (auth frontends, UNVERIFIED) | `SessionService.java:312`, `security/.../rest/SearchGuardInfoAction.java:126` |
| `SG_DLS_QUERY_CCS`, `SG_FLS_FIELDS_CCS`, `SG_MASKED_FIELD_CCS` (`CC:47,49,51`) | `_sg_dls_query_ccs`, `_sg_fls_fields_ccs`, `_sg_masked_fields_ccs` | `SGI:321-329` from response headers of `ClusterSearchShardsResponse`/`SearchShardsResponse` | `SGI:137-139` |
| `SG_DLS_MODE_TRANSIENT`, `SG_DLS_FILTER_LEVEL_QUERY_TRANSIENT`, `SG_DOC_WHITELST_TRANSIENT` (`CC:36,33,43`) | `…_t` | none | none (dead) |
| `SSLConfigConstants.SG_SSL_TRANSPORT_PRINCIPAL` and literals (`ssl/.../util/SSLConfigConstants.java:106`; `SearchGuardSSLRequestHandler.java:133-137`) | `_sg_ssl_transport_principal`, `_sg_ssl_transport_peer_certificates`, `_sg_ssl_transport_local_certificates`, `_sg_ssl_transport_protocol`, `_sg_ssl_transport_cipher` | `SearchGuardSSLRequestHandler.java:133-137` | `SGRH:148`, `TransportWhoAmIAction.java:58` |
| `SG_SSL_TRANSPORT_INTERCLUSTER_REQUEST`, `SG_SSL_TRANSPORT_TRUSTED_CLUSTER_REQUEST` (`SSLConfigConstants.java:100-101`) | `_sg_ssl_transport_intercluster_request`, `_sg_ssl_transport_trustedcluster_request` | `SGRH:239,241` | `HeaderHelper.java:31,73` and all callers of `isInterClusterRequest`/`isTrustedClusterRequest` |
| HTTP-side `SG_SSL_PRINCIPAL`, `SG_SSL_PEER_CERTIFICATES`, `SG_SSL_LOCAL_CERTIFICATES`, `SG_SSL_PROTOCOL`, `SG_SSL_CIPHER` (`SSLConfigConstants.java:90-94`) | `_sg_ssl_principal` … | `ssl/.../http/netty/SSLInfoPopulatingDispatcher.java:169-175` | REST auth (`RestAuthenticationProcessor.java:135`), `SearchGuardInfoAction.java:95` |
| `DiagnosticContext.ACTION_STACK_EXTENSION_TRANSIENT` | (see `DiagnosticContext.java:82-83`) | `:83,105` | `:175` |
| `ReaderContext` keys (not ThreadContext) `_sg_scroll_auth_local`, `_sg_scroll_auth` | `SearchGuardPlugin.java:570,573` | `:582-583` | scroll-owner check, stores the `User` object in the local reader context |

### 4.3 Response headers

`SGI:141,147,224-232` copies all current response headers into the stashed context before sending; `SGI:311-331` reads `_sg_fls_fields`, `_sg_dls_query`, `_sg_masked_fields` from the *response* headers after a `ClusterSearchShardsResponse`/`SearchShardsResponse` and caches them as the `*_CCS` transients. `PrivilegedConfigContext.java:54,62` preserves response headers across the privileged stash. **No code in this repository adds those three response headers** (the only `addResponseHeader` calls are `SGI:229` and `PrivilegedConfigContext.java:62`), so this path only ever receives data from a remote cluster running a different implementation (UNVERIFIED which versions emit them).

### 4.4 Interceptor (send side) and request handler (receive side)

Registered by `SearchGuardPlugin.getTransportInterceptors` (`security/.../SearchGuardPlugin.java:635-665`): `interceptSender` delegates to `SearchGuardInterceptor.sendRequestDecorate`, `interceptHandler` wraps every handler in `SearchGuardRequestHandler` (`:645,657`; interceptor constructed at `:848`). In `searchguard.ssl_only` mode (and when `searchguard.disabled` is set) **no transport interceptor is registered at all**: `getTransportInterceptors` returns the empty list without calling `super` (`SearchGuardPlugin.java:635-665`), so `SearchGuardSSLPlugin.getTransportInterceptors` (`ssl/.../SearchGuardSSLPlugin.java:218-222`) is unreachable through this entry point. (Corrected after cross-check; see README.)

**Send (`SGI:108-222`)**: (1) version gate for batched suggest (`:128-134`); (2) snapshot headers, CCS transients, action stack, response headers (`:136-141`); (3) `stashContext()` so the outgoing request starts from an empty context (`:146`); (4) re-add response headers, wrap the handler so the caller's context is restored on response (`:147-148,288-333`); (5) put `_sg_remotecn` (`:149`); (6) copy **only** the allow-listed headers from the original context (`:151-169`): `_sg_conf_request`, `_sg_origin_header`, `_sg_remote_address_header`, `_sg_user_header`, `_sg_dls_query`, `_sg_fls_fields`, `_sg_masked_fields`, `_sg_doc_whitelist`, `_sg_filter_level_dls_done`, `_sg_dls_mode`, `_sg_dls_filter_level_query`, `_sg_dls_fls_authz`, `_sg_issuggest`, `_sg_source_field_context` (not on search/get), `_sg_trace*`, `_sg_initial_action_class_header*`, custom allow-list; everything else (including ES's own headers already in the context, UNVERIFIED whether ES re-adds them downstream) is dropped; (7) CCS stripping/re-injection (`:171-208`); (8) `x_action_stack` (`:210-212`); (9) send (`:220`). Nothing is validated on send; `User` is already base64 in the header.

**Receive (`SearchGuardSSLRequestHandler.messageReceived` `ssl/...:78-159`, then `SGRH.messageReceivedDecorate` `:78-211`)**: reject any `_sg_ssl_*` header (`ssl:82-88`); direct (in-JVM) channels bypass TLS extraction (`ssl:91-94`, `SGRH:117-128`); otherwise extract peer/local certs from the Netty `ssl_server` handler, compute principal (`ssl:108-130`), call `addAdditionalContextValues` (`SGRH:227-251`: `InterClusterRequestEvaluator` decides node-vs-non-node; equal `_sg_remotecn` → `intercluster`, else → `trustedcluster`), set SSL transients (`ssl:133-137`); then `SGRH`: set `_sg_channel_type`, `_sg_action_name` (`:105-106`), derive `_sg_issuggest` from `ShardSearchRequest` (`:108-114`), deny `internal:*`/shard-level (`[`) actions from non-node certs (`:133-143`), require a transport principal (`:148-154`), default origin to `TRANSPORT` (`:157-159`), for node requests deserialize the cached `User`/remote address/origin from headers (`SearchGuardContext.initializeTransientCaches`, `:165-169`), for non-node certs accept only admin DNs as a TLS-cert user (`:171-190`), record `_sg_initial_action_class_header` (`:197,213-224`). No header *values* are validated (e.g. the `User` header is trusted as-is once the peer is a node certificate).

## 5. User and authentication state serialization

| Item | Format | Where | Breaks if… |
|---|---|---|---|
| `User` in `_sg_user_header` | Java serialization (`ObjectOutputStream`) → base64; read through `SafeObjectInputStream` allow-list (`security/.../support/Base64Helper.java:44-58,61-118`: `String`, `SocketAddress`, `InetSocketAddress`, `InetAddress`, `Number`, `Boolean`, `Collection`, `Map`, `Enum`, `User`, `SourceFieldsContext`, `com.floragunn.dlic.auth.ldap.LdapUser`, `java.util.CollSer`) | `SearchGuardContext.java:45,51,63`; `SGF:205`; `AAL:1110` | Fields (`security/.../user/User.java:52-71`): `serialVersionUID = -5500938501822658596L`, `name`, `subName`, `type`, `authDomain` (String), `roles` Set, `searchGuardRoles` Set, `specialAuthzConfig` Object (auth-token id String per `dlic-security/.../AuthTokenAuthenticationDomain.java:68,77`), `requestedTenant`, `attributes` Map<String,String>, `structuredAttributes` Map<String,Object>, `isInjected` kept "for java serialization interop" (`:68-70`), `authzComplete` transient. With the fixed `serialVersionUID`: **adding** a field → older reader ignores it, newer reader gets `null`/`false` from older writers (silent privilege/attribute loss); **removing** a field → old value ignored; **renaming** → value silently dropped on both sides (e.g. renaming `roles` empties backend roles cross-version); **changing a field type** or the class/package name → `InvalidClassException`/`ClassNotFoundException` and the request fails with "No user in context"-class errors; any non-allow-listed value class inside `structuredAttributes` → rejected on the reader (`Base64Helper.java:107-118`). No custom `writeObject`/`readObject` (grep `User.java` found none). |
| Remote address in `_sg_remote_address_header` | Java-serialized `InetSocketAddress` → base64 | `SearchGuardContext.java:77,84,97` | JDK-level format; stable. |
| Origin | plain enum name string | `SearchGuardContext.java:104-133` | New `Origin` constant unknown to an older node → `Origin.valueOf` failure where parsed (UNVERIFIED call sites). |
| `SourceFieldsContext` in `_sg_source_field_context` | Java serialization, `serialVersionUID = 1L`, fields `includes`, `excludes`, `fetchSource` (`SourceFieldsContext.java:26-36`) | `SGF:397,402`; `FieldReadCallback.java:50` | Same rules as `User`. |
| `AuthToken`, `AuthTokenPrivilegeBase`, `RequestedPrivileges`, `ConfigVersionSet` | positional `Writeable` (section 2); classes are also `Serializable` (`AuthToken.java:35`, `AuthTokenPrivilegeBase.java:42`, `RequestedPrivileges.java:43`) but Java serialization of them is not used on the wire in `src/main` (UNVERIFIED for caches) | `TransportPushAuthTokenUpdateAction.NodeRequest:72` (nodes action, fans out to every node) | Any field add/remove/reorder breaks the nodes fan-out between mixed plugin versions, with no version gate. |
| `SessionToken`, `SessionPrivileges` | positional `Writeable` (section 2), also `Serializable` (`SessionToken.java:38`) | `PushSessionTokenUpdateAction.NodeRequest:140` (nodes action) | Same as above. |
| Auth-token / session / internal JWTs | compact JWT strings (session audience `sg_session_<cluster>` `SessionService.java:832-836`; internal token `InternalAuthTokenProvider.java:103-127`, claim `sg_i`=`n`, `sg_roles` map of role definitions) | REST responses (`CreateAuthTokenResponse.java:48`), Watch metadata (`signals/.../SignalsTenant.java:369`, stored in index) | Signing/encryption keys are config vars (`AuthTokenModule.java:100`, `SessionModule.java:85`, `signals/.../Signals.java:270-279`); a key or algorithm change invalidates tokens issued by other nodes. Not an inter-node transport message, but internal tokens embedded in stored watches are verified on whichever node runs the watch. |
| TLS transport principal | DN string from `X509Certificate.getSubjectX500Principal()` (`ssl/.../transport/DefaultPrincipalExtractor.java:41-49`) | transient only | Not serialized. |

## 6. Cross-cluster (CCS / remote clusters)

| Aspect | Behaviour | Reference |
|---|---|---|
| Detection of "remote" | `remoteClusterService.getRegisteredRemoteClusterNames()` non-empty and `!clusterInfoHolder.hasNode(connection.getNode())` (target not in local cluster state; `hasNode` returns `null` until initialized, guarded by `isInitialized()`) | `SGI:142-143,171-176,189-193`; `security/.../configuration/ClusterInfoHolder.java:35-64` |
| Headers that travel to the remote cluster | Same allow-list as intra-cluster (incl. `_sg_user_header`, `_sg_origin_header`, `_sg_remote_address_header`, `_sg_conf_request`, `_sg_remotecn`, `x_action_stack`) | `SGI:149-169,210-214` |
| Special casing on send | For `indices:admin/shards/search_shards` and `indices:data/read/search` to a remote node: remove `_sg_dls_query`, `_sg_dls_mode`, `_sg_masked_fields`, `_sg_fls_fields`, `_sg_filter_level_dls_done`, `_sg_dls_filter_level_query`, `_sg_doc_whitelist`. For any non-`internal:` non-search-shards action to a remote node: inject DLS/FLS/masked-field values captured from the remote's earlier search-shards *response* headers | `SGI:171-208` |
| Special casing on response | After `ClusterSearchShardsResponse`/`SearchShardsResponse`, capture `_sg_fls_fields`/`_sg_dls_query`/`_sg_masked_fields` response headers into `*_CCS` transients | `SGI:319-331` |
| Receive side on the remote cluster | Peer cert passes `InterClusterRequestEvaluator` but `_sg_remotecn` differs → `_sg_ssl_transport_trustedcluster_request`; treated like a node request (`HeaderHelper.isTrustedClusterRequest`), i.e. the serialized `User` from the other cluster is accepted and used for authorization on the remote cluster | `SGRH:232-242,162-169`; `SGF:168-175` |
| Authorization of remote index expressions | `ActionRequestIntrospector` splits `cluster:index` expressions (`IndexExpression.REMOTE_CLUSTER_INDEX_SEPARATOR`, `isRemoteIndex`) into `localIndices`/`remoteIndices`; remote parts are not resolved against local metadata | `security/.../authz/actions/IndexExpression.java:68,172-174`; `ActionRequestIntrospector.java:784-833` |
| DLS filter-level and FEMT on search-shards | `DlsFilterLevelActionHandler` and femt `ClusterSearchShardsRequestHandler` rewrite `ClusterSearchShardsRequest` on the coordinating node | `dlic-dlsfls/.../filter/DlsFilterLevelActionHandler.java:168-169,335`; `dlic-fe-multi-tenancy/.../request/handler/ClusterSearchShardsRequestHandler.java:28-36` |
| Trust prerequisite | The remote cluster must list the local node certificates in its `searchguard.nodes_dn` (or share the cert OID), otherwise the request is treated as a non-node request and only admin DNs pass | `security/.../transport/DefaultInterClusterRequestEvaluator.java:42-66,74-125` |

## 7. Transport-level authentication

| Step | Where |
|---|---|
| Transport TLS server context requires client certificates (`ClientAuth.REQUIRE`), also on reload | `ssl/.../DefaultSearchGuardKeyStore.java:333,381` |
| Client side: hostname verification on by default (`searchguard.ssl.transport.enforce_hostname_verification`, resolve-hostname variant) | `ssl/.../transport/SearchGuardSSLNettyTransport.java:150-188` |
| Peer/local certificate extraction from the Netty `ssl_server` handler; failure → `ElasticsearchException` "No ssl handler found (SG 11)" / "No X509 transport client certificates found (SG 12)" | `ssl/.../transport/SearchGuardSSLRequestHandler.java:96-147` |
| Principal = subject X.500 DN (pluggable via `searchguard.ssl.transport.principal_extractor_class`, `SSLConfigConstants.java:71`) | `ssl/.../transport/DefaultPrincipalExtractor.java:41-49` |
| Node identity: principal (with and without spaces) wildcard-matched against `searchguard.nodes_dn` (`ConfigConstants.java:153`), else SAN entry of type 8 containing `searchguard.cert.oid` (default `1.2.3.4.5.5`) | `security/.../transport/DefaultInterClusterRequestEvaluator.java:42-125`; evaluator class is pluggable via `searchguard.cert.intercluster_request_evaluator_class` (`SearchGuardPlugin.java:763-769`, `security/.../support/ReflectionHelper.java:155`) |
| Node vs. trusted remote cluster: `_sg_remotecn` equals local cluster name → `intercluster`, else `trustedcluster` | `SGRH:227-251` |
| Non-node certificate: only DNs in `searchguard.authcz.admin_dn` are accepted (as `User(principal, TLS_CERT)`), everything else "Transport request from untrusted node denied" (403) | `SGRH:171-190`; `security/.../configuration/AdminDNs.java:46,68,76` |
| `internal:*` and shard-level actions from non-node certificates are refused (except `internal:transport/handshake`) | `SGRH:133-143` |
| In-JVM direct channels skip all of the above | `ssl/.../SearchGuardSSLRequestHandler.java:91-94,161-171`; `SGRH:103,117-128` |
| Scroll contexts bind the `User` object at creation and re-check on every scroll | `SearchGuardPlugin.java:561-598` |

## Mixed-cluster and compatibility relevance

Ranked by blast radius during a rolling upgrade (older and newer plugin, possibly older and newer ES, in one cluster).

1. **`User` Java serialization in `_sg_user_header`** (`SearchGuardContext.java:45,63`, `Base64Helper.java:44-118`, `User.java:52-71`). Every authenticated request to every data node carries it. The fixed `serialVersionUID` makes add/remove tolerable but *silent*; a rename, type change, package move, or a new non-allow-listed attribute value class breaks or degrades authorization on the other side with no version negotiation. Same mechanism for `SourceFieldsContext` and `InetSocketAddress`.
2. **The interceptor allow-list is the de-facto protocol** (`SGI:151-169`). A header introduced on a new node is silently dropped by an old coordinating node (it is not on the old allow-list), and a header the new node *requires* (e.g. `_sg_issuggest`, `_sg_dls_mode`, `_sg_dls_fls_authz`) will be missing on requests from old nodes. The 9.5 batched-suggest gate at `SGI:239-242` exists precisely because of this; it is the only such gate.
3. **Nodes-action fan-outs with positional `Writeable`s and no version gate**: `TransportPushAuthTokenUpdateAction.NodeRequest` (carries full `AuthToken`/`RequestedPrivileges`), `PushSessionTokenUpdateAction.NodeRequest` (`SessionToken`), `TransportConfigUpdateAction.NodeConfigUpdateRequest`, `GetComponentStateAction.NodeRequest/NodeResponse`, `SearchGuardCapabilities.NodeResponse`, `LicenseInfoNodeResponse`/`SearchGuardLicense`, `ConfigVarRefreshAction`, `ProtectedConfigIndexService`, `AuthcCacheApi`, scheduler `CheckForExecutingTrigger`/`SchedulerConfigUpdate`, Signals `AckWatch`/`AckAndGetWatch`/`DestinationConfigUpdate`/`SettingsUpdate`/`TruststoreUpdated`/`ProxyUpdated`. These run cluster-wide by design (config reload, token revocation, capability discovery); a field change breaks the whole fan-out with `EOFException`/garbage decoding and can leave old nodes with stale security state.
4. **Enum ordinals on the wire** (`writeEnum`) in all of the above and in Signals responses: inserting an enum constant anywhere but the end re-maps values across versions.
5. **`SearchGuardCapabilities` / `SearchGuardVersion`** (`SearchGuardCapabilities.java:131-161`): capabilities are intersected cluster-wide, so a feature gated on a capability is off until every node is upgraded — this is the *only* built-in mixed-version coordination and it covers features, not message formats.
6. **CCS legacy DLS/FLS header exchange** (`SGI:171-208,319-331`): receive-only, no local producer; remote clusters on other versions may send `_sg_dls_query` etc. that nothing here evaluates. Low risk but dead-weight that may mask behaviour differences.
7. **Generic `Action` envelope** (`Action.java`): SMILE body is self-describing, so field additions are safe; but the two version bytes are never checked and the body parser rejects unknown/missing required keys per action, so semantic changes still need care. All of these are local-only in practice (REST → local node).
8. **ES-side dependencies with no guard**: `TransportVersion.current()` is simply forwarded (`SearchGuardPlugin.java:678`); `BaseNodesRequest` being local-only is an ES 9 property (`javap` on 9.4.4) — on `main-es8` the same classes may still serialize the top-level request (UNVERIFIED), so backports of nodes-action changes need separate review.

Messages with **no version gating at all**: every entry in section 2 except the ES-provided base classes; i.e. all plugin-defined `Writeable`s.

## Open questions for human review

1. Is `User` meant to stay Java-serialized long term, or should `_sg_user_header` move to a versioned, self-describing encoding (e.g. the same SMILE envelope as `Action`)? The comment at `User.java:68-70` suggests interop with older serialized forms is already a concern.
   - **Answer:** _pending_
2. Which deployed Search Guard versions, if any, still emit `_sg_dls_query`/`_sg_fls_fields`/`_sg_masked_fields` as *response* headers for CCS (`SGI:319-331`)? If none, the whole CCS DLS/FLS header path (`SGI:137-139,171-208`) and the dead constants at `ConfigConstants.java:32-33,36,43` can be removed.
   - **Answer:** _pending_
3. `_sg_dls_fls_authz` is written (`ThreadContextAuthzHashProvider.java:45,52`) and forwarded (`SGI:163`) but never read in this repository; who consumes it (Kibana plugin, external auditing)?
   - **Answer:** _pending_
4. The allow-list drops all non-SG headers except custom ones (`SGI:151-169`); it is UNVERIFIED whether ES re-adds its own system headers (e.g. `X-Opaque-Id`, task/trace headers) after the interceptor, or whether the stash at `SGI:146` strips them for good.
   - **Answer:** _pending_
5. Should `HandledTransportAction` messages that are only ever invoked locally (`WhoAmI` already does this) declare `localOnly()` to shrink the attack surface and the compatibility surface?
   - **Answer:** _pending_
6. `AuthTokenInfoResponse` writes a literal `1` (`:47`) and the generic envelope writes `0,0` version bytes; is there a convention intended here that should be applied (and enforced) consistently?
   - **Answer:** _pending_
7. `main-es8` behaviour of `BaseNodesRequest` serialization and of the `ClusterSearchShardsResponse`/`SearchShardsResponse` response-header capture was not checked (this pass covers `main` at ES 9.x only).
   - **Answer:** _pending_
