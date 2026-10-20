# REST surface and request entry

*Inventory pass, generated 2026-10-19 on branch experimental-analysis at commit 374449897. Every claim carries a file reference. Items marked UNVERIFIED were inferred, not read.*

## Summary for maintainers

*Written for human readers. The tables below are the authoritative record; this section is a guide to them and must be re-read whenever a `RestHandler` is added or changed, or `AuthenticatingRestFilter` changes.*

**What this covers.** Every HTTP endpoint the plugin exposes, how a request is authenticated before it reaches a handler, how request bodies and responses are shaped, which authentication methods exist over HTTP, and which endpoints are contracts with the Kibana plugin and with sgctl.

**What you need to know.**

- There are about 65 handlers and 177 routes, spread over the core, the enterprise security module, multi-tenancy, DLS/FLS, Signals and SSL.
- Authentication happens in the HTTP dispatcher before Elasticsearch routing. Authorization happens only when a handler dispatches a transport action. Handlers that answer locally, such as authinfo, kibanainfo, whoami, sslinfo, the session read and the has-privileges mock, are never privilege-checked.
- The admin certificate bypasses both authentication and authorization.
- Four paths need no authentication at all: OPTIONS, the legacy license endpoint, health, and session login.
- Two generations of configuration API coexist, the legacy `/_searchguard/api` tier and the newer `/_searchguard/config` tier. Which one is the frozen contract for sgctl and the Kibana UI is undecided, see D-4 in `../decisions.md`.
- What the Kibana plugin and sgctl actually call cannot be determined from this repository. The lists in sections 6 and 7 are inferred from static roles and endpoint design.
- Elasticsearch has changed how request bodies are handled twice recently, ref-counting in 8.18 and a type change in 9.5.5, and both required fixes here. Body handling is a known hotspot for ES upgrades.

**What to review.**

- Section 1, the "Who may call" column for rows marked authn (open question 5) and the unauthenticated paths (open question 4).
- Sections 6 and 7, to be confirmed against the Kibana plugin and sgctl repositories (open question 7). The answer feeds D-4.
- Section 2, the order of steps for a REST call, which the rulebook will cite.

## Scope and method

Read-only pass over `src/main` of `security`, `ssl`, `support`, `signals`, `dlic-security`, `dlic-fe-multi-tenancy`,
`dlic-dlsfls`, `dlic-auditlog`, `dlic-signals`, `scheduler`. Handlers were found by grepping for `BaseRestHandler`,
`RestApi`, `routes()`, `new Route(` and `handlesGet/Post/Put/Patch/Delete(`, then cross-checked against the registration
points (`SearchGuardPlugin.getRestHandlers`, each module's `getRestHandlers`). `sgi8/`, `audit/`, `target/` and
`src/test` were ignored. Line numbers are `path:line` in the working tree at the commit above. No build was run.

Count: **65 RestHandler instances** registering **177 routes** (route total UNVERIFIED after the recount below). Per module: security 28 handlers (25 at `SearchGuardPlugin.java:412-446`, `MockHasPrivilegesAction` at `:449`, two from `SessionModule.java:68-70`; the original count of 29 did not reproduce in cross-check) / 62 routes, ssl 1 / 1,
dlic-security 13 / 53, dlic-fe-multi-tenancy 2 / 10, dlic-dlsfls 1 / 3, signals 20 / 48. `dlic-auditlog`, `dlic-signals`
and `scheduler` register no REST handlers (grep for `RestHandler|RestApi` in their `src/main` returns nothing).

Legend for the "Who may call" column:

- **unauth** — authentication skipped by `AuthenticatingRestFilter.isAuthcRequired` (`security/.../authc/rest/AuthenticatingRestFilter.java:215-219`): any `OPTIONS`, `/_searchguard/license`, `/_searchguard/health`, `POST /_searchguard/auth/session`.
- **authn** — any authenticated user; the handler does its own work locally and no transport action is privilege-checked.
- **perm:`<action>`** — dispatched as an ES transport action; `SearchGuardFilter` grants it to the TLS admin cert without evaluation (`security/.../filter/SearchGuardFilter.java:167,240-246`) and otherwise requires a role granting the named cluster/tenant permission. No handler in this group does its own admin-DN check (grep for `isAdmin`/`AdminDNs` in those handlers finds nothing).
- **admin cert** — handler checks `AdminDNs` itself and returns 403 otherwise.
- **restapi** — legacy admin API: role listed in `searchguard.restapi.roles_enabled` OR admin cert (`dlic-security/.../dlic/rest/api/RestApiPrivilegesEvaluator.java:98,222-241,434-437`).
- **open** — action registered with `Scope.OPEN` / pass-through in the filter (`security/.../authz/actions/Actions.java:335-336`, `SearchGuardFilter.java:171-172`): any authenticated user.

"Tenant" column: **path** = tenant taken from the URL (`security/.../SignalsTenantParamResolver.java:17-50`); **hdr** = handler reads/returns the requested tenant that authc derived from the `sgtenant`/`sg_tenant` header (`security/.../authc/rest/RestRequestAuthenticationProcessor.java:175-184`); **no** = tenant-agnostic.

## 1. Endpoint catalogue

### 1.1 `security` (Apache 2) — registered in `security/src/main/java/com/floragunn/searchguard/SearchGuardPlugin.java:400-451`, plus `SessionModule` (`security/.../authc/session/backend/SessionModule.java:68-70`)

| Method | Path | Handler (ref) | Transport action | Who may call | Tenant |
|---|---|---|---|---|---|
| GET, POST | `/_searchguard/authinfo` | `rest/SearchGuardInfoAction.java:78` | none (local) | authn | hdr (returns `user_requested_tenant`, `sg_tenants` `:116,121`) |
| GET, POST | `/_searchguard/kibanainfo` | `rest/KibanaInfoAction.java:61` | none (local) | authn | no |
| GET, POST | `/_searchguard/health` | `rest/SearchGuardHealthAction.java:50` | none; `?mode=strict|lenient` `:57` | unauth | no |
| GET | `/_searchguard/permission?permissions=a,b` | `rest/PermissionAction.java:59,64` | none; `PrivilegesEvaluator.evaluateClusterAndTenantPrivileges` `:75` | authn | hdr |
| POST | `_searchguard/api/ssl/{certType}/reloadcerts/` | `rest/SSLReloadCertAction.java:44` | none (local keystore reload) | admin cert `:82-83`; also needs `sslCertReloadEnabled` | no |
| GET | `/_searchguard/component/{id}/_health` | `modules/api/ComponentStateRestAction.java:45` | `cluster:admin/searchguard/components/state` (`modules/api/GetComponentStateAction.java:79`, nodes action) | perm | no |
| GET | `/_searchguard/config` | `configuration/api/BulkConfigApi.java:70` | `cluster:admin:searchguard:config/bulk/get` `:77` | perm | no |
| PUT | `/_searchguard/config` | `BulkConfigApi.java:71` | `cluster:admin:searchguard:config/bulk/update` `:193` | perm | no |
| DELETE | `/_searchguard/config/{config_type}` | `configuration/api/GenericTypeLevelConfigApi.java:38` | `cluster:admin:searchguard:config/delete_by_type` `:43` | perm | no |
| POST | `/_searchguard/config/migrate_index` | `configuration/api/MigrateConfigIndexApi.java:39` | `cluster:admin:searchguard:config/migrate_index` `:35` | perm | no |
| GET | `/_searchguard/config/vars` | `configuration/variables/ConfigVarApi.java:43` | `…config/vars/get/all` `:213` | perm | no |
| GET | `/_searchguard/config/vars/{id}` | `ConfigVarApi.java:44` | `…config/vars/get` `:54` | perm | no |
| PUT | `/_searchguard/config/vars` | `ConfigVarApi.java:45` | `…config/vars/put/all` `:249` | perm | no |
| PUT | `/_searchguard/config/vars/{id}` | `ConfigVarApi.java:46` | `…config/vars/put` `:97` | perm | no |
| DELETE | `/_searchguard/config/vars/{id}` | `ConfigVarApi.java:47` | `…config/vars/delete` `:184` | perm | no |
| GET | `/_searchguard/internal_users/{id}` | `authc/internal_users_db/InternalUsersConfigApi.java:48` | `…config/internal_users/get` `:62` | perm | no |
| DELETE | `/_searchguard/internal_users/{id}` | `InternalUsersConfigApi.java:50` | `…config/internal_users/delete` `:178` | perm | no |
| PUT | `/_searchguard/internal_users/{id}` | `InternalUsersConfigApi.java:52` | `…config/internal_users/put` `:103` | perm | no |
| PATCH | `/_searchguard/internal_users/{id}` | `InternalUsersConfigApi.java:54` | `…config/internal_users/patch` `:218` | perm | no |
| GET | `/_searchguard/config/authc` | `authc/rest/RestAuthcConfigApi.java:39` | `…config/authc/get` `:47` | perm | no |
| PUT | `/_searchguard/config/authc` | `RestAuthcConfigApi.java:40` | `…config/authc/put` `:65` | perm | no |
| PATCH | `/_searchguard/config/authc` | `RestAuthcConfigApi.java:41` | `…config/authc/patch` `:81` | perm | no |
| DELETE | `/_searchguard/config/authc` | `RestAuthcConfigApi.java:42` | `…config/authc/delete` `:96` | perm | no |
| GET | `/_searchguard/config/authz` | `authz/config/AuthorizationConfigApi.java:38` | `…config/authz/get` `:50` | perm | no |
| PUT | `/_searchguard/config/authz` | `AuthorizationConfigApi.java:39` | `…config/authz/put` `:68` | perm | no |
| PATCH | `/_searchguard/config/authz` | `AuthorizationConfigApi.java:40` | `…config/authz/patch` `:84` | perm | no |
| DELETE | `/_searchguard/authc/cache` | `authc/rest/AuthcCacheApi.java:55` | `cluster:admin:searchguard:cache/delete` `:79` (nodes action) | perm | no |
| GET | `/_searchguard/config/authc_frontend` | `authc/session/FrontendAuthcConfigApi.java:36` | `…config/authc_frontend/_all/get` `:45` | perm | no |
| PUT | `/_searchguard/config/authc_frontend` | `FrontendAuthcConfigApi.java:37` | `…/_all/put` `:63` | perm | no |
| PATCH | `/_searchguard/config/authc_frontend` | `FrontendAuthcConfigApi.java:39` | `…/_all/patch` `:79` | perm | no |
| GET | `/_searchguard/config/authc_frontend/{id}` | `FrontendAuthcConfigApi.java:97` | `…/_id/get` `:107` | perm | no |
| PUT | `/_searchguard/config/authc_frontend/{id}` | `FrontendAuthcConfigApi.java:99` | `…/_id/put` `:125` | perm | no |
| PATCH | `/_searchguard/config/authc_frontend/{id}` | `FrontendAuthcConfigApi.java:101` | `…/_id/patch` `:141` | perm | no |
| GET | `/_searchguard/license/key` | `license/SearchGuardLicenseKeyApi.java:36` | `…config/license_key/get` `:48` | perm | no |
| PUT | `/_searchguard/license/key` | `SearchGuardLicenseKeyApi.java:37` | `…config/license_key/put` `:65` | perm | no |
| PATCH | `/_searchguard/license/key` | `SearchGuardLicenseKeyApi.java:38` | `…config/license_key/patch` `:81` | perm | no |
| GET | `/_searchguard/license/info` | `license/SearchGuardLicenseInfoAction.java:44` | `cluster:admin:searchguard:get_license_info` `:41` | perm | no |
| GET, POST | `/_searchguard/license` | `license/legacy/SearchGuardLicenseAction.java:43` | `cluster:admin/searchguard/license/info` (`license/legacy/LicenseInfoAction.java:33`, nodes action, `executeLocally` `:50`) | unauth (filter pass-through `SearchGuardFilter.java:171`) | no |
| GET | `/_searchguard/api/license` | `SearchGuardLicenseAction.java:44` | same as above | open | no |
| GET | `/_searchguard/capabilities/cluster_wide` | `SearchGuardCapabilities.java:324` | `cluster:admin/searchguard/capabilities/cluster_wide/get` `:320` (nodes action) | perm | no |
| POST | `/_searchguard/internal/indices/create` | `configuration/ProtectedConfigIndexService.java:677` | `cluster:admin:searchguard:internal/indices/create` `:673` (nodes action) | perm | no |
| GET | `/_searchguard/auth/config?config_id&next_url&frontend_base_url&…` | `authc/session/GetActivatedFrontendConfigAction.java:50` | `cluster:admin:searchguard:auth/frontend/config/get` `:47` | perm (granted to `SGS_KIBANA_SERVER`, `security/src/main/resources/static_config/static_roles.yml:294`) | no |
| POST | `/_searchguard/auth/config` | `GetActivatedFrontendConfigAction.java:53` | same | perm | no |
| GET | `/_searchguard/auth/debug` | `authc/rest/AuthenticatingRestFilter.java:274-278` | none; real response is produced inside authc when `authc.debug` is on `:168-170`; the handler itself always returns 404 | unauth-ish (authc runs, result is returned regardless of outcome) | no |
| GET | `/_searchguard/whoami` | `rest/SearchGuardWhoAmIAction.java:57` | none (reads `SG_SSL_PRINCIPAL`, `:65-66`) | authn | no |
| PUT | `/_searchguard/configupdate?config_types=` | `rest/SearchGuardConfigUpdateAction.java:53` | `cluster:admin/searchguard/config/update` (`action/configupdate/ConfigUpdateAction.java:25`, nodes action) | admin cert `:63-64` | no |
| GET, POST | `/_security/user/_has_privileges`, `/_security/user/{user}/_has_privileges` | `rest/mock/MockHasPrivilegesAction.java:62-63` | none; mock that "always returns true" `:48` | authn | no |
| GET | `/_searchguard/auth/session/extended` | `authc/session/backend/SessionApi.java:346` | `cluster:admin:searchguard:session/_own/get/extended` `:63` | perm (`SGS_PERSONAL_SESSIONS`, `static_action_groups.yml:287-295`) | no |
| GET | `/_searchguard/auth/session` | `SessionApi.java:347,353-368` | none; returns `sso_logout_url` | authn | no |
| POST | `/_searchguard/auth/session/with_header` | `SessionApi.java:348` | `cluster:admin:searchguard:session/create` `:216` | perm | no |
| POST | `/_searchguard/auth/session` | `SessionApi.java:349,370-396` | none; `SessionService.authenticateAndCreateSession` (body keys `config_id`, `mode`: `SessionService.java:379,394`) | unauth | no |
| DELETE | `/_searchguard/auth/session` | `SessionApi.java:350` | `cluster:admin:searchguard:session/_own/delete` `:149` | perm | no |
| GET | `/_searchguard/config/sessions` | `authc/session/backend/SessionServiceConfigApi.java:35` | `…config/sessions/get` `:47` | perm | no |
| PUT | `/_searchguard/config/sessions` | `SessionServiceConfigApi.java:36` | `…config/sessions/put` `:65` | perm | no |
| PATCH | `/_searchguard/config/sessions` | `SessionServiceConfigApi.java:37` | `…config/sessions/patch` `:81` | perm | no |

Notes: all handlers except `MockHasPrivilegesAction` are skipped when `searchguard.ssl_only` is set (`SearchGuardPlugin.java:411,449`). The legacy admin API handlers from `dlic-security` are injected here reflectively via `ReflectionHelper.instantiateMngtRestApiHandler` (`SearchGuardPlugin.java:419-421`, `security/.../support/ReflectionHelper.java:75-88`).

### 1.2 `ssl` (Apache 2) — `ssl/src/main/java/com/floragunn/searchguard/ssl/SearchGuardSSLPlugin.java:209-215`, reached through `super.getRestHandlers` (`SearchGuardPlugin.java:409`)

| Method | Path | Handler (ref) | Transport action | Who may call | Tenant |
|---|---|---|---|---|---|
| GET | `/_searchguard/sslinfo?show_dn&show_server_certs` | `ssl/rest/SearchGuardSSLInfoAction.java:58` | none; reads SSL transients `:76-111` | authn | no |

### 1.3 `dlic-security` (proprietary) — legacy admin API `dlic/rest/api/SearchGuardRestApiActions.java:40-66`; auth tokens `authtoken/AuthTokenModule.java:78-82`

| Method | Path | Handler (ref) | Transport action | Who may call | Tenant |
|---|---|---|---|---|---|
| GET `/{name}`, GET `/`, DELETE `/{name}`, PUT `/{name}`, PATCH `/`, PATCH `/{name}` | `/_searchguard/api/internalusers…` | `InternalUsersApiAction.java:69` via `PatchableResourceApiAction.java:77-81` | none; writes config index then `ConfigUpdateAction` fan-out (`AbstractApiAction.java:404`) | restapi | no |
| same 6 | `/_searchguard/api/roles…` | `RolesApiAction.java:53` | same | restapi | no |
| same 6 | `/_searchguard/api/rolesmapping…` | `RolesMappingApiAction.java:52` | same | restapi | no |
| same 6 | `/_searchguard/api/actiongroups…` | `ActionGroupsApiAction.java:53` | same | restapi | no |
| same 6 | `/_searchguard/api/tenants…` | `TenantsApiAction.java:52` | same | restapi | no |
| same 6 | `/_searchguard/api/blocks…` | `BlocksApiAction.java:53` | same | restapi | no |
| GET, PUT, POST, DELETE | `/_searchguard/api/cache` | `FlushCacheApiAction.java:60-61` | `ConfigUpdateAction` fan-out | restapi | no |
| PUT, POST, DELETE | `/_searchguard/api/license` | `LicenseApiAction.java:62-63` | config index write + `ConfigUpdateAction` | restapi | no |
| GET | `/_searchguard/api/permissionsinfo` | `PermissionsInfoAction.java:69` | none; reports disabled endpoints for caller | authn | no |
| POST | `/_searchguard/authtoken` | `authtoken/api/AuthTokenRestAction.java:45` | `cluster:admin:searchguard:authtoken/_own/create` (`authtoken/api/CreateAuthTokenAction.java:22`) | perm (`SGS_CREATE_MANAGE_OWN_AUTH_TOKEN`, `static_action_groups.yml:323-329`) | no |
| GET | `/_searchguard/authtoken/{id}` | `AuthTokenRestAction.java:45` | `…authtoken/_own/get` (`GetAuthTokenAction.java:22`) | perm | no |
| DELETE | `/_searchguard/authtoken/{id}` | `AuthTokenRestAction.java:46` | `…authtoken/_own/revoke` (`RevokeAuthTokenAction.java:22`) | perm | no |
| GET, POST | `/_searchguard/authtoken/_search` | `authtoken/api/SearchAuthTokenRestAction.java:45` | `…authtoken/_own/search` (`SearchAuthTokensAction.java:22`) | perm | no |
| GET | `/_searchguard/authtoken/_info` | `authtoken/api/AuthTokenInfoRestAction.java:39` | `…authtoken/info` (`AuthTokenInfoAction.java:22`) | perm (`SGS_KIBANA_SERVER`, `static_roles.yml:293`) | no |
| GET | `/_searchguard/config/auth_token_service` | `authtoken/AuthTokenServiceConfigApi.java:35` | `…config/auth_token_service/get` `:49` | perm | no |
| PUT | `/_searchguard/config/auth_token_service` | `AuthTokenServiceConfigApi.java:36` | `…/put` `:67` | perm | no |
| PATCH | `/_searchguard/config/auth_token_service` | `AuthTokenServiceConfigApi.java:38` | `…/patch` `:83` | perm | no |

### 1.4 `dlic-fe-multi-tenancy` (proprietary) — `enterprise/femt/FeMultiTenancyModule.java:218-220`

| Method | Path | Handler (ref) | Transport action | Who may call | Tenant |
|---|---|---|---|---|---|
| GET | `/_searchguard/config/fe_multi_tenancy`, `/_searchguard/config/frontend_multi_tenancy` | `FeMultiTenancyConfigApi.java:38-39` | `…config/fe_multi_tenancy/get` `:58` | perm | no |
| PUT | both paths | `FeMultiTenancyConfigApi.java:41-42` | `…/put` `:76` | perm | no |
| PATCH | both paths | `FeMultiTenancyConfigApi.java:43-44` | `…/patch` `:92` | perm | no |
| PUT | `/_searchguard/config/fe_multi_tenancy/activation` | `FeMultiTenancyConfigApi.java:40` | `cluster:admin:searchguard:config/multitenancy/activate` (`tenants/MultitenancyActivationAction.java:27`) | perm | no |
| GET | `/_searchguard/current_user/tenants` | `FeMultiTenancyConfigApi.java:45` | `cluster:admin:searchguard:femt:user/available_tenants/get` (`tenants/GetAvailableTenantsAction.java:21`) | perm (`SGS_KIBANA_USER*`, `static_roles.yml:42,145`) | hdr (returns `user_requested_tenant`, `tenants/AvailableTenantData.java:11-15`) |
| POST | `/_searchguard/config/fe_multi_tenancy/data_migration/8_8_0` | `datamigration880/rest/DataMigrationApi.java:33` | `…frontend_data_migration/8_8_0/start` (`StartDataMigrationAction.java:43`) | perm | no |
| GET | same | `DataMigrationApi.java:36` | `…/8_8_0/get` (`GetDataMigrationStateAction.java:40`) | perm | no |

### 1.5 `dlic-dlsfls` (proprietary) — `enterprise/dlsfls/DlsFlsModule.java:175-177`

| Method | Path | Handler (ref) | Transport action | Who may call | Tenant |
|---|---|---|---|---|---|
| GET | `/_searchguard/config/authz_dlsfls` | `DlsFlsConfigApi.java:35` | `…config/authz_dlsfls/get` `:47` | perm | no |
| PUT | same | `DlsFlsConfigApi.java:36` | `…/put` `:65` | perm | no |
| PATCH | same | `DlsFlsConfigApi.java:37` | `…/patch` `:81` | perm | no |

### 1.6 `signals` (Apache 2) — `signals/src/main/java/com/floragunn/signals/SignalsModule.java:129-146`; only when Signals is enabled `:130`

Tenant-scoped actions (`cluster:admin:searchguard:tenant:signals:…`) get `Scope.TENANT` (`security/.../authz/actions/Actions.java:423`), so the role must grant them on the tenant. Handlers extending `SignalsTenantAwareRestHandler` reject the private tenant (`signals/api/SignalsTenantAwareRestHandler.java:41-44`).

| Method | Path | Handler (ref) | Transport action | Who may call | Tenant |
|---|---|---|---|---|---|
| GET, PUT, DELETE | `/_signals/watch/{tenant}/{id}` | `api/WatchApiAction.java:42-43` | `…tenant:signals:watch/get`, `/put`, `/delete` (`actions/watch/{get,put,delete}/*Action.java:8`) | perm | path |
| POST | `/_signals/watch/{tenant}/_execute`, `/_signals/watch/{tenant}/{id}/_execute` | `api/ExecuteWatchApiAction.java:59-60` | `…tenant:signals:watch/execute` (`actions/watch/execute/ExecuteWatchAction.java:8`) | perm | path |
| PUT, DELETE | `/_signals/watch/{tenant}/{id}/_active` | `api/DeActivateWatchAction.java:29` | `…watch/activate_deactivate` (`actions/watch/activate_deactivate/DeActivateWatchAction.java:8`) | perm | path |
| PUT, DELETE | `/_signals/watch/{tenant}/{id}/_ack[/{actionId}]` | `api/AckWatchApiAction.java:47-48` | `…watch/ack` (`actions/watch/ack/AckWatchAction.java:8`, nodes action) | perm | path |
| PUT, DELETE | `/_signals/watch/{tenant}/{id}/_ack_and_get[/{actionId}]` | `api/AckAndGetWatchApiAction.java:44-45` | `…watch/ack_and_get` (`actions/watch/ackandget/AckAndGetWatchAction.java:8`, nodes action) | perm | path |
| GET, POST | `/_signals/watch/{tenant}/_search` | `api/SearchWatchApiAction.java:34` | `…watch/search` (`actions/watch/search/SearchWatchAction.java:8`) | perm | path |
| GET | `/_signals/watch/{tenant}/{id}/_state` | `api/WatchStateApiAction.java:31` | `…watch:state/get` (`actions/watch/state/get/GetWatchStateAction.java:8`) | perm | path |
| GET, POST | `/_signals/watch/{tenant}/_search/_state` | `api/SearchWatchStateApiAction.java:33-34` | `…watch:state/search` (`actions/watch/state/search/SearchWatchStateAction.java:8`) | perm | path |
| POST | `/_signals/watch/{tenant}/summary` | `actions/summary/LoadOperatorSummaryAction.java:12-15` | `cluster:admin:searchguard:signals:summary/load` `:9` | perm | path |
| GET, PUT, DELETE | `/_signals/account/{type}/{id}` (global) and `/_signals/account/{tenant}/{type}/{id}` | `api/AccountApiAction.java:52-57` | `…signals:account/get|put|delete` or `…tenant:signals:account/…` (`actions/account/*/`) | perm | path (tenant variant) |
| GET, POST | `/_signals/account/_search`, `/_signals/account/{tenantOrAccountType}/_search` | `api/SearchAccountApiAction.java:37-39` | `…signals:account/search` / `…tenant:signals:account/search` | perm | path (tenant variant) |
| PUT, DELETE | `/_signals/tenant/{tenant}/_active` | `api/DeActivateTenantAction.java:30` | `…tenant:signals:tenant/start_stop` (`actions/tenant/start_stop/StartStopTenantAction.java:8`) | perm | path |
| PUT, DELETE | `/_signals/admin/_active` | `api/DeActivateGloballyAction.java:30` | `cluster:admin:searchguard:signals:admin/start_stop` (`actions/admin/start_stop/StartStopAction.java:8`) | perm | no |
| GET | `/_signals/settings`, `/_signals/settings/{key}` | `api/SettingsApiAction.java:40` | `…signals:settings/get` (`actions/settings/get/GetSettingsAction.java:8`) | perm | no |
| PUT, DELETE | `/_signals/settings/{key}` | `api/SettingsApiAction.java:41` | `…signals:settings/put` (`actions/settings/put/PutSettingsAction.java:8`); fan-out via `…settings/update` | perm | no |
| POST | `/_signals/convert/es` | `api/ConvertWatchApiAction.java:33,40-46` | none; local `EsWatcherConverter` | authn | no |
| PUT | `/_signals/truststores/{id}` | `truststore/rest/CreateOrReplaceTruststoreAction.java:52` | `…signals:truststores/createorreplace` `:47` | perm | no |
| GET | `/_signals/truststores/{id}` | `truststore/rest/FindOneTruststoreAction.java:46` | `…truststores/findone` `:41` | perm | no |
| GET | `/_signals/truststores` | `truststore/rest/FindAllTruststoresAction.java:46` | `…truststores/findall` `:41` | perm | no |
| DELETE | `/_signals/truststores/{id}` | `truststore/rest/DeleteTruststoreAction.java:45` | `…truststores/delete` `:40` | perm | no |
| PUT, DELETE, GET `/{id}`, GET | `/_signals/proxies[/{id}]` | `proxy/rest/ProxyApi.java:46-52` | `…signals:proxies/createorreplace|delete|findone|findall` `:61,149,229,192` | perm | no |

## 2. REST entry and wrapping

There is no `getRestHandlerWrapper`/`RestInterceptor` in this code base (grep finds none). Interception happens one level lower, in the HTTP transport's `Dispatcher` chain that `SearchGuardPlugin.getHttpTransports` assembles (`SearchGuardPlugin.java:685-730`).

| Step | Class (ref) | What happens |
|---|---|---|
| 0 | `http/SearchGuardHttpServerTransport.java:57,68-72` (TLS) / `http/SearchGuardNonSslHttpServerTransport.java:44,61-64` (plain) | `incomingRequest` wraps the Netty request in `AttributedHttpRequest` (carries `SslHandler` + event loop); the TLS variant also rewrites non-ES `Content-Type` into a supported type and keeps the original in `X-SG-Original-Content-Type` (`:80-100`). Registered under the name `com.floragunn.searchguard.http.SearchGuardHttpServerTransport` either way (`SearchGuardPlugin.java:723,725`). |
| 1 | `ssl/http/netty/SSLInfoPopulatingDispatcher.java:77-80,91-100,149-178` | Rejects requests carrying `_sg_ssl_`-prefixed headers (403), extracts the client cert principal via `PrincipalExtractor` and stores `SG_SSL_PRINCIPAL`, peer/local certs, protocol and cipher as thread-context transients. Built in `SearchGuardPlugin.java:717`. |
| 2 | `authc/rest/AuthenticatingRestFilter.java:113-115` → `ExecuteInNettyEventLoopDispatcher.java:45-71` | Re-dispatches onto the request's Netty event loop if not already there (thread context preserved). |
| 3 | `AuthenticatingRestFilter.AuthenticatingRestHandler.handleRequest` `:138-213` | (a) `checkRequest`: 403 + `auditLog.logBadHeaders` on any `_sg_`-prefixed header (`:221-236,289-301`), sets origin `REST`. (b) `isAuthcRequired` (`:215-219`). (c) **Admin-cert check**: if `SG_SSL_PRINCIPAL` is in `searchguard.authcz.admin_dn`, a `User` with `AuthDomainInfo.TLS_CERT` is set, `logSucceededLogin`, and the request is dispatched without consulting any authc config (`:147-158`). (d) Otherwise 503 "SG11" if no `sg_authc` config loaded yet (`:160-165`), else `RestAuthenticationProcessor.authenticate` (`:167`). |
| 4 | `authc/rest/RestAuthenticationProcessor.java:144-181` | IP acceptance rules (`:147`), trusted-proxy / `X-Forwarded-For` resolution (`:161-165`, header name from `RestAuthcConfig.java:132`), blocked-IP registry with `logBlockedIp` (`:168-176`), then `RestRequestAuthenticationProcessor` iterates the configured HTTP authentication domains (implicit module domains first: `:99`). Blocked users (`RestRequestAuthenticationProcessor.java:100-104`), impersonation via `sg_impersonate_as` (`:190`), requested tenant from `sgtenant`/`sg_tenant` (`:184`), `WWW-Authenticate` challenges (`:162-171`). Audit: `logFailedLogin`/`logSucceededLogin` in `authc/base/RequestAuthenticationProcessor.java:225,268,275,334`. |
| 5 | `AuthenticatingRestFilter.java:173-202` | On `PASS` the original ES `RestController` dispatcher runs the handler; on failure a 401/403 with optional JSON envelope (`createResponse` `:253-266`). Debug mode short-circuits to `/_searchguard/auth/debug` (`:168-170`). |
| 6 | `filter/SearchGuardFilter.java:166-172,240-246` | **Authorization** happens only when the handler executes a transport action: `SearchGuardFilter` (ES `ActionFilter`) lets admin-cert users, `_sg_conf_request`, internal and pass-through actions through (admin grants are audited via `logGrantedPrivileges` `:243`), everything else goes to `PrivilegesEvaluator`. Handlers that answer locally (section 1, "authn") are therefore never authorized beyond authentication. |

Order for a REST call is therefore: TLS/principal extraction → bad-header check → admin-cert short-circuit → authentication (+ login audit) → ES routing → per-transport-action authorization (+ privilege audit). The legacy `/_searchguard/api/*` handlers add their own gate before doing anything: `RestApiPrivilegesEvaluator.checkAccessPermissions` plus `auditLog.logMissingPrivileges` on failure (`dlic-security/.../AbstractApiAction.java:447-456`).

`AuthenticatingRestFilter` also exposes component state `authc/rest_filter` (`:71`) and is rebuilt on every `sg_authc` config change (`:86-109`).

## 3. Request content handling

| Mechanism | Where | Notes |
|---|---|---|
| `RestApi` base (support) | `support/.../action/RestApi.java:236-246,295-307,363-375` | Reads `Content-Type` (or `X-SG-Original-Content-Type`), 400 if missing when a body is present, copies the body with `BytesReference.toBytes(restRequest.content())` into a codova `UnparsedDocument`; the parser lambda receives `(params, UnparsedDocument[, headers])`. `If-Match`/`If-None-Match` become request ETags (`:248-258`). |
| ES-native XContent | `rest/mock/MockHasPrivilegesAction.java:108-110`, `authc/session/backend/SessionApi.java:371-396` | `request.requiredContent()` + codova `DocReader` by `XContentType`. `SessionApi` does `body.mustIncRef()`/`decRef()` around the async handler. |
| Legacy admin API | `dlic-security/.../AbstractApiAction.java:118-127,437,460-461` | Body is `ReleasableBytesReference`; `request.content()` is called in `prepareRequest` to consume it, then `mustIncRef()` and handed to `handleApiRequest` — the javadoc explains that reading the content after `handleRequest` returns fails since ES's ref-counted request bodies (elastic/elasticsearch#116115). This was introduced by the ES 8.18.0 upgrade commit `abd886809`, which touched `AbstractApiAction`, `PatchableResourceApiAction`, `SessionApi`, `AccountApiAction` and `WatchApiAction`. |
| Signals handlers | `signals/api/SettingsApiAction.java:108`, `ExecuteWatchApiAction.java:74`, `ConvertWatchApiAction.java:40` (`utf8ToString()`), `WatchApiAction.java:115`, `AccountApiAction.java:141` (`ReleasableBytesReference`) | Mixed: string conversion vs ref-counted handoff. |
| Audit log / FEMT (`BytesReference` → String) | `dlic-auditlog/.../impl/Utils.java:55`, `AbstractAuditLog.java:1318`, `RequestResolver.java:184,262,308`; `dlic-fe-multi-tenancy/.../FrontendDataMigrationInterceptor.java:146,154` | Commit `5a0d3aaae` (ES 9.5.2 → 9.5.5) replaced direct `request.source()` string handling with `XContentHelper.convertToMap/convertToJson(bytes, xContentType)` and `source().utf8ToString()` because `PutMappingRequest.source()` became a `BytesReference`. This is the "BytesReference handling" change referenced in CLAUDE.md; it is on the transport side, not in REST handlers. |
| Size limits | none found | No plugin-specific body size limit (grep for `max_content_length`, `maxContentLength`, `REQUEST_ENTITY_TOO_LARGE` in `src/main` of security/ssl/support/dlic-security/signals finds nothing); ES's `http.max_content_length` applies. UNVERIFIED that nothing in the Netty pipeline customisation changes it. |

## 4. Response format conventions

| Convention | Where | Shape |
|---|---|---|
| `StandardResponse` envelope (all `RestApi`-based endpoints) | `support/.../action/StandardResponse.java:32-146,179` | `{"status": <int>, "error": {"code","message","details"}?, "message"?, "data"?}`; `ETag` header when the response carries a concurrency tag (`RestApi.java:136-138`); 405 `Method not allowed` from `RestApi.java:157`; 400 `Content-Type header is missing`; exceptions mapped by `StandardResponse(Exception)` `:65`. |
| `Action.Response` | `support/.../action/Action.java:167-215` | Every new-style transport response is a codova `Document` serialised as JSON (`RestApi.toRestResponse` `:130-147`); `?pretty` honoured. |
| Legacy `BaseRestHandler` JSON | `signals/api/SignalsBaseRestHandler.java:27-70`, `support/.../action/Responses.java:43-120` | `{"status": <int>, "error": "<text>", "detail": ...}` / `{"status": <int>}`. |
| Legacy admin API | `dlic-security/.../AbstractApiAction.java:562` and `response(...)` helpers | `{"status": "<NAME>", "message": "..."}`-style; errors as 403 `forbidden(...)`. UNVERIFIED exact field set. |
| Authc failure envelope | `authc/rest/AuthenticatingRestFilter.java:253-266` | JSON `{"status", "error": {"reason", "type": "authentication_exception"}}` only when `Accept` is JSON, else plain text; `WWW-Authenticate` headers from the frontends. |
| Version marker | `security/.../SearchGuardVersion.java:33-40` | `X-Search-Guard-Version` response header on: capabilities, internal index create, type-level delete, bulk config, migrate_index, FEMT data migration, Signals truststores and proxies (`responseHeaders(SearchGuardVersion.header())` in those classes). `GET /_searchguard/config` adds `_version`, `_seq_no`, `_primary_term`, `_etag` per config type (`BulkConfigApi.java:157-162`). Audit messages carry `audit_format_version: 4` (`dlic-auditlog/.../AuditMessage.java:55,121`). No endpoint returns an explicit API format version. |

## 5. Authentication frontends reachable over HTTP

Registry: `security/.../SearchGuardModulesRegistry.java:456-463` (community types) and `dlic-security/.../enterprise/auth/EnterpriseAuthFeaturesModule.java:45-48` (enterprise types). Implicit domains that always run first: `SessionModule.java:141` (session), `AuthTokenModule.java:154` (sg_auth_token).

| Type | Module | Class (ref) | Reads |
|---|---|---|---|
| admin TLS cert | security | `AuthenticatingRestFilter.java:147-158` | `SG_SSL_PRINCIPAL` transient vs `searchguard.authcz.admin_dn` |
| `basic` | security | `authc/rest/authenticators/BasicAuthenticationFrontend.java:62` | `Authorization: Basic …` |
| `clientcert` | security | `authc/rest/authenticators/HttpClientCertAuthenticationFrontend.java:52` | client cert subject (`SG_SSL_PRINCIPAL`) |
| `trusted_origin` (proxy) | security | `authc/rest/authenticators/HttpTrustedOriginAuthenticationFrontend.java:38-44` | connecting IP must be in `network.trusted_proxies` (`RestAuthcConfig.java:130-132`); user name comes from user-mapping config, not a fixed header |
| `anonymous` | security | `authc/rest/authenticators/AnonymousAuthenticationFrontend.java:38` | nothing; user `anonymous` |
| `session` (session token) | security | `authc/session/backend/SessionTokenAuthenticationDomain.java:93,104` | `Authorization: Bearer <session JWT>` |
| `sg_auth_token` (API token) | dlic-security | `authtoken/AuthTokenHttpJwtAuthenticator.java:44,54` | `Authorization: Bearer <SG auth token JWT>` |
| `jwt` | dlic-security | `enterprise/auth/jwt/JwtAuthenticator.java:94,181-182` | configurable header (default `Authorization: Bearer`) or URL param `jwt_url_parameter` |
| `kerberos` | dlic-security | `enterprise/auth/kerberos/KerberosAuthenticationFrontend.java:144,222-224` | `Authorization: Negotiate …`, challenge `WWW-Authenticate: Negotiate` |
| `oidc` | dlic-security | `enterprise/auth/oidc/OidcAuthenticator.java:214` | `ApiAuthenticationFrontend`: body field `sso_result` of `POST /_searchguard/auth/session` |
| `saml` | dlic-security | `enterprise/auth/saml/SamlAuthenticator.java:210-226` | `ApiAuthenticationFrontend`: body field `saml_response` of `POST /_searchguard/auth/session` |
| `link` | security | `authc/session/LinkApiAuthenticationFrontend.java:37-52` | `ApiAuthenticationFrontend` for the session API; UNVERIFIED which body field |

Also read during authc, independent of frontend: `sgtenant`/`sg_tenant` (`RestRequestAuthenticationProcessor.java:184`), `sg_impersonate_as` (`:190`), `X-Forwarded-For` or `authc.network.http.remote_ip_header` (`RestAuthcConfig.java:132`).

## 6. Kibana-facing endpoints

Cross-repo contract with `search-guard-kibana-plugin`. Which of these the Kibana plugin actually calls is UNVERIFIED from this repo; the list is what the static Kibana roles grant plus the handlers whose names/fields target Kibana.

| Endpoint | Purpose / fields | Ref |
|---|---|---|
| GET/POST `/_searchguard/authinfo` | `user_name`, `user_requested_tenant`, `sg_roles`, `sg_tenants`, `sso_logout_url`, `backend_roles`, `principal` | `rest/SearchGuardInfoAction.java:114-132` |
| GET/POST `/_searchguard/kibanainfo` | `not_fail_on_forbidden_enabled`, `kibana_mt_enabled`, `kibana_index`, `kibana_server_user`, `kibana_rbac_enabled` | `rest/KibanaInfoAction.java:78-83` |
| GET/POST `/_searchguard/auth/config` | activated frontend login config (`SGS_KIBANA_SERVER` grant) | `GetActivatedFrontendConfigAction.java:50-53`; `static_roles.yml:294` |
| POST/GET/DELETE `/_searchguard/auth/session`, GET `…/extended`, POST `…/with_header` | login, logout, session info | `SessionApi.java:346-350`; `SGS_PERSONAL_SESSIONS` `static_action_groups.yml:287-295` |
| GET `/_searchguard/current_user/tenants` | `multi_tenancy_enabled`, `tenants`, `username`, `user_requested_tenant`, `default_tenant` | `FeMultiTenancyConfigApi.java:45`; `AvailableTenantData.java:11-15` |
| GET/PUT/PATCH `/_searchguard/config/fe_multi_tenancy` (+ `frontend_multi_tenancy` alias), PUT `…/activation`, `…/data_migration/8_8_0` | MT config and 8.8 migration | `FeMultiTenancyConfigApi.java:38-44`; `DataMigrationApi.java:33-36` |
| GET `/_searchguard/license/info`, GET/POST `/_searchguard/license` | license display | `SearchGuardLicenseInfoAction.java:44`; `SearchGuardLicenseAction.java:43` |
| GET `/_searchguard/permission?permissions=` | per-permission boolean map | `rest/PermissionAction.java:59-82` |
| GET `/_searchguard/authtoken/_info`, `/_searchguard/authtoken*` | auth-token UI (`SGS_KIBANA_SERVER` grants `authtoken/info`) | `AuthTokenInfoRestAction.java:39`; `static_roles.yml:293` |
| `/_searchguard/api/*` | legacy config UI (roles, rolesmapping, internalusers, actiongroups, tenants, blocks, license, cache, permissionsinfo) | `SearchGuardRestApiActions.java:46-63` |
| `/_signals/*` | Signals UI | section 1.6 |
| GET/POST `/_security/user/_has_privileges` | X-Pack shim so Kibana core features work | `MockHasPrivilegesAction.java:48,62-63` |
| headers | `sgtenant` (tenant switch), `Authorization: Bearer` (session) | `RestRequestAuthenticationProcessor.java:184`; `SessionTokenAuthenticationDomain.java:104` |

## 7. sgctl-facing endpoints

sgctl is a separate repo (`git.floragunn.com/search-guard/sgctl`, referenced from `.gitlab/issue_templates/new_release_minor_flx.md:17-33` and `security/tools/install_demo_configuration.sh:24-25`). Which endpoints it calls is UNVERIFIED from this repo; the following are the new-style admin APIs designed for it (the 503 text on uninitialised clusters points users at sgctl: `AuthenticatingRestFilter.java:161-163`).

| Endpoint | Ref |
|---|---|
| GET/PUT `/_searchguard/config` (bulk get/update, ETag/`_etag` concurrency) | `BulkConfigApi.java:70-71,157-162,342` |
| DELETE `/_searchguard/config/{config_type}` | `GenericTypeLevelConfigApi.java:38` |
| `/_searchguard/config/{authc,authz,authz_dlsfls,sessions,authc_frontend[/{id}],auth_token_service,fe_multi_tenancy}` | sections 1.1, 1.3, 1.4, 1.5 |
| `/_searchguard/config/vars[/{id}]` | `ConfigVarApi.java:43-47` |
| `/_searchguard/internal_users/{id}` | `InternalUsersConfigApi.java:48-54` |
| GET/PUT/PATCH `/_searchguard/license/key`, GET `/_searchguard/license/info` | `SearchGuardLicenseKeyApi.java:36-38`; `SearchGuardLicenseInfoAction.java:44` |
| GET `/_searchguard/component/{id}/_health` | `ComponentStateRestAction.java:45` |
| POST `/_searchguard/config/migrate_index`, POST `/_searchguard/internal/indices/create` | `MigrateConfigIndexApi.java:39`; `ProtectedConfigIndexService.java:677` |
| PUT `/_searchguard/configupdate`, DELETE `/_searchguard/authc/cache`, `/_searchguard/api/cache` | `SearchGuardConfigUpdateAction.java:53`; `AuthcCacheApi.java:55`; `FlushCacheApiAction.java:60-61` |
| GET `/_searchguard/whoami`, `/_searchguard/sslinfo`, `/_searchguard/capabilities/cluster_wide`, `/_searchguard/health` | `SearchGuardWhoAmIAction.java:57`; `SearchGuardSSLInfoAction.java:58`; `SearchGuardCapabilities.java:324`; `SearchGuardHealthAction.java:50` |
| POST `_searchguard/api/ssl/{certType}/reloadcerts/` | `SSLReloadCertAction.java:44` |

All of these are callable with the admin certificate (sgctl's normal credential) because of the filter bypass in `SearchGuardFilter.java:240-246`; the responses with `X-Search-Guard-Version` (section 4) are the ones sgctl can version-negotiate on. UNVERIFIED that sgctl reads that header.

## Mixed-cluster and compatibility relevance

**External contracts (shape cannot change without coordination):**

- Kibana plugin: everything in section 6, in particular the field names of `/_searchguard/authinfo`, `/_searchguard/kibanainfo`, `/_searchguard/current_user/tenants`, the session API body/response (`SessionApi.java:323-337`), the `sgtenant` header, and the `_has_privileges` mock.
- sgctl: section 7, especially `GET/PUT /_searchguard/config` document shape (`content`, `exists`, `_version`, `_seq_no`, `_primary_term`, `_etag`: `BulkConfigApi.java:144-169`), `StandardResponse` envelope, `X-Search-Guard-Version`, 503 "SG11" on uninitialised cluster.
- Users' scripts / older tooling: legacy `/_searchguard/api/*` (sgadmin-era, `dlic-security`), `/_searchguard/license` (unauthenticated), `/_searchguard/configupdate`, `/_searchguard/health?mode=`, `/_searchguard/authinfo`.
- Signals UI and automation: all `/_signals/*` paths and their `{"status","error"}` JSON (`SignalsBaseRestHandler.java:27-70`).

**Endpoints that fan out to other nodes** (transport request extends `BaseNodesRequest` / handler extends `TransportNodesAction`): `/_searchguard/configupdate` and every legacy `/_searchguard/api/*` write (`ConfigUpdateRequest`), `/_searchguard/component/{id}/_health` (`GetComponentStateAction`), `/_searchguard/license` legacy (`LicenseInfoRequest`), `/_searchguard/whoami`'s `WhoAmIRequest` (REST handler itself answers locally), `/_searchguard/capabilities/cluster_wide`, `/_searchguard/internal/indices/create`, `/_searchguard/authc/cache`, config-var refresh behind `/_searchguard/config/vars` (`ConfigVarRefreshAction`), session/auth-token pushes behind `POST/DELETE /_searchguard/auth/session` and `/_searchguard/authtoken` (`PushSessionTokenUpdateAction`, `PushAuthTokenUpdateRequest`), Signals `_ack`/`_ack_and_get`, `/_signals/settings` (`SettingsUpdateRequest`), `/_signals/truststores`, `/_signals/proxies` and account writes (`DestinationConfigUpdateRequest`) — see the class list from `grep "extends TransportNodesAction|extends BaseNodesRequest"` over `src/main`. During a rolling upgrade these are the paths where a newer node's request/response serialisation meets an older node; the REST shape is unaffected but a `StreamInput` mismatch surfaces as a 500 on the coordinating node. UNVERIFIED whether any of them carry a transport-version guard.

`/_searchguard/config/{config_type}` DELETE and `/_searchguard/config/migrate_index` write the config index and rely on `ConfigurationRepository` change propagation rather than a nodes action (UNVERIFIED from this pass).

## Open questions for human review

1. The legacy `/_searchguard/api/*` tier (`dlic-security/.../dlic/rest/api/`) and the new `RestApi`-based config tier overlap (`internalusers` vs `/_searchguard/internal_users`, `license` vs `/_searchguard/license/key`). Which one is the supported contract for sgctl and the Kibana config UI, and is the legacy tier slated for removal on `main` (ES 9)?
   - **Answer:** _pending_
2. `MockHasPrivilegesAction` registers `/_security/user/_has_privileges` even in `ssl_only` mode (`SearchGuardPlugin.java:448` is outside the `!sslOnly` block) and always answers true. Intended?
   - **Answer:** _pending_
3. `GET /_searchguard/auth/debug` is reachable only through the authc-debug short-circuit; its registered handler is a 404 stub (`AuthenticatingRestFilter.java:273-287`). Confirm this is deliberate and documented.
   - **Answer:** _pending_
4. `/_searchguard/license` (legacy) and `/_searchguard/health` are unauthenticated by path string match (`AuthenticatingRestFilter.java:216-218`; the `license` check is duplicated). Confirm the information disclosed (license type/expiry, config-initialised state) is acceptable without auth.
   - **Answer:** _pending_
5. `GET /_searchguard/auth/session` (`SessionApi.java:353-368`) and the three "authn" Signals/info handlers execute without any permission check. Confirm this matches the privilege model expected by the Kibana plugin.
   - **Answer:** _pending_
6. No plugin-side request body size limit exists; is reliance on `http.max_content_length` intended for the bulk config PUT (`BulkConfigApi.java:71`) and the Signals execute endpoint?
   - **Answer:** _pending_
7. Which endpoints does the current sgctl version actually call, and does it read `X-Search-Guard-Version`? This repo cannot answer that; the answer determines which rows of section 7 are frozen contracts.
   - **Answer:** _pending_
8. `SessionApi.POST` and the legacy `AbstractApiAction` manually `mustIncRef()` the body since ES 8.18; `RestApi` (support) copies bytes synchronously instead. Confirm no `RestApi` endpoint defers reading `restRequest.content()` past `prepareRequest` after future ES upgrades.
   - **Answer:** _pending_
