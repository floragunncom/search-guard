# Persisted data and schemas

*Inventory pass, generated 2026-10-19 on branch experimental-analysis at commit 374449897. Every claim carries a file reference. Items marked UNVERIFIED were inferred, not read.*

## Summary for maintainers

*Written for human readers. The tables below are the authoritative record; this section is a guide to them and must be re-read whenever a `CType`, a parser, an index mapping or `ConfigurationRepository` changes.*

**What this covers.** Everything the plugin stores: the indices it creates, the configuration documents and their field shapes, how configuration is loaded and pushed to other nodes, what it reads from and writes to disk, how secrets are protected, and which migrations exist.

**What you need to know.**

- Configuration documents carry no version marker. When a node loads a document with a field it does not know, it logs an error and keeps using the document. When a node tries to write such a document through the API, the write is rejected. So an old node keeps running with a newer configuration but cannot save that configuration type until the unknown field is gone, and any write that does succeed replaces the whole document.
- Three exceptions to that rule. A new key inside a role's permission block makes the old node throw while parsing that role. Internal users keep unknown fields untouched. Signals watches are strict, so a watch containing a newer attribute silently stops running on older nodes.
- Two configuration types, sessions and DLS/FLS, share the same numeric identifier, and that identifier is sent between nodes inside auth-token snapshots. Confirmed defect candidate.
- Encrypted configuration variables fall back to a key compiled into the plugin unless an active key is configured, and the file-based key option does not work because it reads the wrong setting. Signals stores its own signing keys and account passwords in clear text.
- The plugin does not use Elasticsearch system indices. Protection of its indices is its own filter plus the hidden flag.
- Only one migration runs automatically, the Signals watch mapping version. Renaming the config index and the multi-tenancy 8.8 data migration are manual operations.

**What to review.**

- Section 2, the duplicate identifier (open question 1) and the encryption defaults (open question 2). Both need a decision and an issue.
- Section 3, the unknown-field column per document, especially the behaviour of a role with an unknown nested key (open question 8) and whether sessions and auth tokens really ignore unknown fields (open question 9).
- The "Mixed-cluster and compatibility relevance" table, whether it matches what operators are told to expect during a rolling upgrade.
- Section 6, the clear-text secrets.

## Scope and method

Covers everything the plugin writes to or reads from Elasticsearch indices and the node file system, across all
Maven modules except `sgi8/`, `audit/` and `target/`. Method: grep for index-name constants and settings,
`CreateIndexRequest`/`ProtectedConfigIndexService.createIndex` call sites, `new CType<`, `IndexRequest` writers and
`parse(DocNode…)` parsers, then reading each hit. Test sources were ignored. Codova semantics (`ValidationResult`,
`checkForUnusedAttributes`) were checked against the `codova-1.10.1.jar` bytecode and the `1.4.0` sources jar in
`~/.m2`, since codova is an external dependency (`pom.xml:83-84`).

## 1. Indices

Nothing in the plugin registers an ES `SystemIndexDescriptor` (grep over all main sources: no hit). "System-ness" is
implemented by Search Guard itself: `SearchGuardPlugin.ProtectedIndices` (`security/src/main/java/com/floragunn/searchguard/SearchGuardPlugin.java:1212-1250`),
seeded with the two config index names (`:1222`) and extended by every `ProtectedConfigIndexService.createIndex` call
(`security/src/main/java/com/floragunn/searchguard/configuration/ProtectedConfigIndexService.java:127`). All indices
created through that service get `number_of_shards=1, auto_expand_replicas=0-all, index.hidden=true`
(`ProtectedConfigIndexService.java:107-108`); existing non-hidden ones are made hidden on the fly (`:228-250`).
Creation happens only on the elected master (`:318-321`); other nodes queue and retry every 30 s (`:201-206`).

| Index (default) | Setting key | Module | Created at | Mapping source | Version / migration logic |
|---|---|---|---|---|---|
| `.searchguard` (new) / `searchguard` (legacy) | `searchguard.config_repository.index_name` / deprecated `searchguard.config_index_name` (`ConfigurationRepository.java:124-133`) | security | `ConfigurationRepository.createConfigIndex` `security/src/main/java/com/floragunn/searchguard/configuration/ConfigurationRepository.java:605-622` (1 shard, `0-all`, hidden only if name starts with `.`); also lazily on first write `:723-732` | `SG_INDEX_MAPPING`: one dynamic template mapping every field to `binary` (`:172-173`) | No per-doc version. Legacy→new index copy via `POST /_searchguard/config/migrate_index` (`MigrateConfigIndexApi.java:39-40` → `ConfigurationRepository.migrateIndex` `:634-717`). Loader prefers new name, falls back to legacy (`:276-290`, `:624-632`) |
| `.searchguard_config_vars` | none (hard-coded `ConfigVarService.java:92`) | security | `security/src/main/java/com/floragunn/searchguard/configuration/variables/ConfigVarService.java:104-105` | `ConfigVar.INDEX_MAPPING` `ConfigVar.java:40-44` (`value` disabled, `scope`/`updated` keyword, `encrypted.{value binary,key,iv}`), mapping version 1 (`ProtectedConfigIndexService.java:602-606`) | none |
| `.searchguard_sessions` | `searchguard.sessions.index.name` (`SessionService.java:130`) | security | `security/src/main/java/com/floragunn/searchguard/authc/session/backend/SessionService.java:200-202` | `SessionToken.INDEX_MAPPING` `SessionToken.java:42-43` (`dynamic:true`; `created_at`,`expires_at`,`dynamic_expires_at` date) | none |
| `.searchguard_authtokens` | `searchguard.authtokens.index.name` (`AuthTokenService.java:126-127`) | dlic-security | `dlic-security/src/main/java/com/floragunn/searchguard/authtoken/AuthTokenService.java:173-179` | `AuthToken.INDEX_MAPPING` `AuthToken.java:38-39` (same shape as sessions) | none |
| `.searchguard_config_history` | `searchguard.config_history.index.name` (`ConfigHistoryService.java:68-69`) | dlic-security | `dlic-security/src/main/java/com/floragunn/searchguard/sgconf/history/ConfigHistoryService.java:104-107` | none (dynamic) | doc id `<ctype>_<docVersion>` (`ConfigVersion.java:90-91`); source `{"config": <uninterpolated JSON bytes>}` (`ConfigHistoryService.java:408-410`) |
| `.searchguard_resource_owner` | none (hard-coded `ResourceOwnerService.java:79`) | security | `security/src/main/java/com/floragunn/searchguard/privileges/extended_action_handling/ResourceOwnerService.java:103-104` | none; docs `{user_name, expires}` (`:121`) | none |
| `.signals_watches` | `signals.index_names.watches` (`SignalsSettings.java:545`) | signals | `signals/src/main/java/com/floragunn/signals/Signals.java:183-184` | `Watch.getIndexMapping()` `signals/src/main/java/com/floragunn/signals/watch/Watch.java:524-535` (`dynamic:true`, `_tenant` text/keyword analyzer, `checks` + `actions.checks` from `Check.getIndexMapping`) | **mapping version 2** with `_meta.version`; `mappingUpdate(0, Watch.getIndexMappingUpdate())` (`Watch.java:537-545` → `Check.java:85-94`, StaticInput properties). Applied by `ProtectedConfigIndexService.java:257-310` when `_meta.version` (`:381-404`) is below 2 |
| `.signals_watches_state` | `signals.index_names.watches_state` (`:546`) | signals | `Signals.java:189-190` | `WatchState.getIndexMapping()` `signals/src/main/java/com/floragunn/signals/watch/state/WatchState.java:198-212` (`last_execution.data` object `enabled:false`) | none |
| `.signals_watches_trigger_state` | `signals.index_names.watches_trigger_state` (`:547-548`) | signals / scheduler | `Signals.java:191` | none (dynamic) | Quartz trigger state written by `scheduler/src/main/java/com/floragunn/searchsupport/jobs/core/IndexJobStateStore.java:1110` (id = trigger key; fields `state`,`info`,`node` `:2400-2405`) |
| `.signals_accounts` | `signals.index_names.accounts` (`:549`) | signals | `Signals.java:192` | `Account.getIndexMapping()` `signals/src/main/java/com/floragunn/signals/accounts/Account.java:110-116` (`dynamic:true`, `_tenant`) | none |
| `.signals_settings` | `signals.index_names.settings` (`:550`) | signals | `Signals.java:193` | none; one doc per setting key, source `{"value": "<json string>"}` (`SignalsSettings.java:328-335`, read back `:338-365`) | none; change propagated with `SettingsUpdateAction` `cluster:admin:searchguard:signals:settings/update` (`signals/.../actions/settings/update/SettingsUpdateAction.java:13`) |
| `.signals_truststores` | constant (`SignalsSettings.java:543`) | signals | `Signals.java:185-186` | `TruststoreData.MAPPINGS` `signals/.../truststore/service/persistence/TruststoreData.java:43-55` | none |
| `.signals_proxies` | constant (`:544`) | signals | `Signals.java:187-188` | `ProxyData.MAPPINGS` `signals/.../proxy/service/persistence/ProxyData.java:33-37` | none |
| `<.signals_log_{now/d}>` | `signals.index_names.log` (`:551`), overridable per cluster via dynamic `watch_log.index` (`:98`) | signals | not created directly; composable template `signals_log_template` put by master (`Signals.java:340-375`) with `index.hidden` if name starts with `.` and `mapping.total_fields.limit` from `signals.watch_log.mapping_total_fields_limit` (`:539`) | template only; `data` can be made non-searchable (`:384-389`) | none |
| `sg7-auditlog-YYYY.MM.dd` | `searchguard.audit.config.index` (prefix `ConfigConstants.java:103` + `:110`; default and Joda date pattern `dlic-auditlog/.../sink/InternalESSink.java:47-53`) | dlic-auditlog | auto-created by ES on first index request (`InternalESSink.java:74`) — no mapping, no template | none | none. `ExternalESSink.java:58` uses the same key for a remote cluster |
| `.kibana` (+ `.kibana_analytics`, `.kibana_ingest`, `.kibana_security_solution`, `.kibana_alerting_cases`) | `frontend_multi_tenancy.index` default `.kibana` (`FeMultiTenancyConfig.java:41,68`); alias list `dlic-fe-multi-tenancy/.../tenants/TenantRepository.java:52-54` | dlic-fe-multi-tenancy | created by Kibana, not by the plugin | plugin adds `sg_tenant: keyword` to the mapping (`TenantRepository.java:121-139`, `MultitenancyActivationService.java:65-67`, `FrontendDataMigrationInterceptor.java:96-101`) | 8.8.0 data migration (section 7). Pre-8.8 per-tenant indices `.kibana_<hash>_<tenant>_<ver>` are still recognised by the regex at `MultiTenancyAuthorizationFilter.java:104-107` |
| `.sg_data_migration_state` | constant `IndexMigrationStateRepository.java:56` | dlic-fe-multi-tenancy | `dlic-fe-multi-tenancy/.../datamigration880/service/persistence/IndexMigrationStateRepository.java:125-133` | `MigrationExecutionSummary.MAPPING` `MigrationExecutionSummary.java:46-50` + `StepExecutionSummary.MAPPING` `:46-52` | the migration's own state, optimistic lock via seqNo/primaryTerm (`:67-68`) |
| `backup_fe_migration_to_8_8_0_<date>` and `<index>_reindex_temp` | prefix `DataMigrationContext.java:30`; temp postfix `MultiTenancyAuthorizationFilter.java:66-67` | dlic-fe-multi-tenancy | migration steps `StepRepository.java:169-185` | cloned from the global tenant index (`IndexSettingsManager.java:50-75`) | — |

Static, non-persisted config merged at load time: `security/src/main/resources/static_config/static_{roles,action_groups,tenants}.yml`
read by `StaticSgConfig.java:78-86`; entries are stripped again before every write (`SgDynamicConfiguration.withoutStatic`
`:198-208`, used in `ConfigurationRepository.java:1135`).

## 2. Config documents (CType registry)

All CTypes live as one document each in the config index: `_id` = CType name, source = `{"<name>": <base64 binary of the JSON>}`
(write `ConfigurationRepository.java:1133-1136` and `:1374-1378`; read `ConfigurationLoader.java:256-271`, which asserts the single
field name equals the `_id` and decodes `parser.binaryValue()`). The `ord` is used on the wire by `ConfigVersion`
(`dlic-security/.../sgconf/history/ConfigVersion.java:48,134`), hence the "do not change" warning at `CType.java:44-50`.

| name | ord | model / parser | arity | storage | `_sg*`/version field | legacy conversion | consumers (main) |
|---|---|---|---|---|---|---|---|
| `internalusers` | 0 | `InternalUser` / `InternalUser::parse` (`CType.java:58-59`) | MULTI | required | none; `${env.}`/`${envbc.}`/`${envbase64.}` substituted on load (`replaceLegacyEnvVars`, `SgDynamicConfiguration.java:92-96,380-451`) | none | `InternalUsersDatabase` (authc) |
| `actiongroups` | 1 | `ActionGroup` (`:60-61`) | MULTI | required | env vars | none | `Actions`/`PrivilegesEvaluator` |
| `roles` | 3 | `Role` (`:62`) | MULTI | required | env vars | `Role.Index.legacyIndexPatterns` kept for `LegacyRoleBasedDocumentAuthorization` (`Role.java:172-186`) | `RoleBasedActionAuthorization`, DLS/FLS |
| `rolesmapping` | 4 | `RoleMapping` (`:63-64`) | MULTI | required | env vars | none | `RoleMapper` |
| `tenants` | 5 | `Tenant` (`:65`) | MULTI | required | env vars | none | `TenantManager`, FEMT |
| `blocks` | 6 | `Blocks` (`:66`) | MULTI | optional | none | none | `BlockedIpRegistry`/`BlockedUserRegistry` |
| `config_vars` | 7 | `ConfigVar`, parser `null` (`:68-69`) | MULTI | EXTERNAL — not in the config index, lives in `.searchguard_config_vars` and is excluded from `CType.all()` (`:120-122`) | `updated` timestamp per var | none | `ConfigVarService`, `${var:…}` resolver (`ConfigurationRepository.java:211`) |
| `frontend_authc` | 8 | `FrontendAuthcConfig` (`:71-72`) | MULTI (keyed by config id, `default`) | optional | none | none | `SessionService` (`:316-318`) |
| `authc` | 10 | `RestAuthcConfig` (`:74-75`) | SINGLE (stored under key `default`, `ConfigurationRepository.java:1119-1124`) | optional | none | `Network.parseLegacy`-style attributes `internalProxies`/`remoteIpHeader` (`RestAuthcConfig.java:143-150`, deprecated) | `RestAuthenticationProcessor` |
| `authz` | 20 | `AuthorizationConfig` (`:77-78`) | SINGLE | optional | none | deprecated legacy accessor `AuthorizationConfig.java:171` | `PrivilegesEvaluator`, field masking salt |
| `license_key` | 30 | `SearchGuardLicenseKey` (`:80-81`) | SINGLE | optional | none; accepts bare string or `{key: …}` (`SearchGuardLicenseKey.java:83-94`) | none | `LicenseRepository` |
| `frontend_multi_tenancy` | 10001 | `FeMultiTenancyConfig` (`dlic-fe-multi-tenancy/.../FeMultiTenancyConfig.java:37-38`) | SINGLE | optional | none | `parseLegacySgConfig` reads `dynamic.kibana.*` (`:77-92`) — **no caller in main sources** (grep), UNVERIFIED whether tests only | `FeMultiTenancyModule`, `MultiTenancyAuthorizationFilter` |
| `sessions` | **10011** | `SessionServiceConfig` (`security/.../session/backend/SessionServiceConfig.java:47-48`) | SINGLE | optional | none | none | `SessionService` |
| `authz_dlsfls` | **10011** (collides with `sessions`; `ordToInstanceMap.put` last-wins, `CType.java:118`) | `DlsFlsConfig` (`dlic-dlsfls/.../DlsFlsConfig.java:36`) | SINGLE | optional | none; `use_impl` accepted and ignored (`:75`) | none | `DlsFlsProcessedConfig` |
| `auth_token_service` | 10021 | `AuthTokenServiceConfig` (`dlic-security/.../authtoken/AuthTokenServiceConfig.java:51-52`) | SINGLE | optional | none | `exclude_index_permissions` rejected / logged in lenient mode (`:165-172`) | `AuthTokenService` |

Skipped keys: any top-level entry whose id starts with `_sg` is ignored when parsing (`SgDynamicConfiguration.java:124-126`)
— this is how pre-FLX `_sg_meta` headers are tolerated. There is **no** v6→v7 or format-version conversion code in main
sources; `sg_config.yml` is only checked for existence during default init and is never uploaded (`ConfigurationRepository.java:561-574`).
Dotted keys at depth ≥1 are expanded into nested objects on read (`:98`) and on SINGLE-arity write (`:1121`).

## 3. Document schemas

Parsers are codova `ValidatingDocNode`s. `checkForUnusedAttributes()` records an `UnsupportedAttribute` validation error
for each unconsumed key (codova `ValidatingDocNode.java:269-274`, 1.4.0 sources; 1.10.1 assumed identical — UNVERIFIED).
Parsers then return `new ValidationResult<>(object, errors)`, whose constructor sets `hasResult=true` regardless of errors
(verified by `javap` on `codova-1.10.1.jar`). Consequence, see section 4: on **load** unknown keys are logged and the entry is
still used; on **write via API** they are rejected.

| Document | Fields (required in bold) | Unknown keys | Ref |
|---|---|---|---|
| roles entry | `reserved`,`hidden`,`static` (bool, default false), `description`, `cluster_permissions[]`, `exclude_cluster_permissions[]`, `index_permissions[]`, `alias_permissions[]`, `data_stream_permissions[]`, `tenant_permissions[]`, `exclude_index_permissions[]` (error, or log-only in lenient mode) | top-level: error, entry kept; nested `index_permissions[*]` (`index_patterns`/`alias_patterns`/`data_stream_patterns`, `allowed_actions`, `dls`, `fls[]`, `masked_fields[]`) and `tenant_permissions[*]` (`tenant_patterns`, `allowed_actions`): **throws** → that element fails (`Role.java:194-195`, `:488`); effect on the whole role UNVERIFIED. Round-trip preserves original source (`toBasicObject` returns `source`, `:150-152`) | `security/.../authz/config/Role.java:59-100,176-196,483-488` |
| rolesmapping entry | `reserved`,`hidden`,`backend_roles`,`hosts`,`users` (pattern lists), `and_backend_roles[]`, `ips`, `description` | error | `RoleMapping.java:59-74` |
| actiongroups entry | **`type`**, **`allowed_actions[]`**, `reserved`,`hidden`,`static`,`description` | error | `ActionGroup.java:48-60` |
| internalusers entry | `password` (hashed on parse, removed) or `hash`, `description`, `reserved`,`hidden`, `backend_roles[]`, `search_guard_roles[]`, `attributes{}` | **no check**; the raw document is kept and written back verbatim with `password`→`hash` (`:143-150`, `:75-77`) → unknown fields preserved | `security/.../internal_users_db/InternalUser.java:117-157` |
| tenants entry | `reserved`,`hidden`,`static`,`description` | error | `Tenant.java:33-43` |
| blocks entry | **`value[]`**, `type` (enum), `verdict` (enum), `description` | error | `Blocks.java:35-44` |
| authc (`default`) | `auth_domains[]`, `network{trusted_proxies,trusted_proxies_regex,http.remote_ip_header,accept,deny}`, `debug`, `user_cache{}`, `metrics` | error (also inside `network`) | `RestAuthcConfig.java:77-97,126-136` |
| authz (`default`) | `ignore_unauthorized_indices.{enabled,affected_actions,empty_result_allowed_for_actions}`, `field_anonymization.salt`, `role_mapping.resolution_mode`, `debug`, `metrics` | error | `AuthorizationConfig.java:81-101` |
| frontend_authc entry | `auth_domains[]{type,label,enabled,message,capture_url_fragment,auto_select,enable_by_host,…}`, `login_page{brand_image,show_brand_image,title,button_style}`, `debug`, `metrics` | error | `FrontendAuthcConfig.java:80-91,147-153,241-253` |
| license_key | string, or `{key: <PGP clear-signed base64>}` | error | `SearchGuardLicenseKey.java:79-100` |
| frontend_multi_tenancy (`default`) | `enabled` (false), `index` (`.kibana`), `server_user` (`kibanaserver`), `global_tenant_enabled` (true), `preferred_tenants[]` | error | `FeMultiTenancyConfig.java:63-74` |
| sessions (`default`) | `enabled`, `metrics`, `jwt_signing_key`/`jwt_signing_key_hs512` (else config var), `jwt_encryption_key`, `jwt_audience`, `max_validity`, `inactivity_timeout`, `max_sessions_per_user`, `required_login_privileges[]`, `refresh_session_activity_index` | UNVERIFIED (no `checkForUnusedAttributes` in the grep of `:121-170`) | `SessionServiceConfig.java:121-170` |
| auth_token_service (`default`) | `enabled`, `jwt_signing_key`/`jwt_signing_key_hs512`, `jwt_encryption_key`/`jwt_encryption_key_a256kw`, `token_cache{}`, `jwt_aud_claim`, `max_validity`, `exclude_cluster_permissions[]`, `max_tokens_per_user`, `freeze_privileges` | UNVERIFIED (no check seen `:118-180`) | `AuthTokenServiceConfig.java:118-180` |
| authz_dlsfls (`default`) | `field_anonymization{salt,personalisation,prefix}`, `debug`, `metrics`, `use_impl` (ignored), `dls.allow_now`, `dls.mode` | error | `DlsFlsConfig.java:61-80,137-154` |
| config var (`.searchguard_config_vars`, `_id`=name) | `value` (JSON string in index) **or** `encrypted{value,key,iv}`, `scope`, `updated` | ignored (plain `DocNode` getters) | `ConfigVar.java:65-99` |
| Signals watch (`_id`=`<tenant>/<id>`, `SignalsTenant.java:153,334-336`) | `trigger`, `checks[]`/`inputs[]`, `severity`, `actions[]`, `resolve_actions[]`, `throttle_period`, `active`, `log_runtime_data`, `description`, `_meta{auth_token,last_edit{user,date}}`, `_ui`, `_tenant` (added on write `:374`) | error (`Watch.java:513`) | `Watch.java:405-513,270-330` |
| Signals watch state (`_id`=`<tenant>/<id>`, `WatchStateIndexWriter.java:95`) | `_tenant`, `last_execution`, `actions`, `last_status`, `active`, `node` | ignored (plain getters) | `WatchState.java:128-195` |
| Signals account (`_id`=`<type>/<id>` UNVERIFIED) | `type`, `_name`, `_tenant`, then type-specific; e-mail: `host`,`port`,`user`,`password`,`proxy_*`,`session_timeout`,`enable_tls`,`trust_all`,`trusted_hosts`,`default_from/to/cc/bcc` | per account factory | `Account.java:122-135`, `EmailAccount.java:232-301` |
| Signals watch log | `tenant`,`watch_id`,`watch_version`,`status`,`error`,`execution_start`,`execution_end`,`data`,`runtime_attributes`,`node` | n/a (write-only) | `watch/result/WatchLog.java:128-167` |
| auth token (`_id` random) | **`user_name`**, `token_name`, **`requested{}`**, **`base{config_versions,…}`**, `created_at`, `expires_at`, `revoked_at`, `dynamic_expires_at` (epoch millis) | ignored (only known keys read) | `AuthToken.java:85-97,138-165`, `AuthTokenPrivilegeBase.java:53-109` |
| session (`_id` random) | **`user_name`**, `token_name`, **`base`**, `created_at`, `expires_at`, `revoked_at`, `dynamic_expires_at` | ignored | `SessionToken.java:87-102,137-153` |
| audit record | `audit_format_version` (=4, `AuditMessage.java:121`), `audit_category`, `audit_request_effective_user[_auth_domain]`, `audit_request_initiating_user[_auth_domain]`, `@timestamp`, `audit_cluster_name`, `audit_node_*`, `audit_request_origin`, `audit_request_remote_address`, `audit_rest_request_{path,params,headers}`, `audit_transport_{request_type,action,headers}`, `audit_trace_{doc_id,indices,index_templates,shard_id,resolved_indices,task_id,task_parent_id}`, `audit_request_{exception_stacktrace,privilege,body,layer}`, `audit_request_effective_user_is_admin`, `audit_compliance_{diff_is_noop,diff_content,file_infos,operation,doc_version,index_template_version}`, `audit_custom_*` | n/a (write-only, dynamic mapping) | `dlic-auditlog/.../impl/AuditMessage.java:55-111` |
| FEMT saved object | Kibana's own document plus `sg_tenant` keyword (`RequestResponseTenantData.java:35`); `_id` suffixed `__sg_ten__<internalTenant>` for non-global tenants (`:37,44-46`); internal tenant name = `<String.hashCode>_<[a-z0-9]-only>` (`TenantManager.java:84-90`) | n/a | `dlic-fe-multi-tenancy/.../RequestResponseTenantData.java:30-60` |
| migration state (`_id`=`MigrationStepsExecutor.MIGRATION_ID`) | `start_time`,`status`,`temp_index_name`,`backup_index_name`,`stages[]{number,start_time,name,status,message,details}` | ignored | `MigrationExecutionSummary.java:43-76`, `StepExecutionSummary.java:39-52` |

## 4. Config loading and hot reload

| Step | What happens | Ref |
|---|---|---|
| Node start | `initOnNodeStart` waits for `STATE_NOT_RECOVERED_BLOCK` to clear, then `checkIndicesNow` | `ConfigurationRepository.java:233-274` |
| Index choice | new index name → load; else legacy name → load; else if `searchguard.allow_default_init_sgindex` → create index + upload YAML from `<plugins>/search-guard-flx/sgconfig/` (or `-Dsg.default_init.dir`); else if `searchguard.background_init_if_sgindex_not_exist` (default true) poll; else SUSPENDED until an update request arrives | `:276-320`, `:553-603` |
| Readiness | wait for yellow, then until doc count ≥ number of **required** CTypes (max 30×0.5 s), then until `ConfigVarService` is initialised, then `doReload` retried every 3 s until success | `:415-551` |
| Read | one `MultiGetRequest` (`refresh=true, realtime=true`) for all non-EXTERNAL CTypes; each hit → `SgDynamicConfiguration.fromJson`; static config merged in | `ConfigurationLoader.java:137-239` |
| Missing doc | optional type → empty config in state `SUSPENDED/config_does_not_exist`; required type → load fails with `ConfigUnavailableException` | `ConfigurationLoader.java:241-250`, `:214-221` |
| Parse errors | per-entry errors are collected; entries with a result are still kept; config state `PARTIALLY_INITIALIZED/has_errors`, errors logged at ERROR | `SgDynamicConfiguration.java:120-145,168-171` |
| Hard failure | exception inside a parser or a required type absent → that reload fails; `currentConfig` is left untouched (old config stays active); at startup the loop retries forever | `ConfigurationRepository.java:734-777`, `:496-517` |
| Write | API → `update(Map<CType,ConfigWithMetadata>)`: parse with `withoutLenientValidation()`, reject on any validation error, bulk index with `IMMEDIATE` refresh, optional `if_seq_no/if_primary_term` from the ETag `primaryTerm.seqNo` | `:1083-1180`, ETag `SgDynamicConfiguration.java:177-179` |
| Propagation | after the bulk, `ConfigUpdateAction` (`cluster:admin/searchguard/config/update`) is broadcast to all nodes with the list of CType names; each node calls `reloadConfiguration` on its single `ReloadThread` | `:1218-1232`, `action/configupdate/ConfigUpdateAction.java:25`, `TransportConfigUpdateAction.java:115-116`, `ReloadThread` `:1455-1650` |
| Unknown CType name in request | `CType.fromStringValues` silently drops names it does not know | `CType.java:163-165` |
| Config vars | `ConfigVarRefreshAction` (`cluster:admin:searchguard:config_vars/refresh`) → every node re-reads `.searchguard_config_vars` and triggers a full config reload | `variables/ConfigVarRefreshAction.java:51`, `ConfigurationRepository.java:221-228` |
| Sessions / auth tokens | `PushSessionTokenUpdateAction` (`cluster:admin/searchguard/session_token/update/push`), `PushAuthTokenUpdateAction` (`cluster:admin/searchguard/auth_token/update/push`) | `session/backend/PushSessionTokenUpdateAction.java:47`, `authtoken/update/PushAuthTokenUpdateAction.java:23` |
| Signals settings | `SettingsUpdateAction` after each `.signals_settings` write | `SignalsSettings.java:300-325` |
| Lenient mode | load-time `Context` has `lenientValidationEnabled=true`; a few parsers downgrade "no longer supported" attributes to log lines in that mode | `ConfigurationRepository.java:1291-1300,1349-1353`; `Role.java:82-90`; `AuthTokenServiceConfig.java:165-172`; `FrontendAuthcConfig.java:246-249` |

Node reading a document from a newer version: unknown top-level attribute → error logged, entry used (sections 3 and 8);
unknown value for an enum/required structure → that entry is dropped (`ValidationResult` without result) or, for required
types, the reload fails and the previous in-memory config stays. The `ETag` scheme means an older node's write without
`If-Match` simply overwrites the newer document (`:1139-1163` only checks when an ETag is supplied).

## 5. On-disk artifacts

| Artifact | Setting / location | Read or written by | Ref |
|---|---|---|---|
| TLS keystores / truststores / PEM / CRL | `searchguard.ssl.{http,transport}.{keystore_filepath,truststore_filepath,pemkey_filepath,pemcert_filepath,pemtrustedcas_filepath}`, `searchguard.ssl.http.crl.file_path`; relative paths resolve against the ES config dir | read; `DefaultSearchGuardKeyStore`, `PemKeyReader`, `SSLCertificateHelper` | `ssl/.../util/SSLConfigConstants.java:36-65,73`; `DefaultSearchGuardKeyStore.java:222-243,298,319`; `PemKeyReader.java:245`; `SSLCertificateHelper.java:218-246` |
| Default config YAMLs | `<plugins>/search-guard-flx/sgconfig/sg_*.yml` or `-Dsg.default_init.dir`; files: `sg_authc.yml` (required to trigger), `sg_roles.yml`, `sg_roles_mapping.yml`, `sg_internal_users.yml`, `sg_action_groups.yml`, `sg_tenants.yml`, `sg_frontend_authc.yml`, optional `sg_blocks.yml`, `sg_authz.yml`, `sg_license_key.yml`. `sg_frontend_multi_tenancy.yml` ships in `security/sgconfig/` but is **not** uploaded | read once at default init | `ConfigurationRepository.java:553-603`; `security/sgconfig/` |
| `${file:…}` / `${json_file:…}` config variables | relative to ES config dir | read on every config parse | `ConfigurationRepository.java:209-212` |
| Config-var encryption key file | `searchguard.config_vars.encryption_keys.<id>.key_file` (bug: presence of `key_file` is checked but `file` is read, `EncryptionKeys.java:92-94`) | read at startup | `variables/EncryptionKeys.java:55,80-98` |
| Kerberos keytab | `acceptor_keytab` in the Kerberos auth domain | read | `dlic-security/.../kerberos/KerberosAuthenticationFrontend.java:98-99` |
| Audit webhook CA | `searchguard.audit.config.webhook.ssl.pemtrustedcas_filepath` | read | `ConfigConstants.java:135` |
| Compliance file infos | every `searchguard.*filepath/*file_path` setting is hashed (sha256) with mtime into `audit_compliance_file_infos` | read | `dlic-auditlog/.../impl/AbstractAuditLog.java:851-861`, `AuditMessage.java:289-291` |
| Config dir permission check | walks the ES config dir and warns on loose POSIX permissions | read | `SearchGuardPlugin.java:319-380` |
| License signing keyring | bundled classpath resource `/KEYS` | read | `security/src/main/resources/KEYS`, `LicenseHelper.java:105-106` |
| Demo installer | writes `kirk.pem`, `esnode.pem`, `root-ca.pem`, `esnode-key.pem`, `kirk-key.pem` into the ES config dir, appends a `searchguard.*` block to `elasticsearch.yml`, writes `~/.searchguard/cluster_demo.yml` for sgctl | written by script | `security/tools/install_demo_configuration.sh:342-433`; `plugin/src/main/assemblies/searchguard-test-instance-installer.sh` |
| Entitlements | plugin may read `/etc/os-release`, `/usr/lib/os-release`, `/proc/sys/net/core/somaxconn` | read | `plugin/src/main/descriptor/entitlement-policy.yaml` |
| Lock files | none found (grep for `FileLock`, `.lock`, `lockfile` over main sources: no hit) | — | — |

## 6. Secrets and encryption at rest

| Item | Algorithm | Key source | Ref |
|---|---|---|---|
| Internal user passwords | bcrypt (OpenBSD `$2y$`), cost 12, 16-byte random salt; stored in `hash`, `password` removed on parse | — | `InternalUser.java:125-133,164-170` |
| `${envbc.X}` and `\|bcrypt` pipe | same bcrypt/12 | env var or config | `SgDynamicConfiguration.java:453-460`; `BcryptPipeFunction.java:25-49` |
| Config vars (`encrypted` form) | AES, default cipher `AES/CBC/PKCS5Padding`; GCM (128-bit tag) if cipher contains `/GCM/`; 16-byte random IV (hex), ciphertext base64, key id stored in doc | `searchguard.config_vars.encryption_keys.<id>.{cipher,key\|key_file,active}`; **fallback is a hard-coded base64 AES key `v9hGHVFi…` compiled into the plugin** (`EncryptionKeys.java:57-58`), used whenever no key is marked `active` (`:203-211`) | `EncryptionKeys.java:55-58,100-165`; `ConfigVar.java:40-44` |
| Session JWTs | HS512 by default; signing key is the 512-bit random config var `sessions_signing_key` (scope `authc`), optional JWE key | generated into `.searchguard_config_vars` (encrypted with the active key) | `SessionModule.java:85`; `SessionServiceConfig.java:50,129-158`; `ConfigVarService.java:388,438-471` |
| Auth-token JWTs | HS512 default; key `auth_tokens_signing_key_hs512`; optional `A256KW` encryption key | same mechanism | `AuthTokenModule.java:100`; `AuthTokenServiceConfig.java:57,128-155` |
| Signals internal auth token keys | stored **in clear** as `internal_auth.token_signing_key` / `internal_auth.token_encryption_key` docs in `.signals_settings` | generated at Signals init | `Signals.java:303-304`; `SignalsSettings.java:100-101` |
| Signals account credentials | stored in clear in `.signals_accounts` (e.g. e-mail `password`, `proxy_user`) | — | `EmailAccount.java:241-257` |
| License key | PGP clear-signed JSON, verified against bundled `/KEYS`; stored as-is | — | `LicenseHelper.java:72-112` |
| Field masking (FLS) | Blake2b-256 with salt + personalisation | `authz_dlsfls.field_anonymization.salt/personalisation` or legacy `authz.field_anonymization.salt` | `RoleBasedFieldMasking.java:291-292`; `DlsFlsConfig.java:141-143`; `AuthorizationConfig.java:95` |
| Compliance read-history hashing | salt `searchguard.compliance.salt`, default `e1ukloTsQlOgPquJ` | elasticsearch.yml | `ConfigConstants.java:167-168` |
| Keystore / PEM passwords | plain `searchguard.ssl.*.{keystore_password,keystore_keypassword,truststore_password,pemkey_password}` node settings | elasticsearch.yml (secure-settings support UNVERIFIED) | `SSLConfigConstants.java:39-47,56-64` |

## 7. Migrations

| Migration | Trigger | Mechanism | Ref |
|---|---|---|---|
| Config index `searchguard` → `.searchguard` | manual `POST /_searchguard/config/migrate_index` | search all docs (size 1000), create new index, bulk copy with same ids, reload+validate, broadcast `ConfigUpdateAction`; deletes new index on failure. Old index is **not** deleted; loader keeps preferring the new name | `MigrateConfigIndexApi.java:39-59`; `ConfigurationRepository.java:634-717` |
| Protected-index mapping versions | every cluster-state change, on every node, when the index already exists | `_meta.version` of the current mapping (0 if absent) vs declared version; applies the registered delta for the lowest `fromVersion ≥ current` via `PutMapping`; only Signals watches registers one (0→2) | `ProtectedConfigIndexService.java:255-310,381-404,585-625`; `Signals.java:183-184` |
| Protected indices made hidden | same | `UpdateSettings index.hidden=true` for pre-existing non-hidden SG indices | `ProtectedConfigIndexService.java:228-250` |
| Legacy env-var placeholders | every load of `internalusers`, `actiongroups`, `roles`, `rolesmapping`, `tenants` | regex substitution of `${env.}`, `${envbc.}`, `${envbase64.}` before JSON parsing | `CType.java:58-65`; `SgDynamicConfiguration.java:380-451` |
| FEMT 8.8.0 data migration (per-tenant Kibana indices → single `.kibana` with `sg_tenant`) | manual `POST /_searchguard/config/fe_multi_tenancy/data_migration/8_8_0` (status via GET) | 15 steps: populate tenants, find previous backups, check index state/blocks, write-block, create temp index, copy per-tenant data (scoped ids + `sg_tenant`), backup global index to `backup_fe_migration_to_8_8_0_<date>`, verify backup, add `sg_data_migrated_to_8_8_0` boolean marker to global index mapping, delete global content, copy temp→global, unblock, delete temp, enable MT in `frontend_multi_tenancy` config. State and optimistic lock in `.sg_data_migration_state`. CI job is manual-only | `datamigration880/rest/DataMigrationApi.java:35-38`; `service/steps/StepsFactory.java:42-55`; `IndexSettingsManager.java:39,125`; `DataMigrationService.java:60-120`; `ci/mt-data-migration-tests.yml` |
| FEMT runtime migration interception | Kibana server user bulk-writes / put-mapping / create-index on a `.kibana*` index (Kibana saved-object migration) | injects `sg_tenant` into documents with scoped ids and extends mappings | `FrontendDataMigrationInterceptor.java:66-110,214-220` |
| FEMT activation | `MultitenancyActivationAction` | puts `sg_tenant: keyword` on all FEMT aliases, then enables the config | `tenants/MultitenancyActivationService.java:54-72`; `TenantRepository.java:121-139` |

## Mixed-cluster and compatibility relevance

| Document | Newer node writes a field the older node does not know | Older node writes / older format read by newer node |
|---|---|---|
| `roles`, `rolesmapping`, `actiongroups`, `tenants`, `blocks`, `authc`, `authz`, `frontend_authc`, `frontend_multi_tenancy`, `authz_dlsfls`, `license_key` | Old node: load succeeds, entry is kept, the type is `PARTIALLY_INITIALIZED` and an ERROR with `UnsupportedAttribute` is logged on every reload (`SgDynamicConfiguration.java:131-135,168-171`). **Exception:** an unknown key inside `index_permissions[*]`/`alias_permissions[*]`/`data_stream_permissions[*]`/`tenant_permissions[*]` throws in `Role.Index` (`Role.java:194-195`) — that permission element is lost on the old node (whole-role effect UNVERIFIED). Old node's own API writes of that type will be rejected until the field is removed, because write validation is strict (`ConfigurationRepository.java:1126-1129`), and any successful old-node write overwrites the whole document (one doc per type). | Newer node reads the older shape without issue unless an attribute was removed-and-rejected (`exclude_index_permissions` in `roles`: load logs, write rejects; `Role.java:82-90`). Role `toBasicObject` returns the original source (`:150-152`), so API round-trips preserve unknown fields unless re-parsed strictly. |
| `internalusers` | Old node keeps and re-emits unknown fields untouched (`InternalUser.java:143-157,75-77`); no error. | Same. |
| `sessions`, `auth_token_service` | No `checkForUnusedAttributes` observed (UNVERIFIED) → ignored. | — |
| `config_vars` | Unknown keys ignored (`ConfigVar.java:65-99`); unknown `encrypted.key` id → `EncryptionException` on that var (`EncryptionKeys.java:147-149`), var unavailable → `${var:…}` resolution fails on that node. | A cipher/key added only on new nodes breaks old nodes' decryption; keep `encryption_keys` identical cluster-wide. |
| Session / auth-token docs | Extra fields ignored (`SessionToken.java:137-153`, `AuthToken.java:138-165`); `base.config_versions` carries CType `ord` on the wire (`ConfigVersion.java:48,134`) — the `sessions`/`authz_dlsfls` ord collision (10011) means an auth token whose base references one of them can resolve to the other CType after transport (UNVERIFIED which wins; `ordToInstanceMap.put` order depends on class-loading order). | — |
| Signals watch | Strict (`Watch.java:513`): a watch containing an attribute unknown to an old node fails to parse on that node → the job is skipped there and logged (`IndexJobConfigSource.java:129-132`); the watch may run only on new nodes. | Mapping is `dynamic:true`; new node's mapping update (`_meta.version`) is applied cluster-wide on first contact, old nodes keep running. |
| Signals state / accounts / settings | Lenient getters; unknown fields ignored. `.signals_settings` values unknown to an old node are put into `Settings` and ignored (`SignalsSettings.java:338-365`). | — |
| Audit records | Write-only, dynamic mapping, `audit_format_version` constant 4; daily index names mean mixed versions land in the same index. | — |
| FEMT saved objects | Old (pre-8.8) nodes do not know `sg_tenant`/scoped ids; mixing pre- and post-migration plugin versions is unsupported by design — the migration marker is an index-mapping field (`IndexSettingsManager.java:39`), not a cluster flag. | — |
| Config index itself | One ES document per CType, no format field, no per-field schema in the index (binary). Any node can rewrite any document; last writer wins unless the caller passes the ETag. | Legacy name `searchguard` still readable (`ConfigurationRepository.java:285-288`). |

## Open questions for human review

1. `CType` ord **10011 is used twice** (`sessions` `SessionServiceConfig.java:47`, `authz_dlsfls` `DlsFlsConfig.java:36`); `ConfigVersion` serialises the ord over transport (`ConfigVersion.java:48,134`) and `ordToInstanceMap` is last-wins (`CType.java:118`). Is the ord still load-bearing for config-history/auth-token snapshots?
   - **Answer:** _pending_
2. `EncryptionKeys` ships a **hard-coded default AES key** (`EncryptionKeys.java:57-58`) that encrypts every generated signing key unless an `active` key is configured; and `Entry.fromSettings` checks `key_file` but reads `file` (`:92-94`), so the documented `key_file` option cannot work as written.
   - **Answer:** _pending_
3. `ConfigurationRepository.getConfiguredSearchguardIndices(Settings)` assigns the new name to the variable called `…Old` and vice versa (`:1437-1442`); harmless today because the result is a set.
   - **Answer:** _pending_
4. Default init uploads `sg_frontend_authc.yml` but never `sg_frontend_multi_tenancy.yml` (`:573-593`) although the file ships in `security/sgconfig/`; `sg_config.yml` is checked for existence but never used (`:561-563`).
   - **Answer:** _pending_
5. `FeMultiTenancyConfig.parseLegacySgConfig` (`:77-92`) and `RestAuthcConfig`'s legacy `Network` attributes (`:143-150`) have no callers in main sources — dead code or exercised only by `security-legacy` tests?
   - **Answer:** _pending_
6. No ES `SystemIndexDescriptor` is registered; SG indices rely on `index.hidden` plus `ProtectedIndices`. Verify this is intended for ES 9 (system-index access level is consulted in `authz/SystemIndexAccess.java`).
   - **Answer:** _pending_
7. The internal audit index has no mapping or template (`InternalESSink.java:47-74`); field explosion/`audit_request_body` type conflicts across days are possible.
   - **Answer:** _pending_
8. Exact behaviour of a `Role` whose nested index-permission element throws on an unknown attribute (dropped element vs. dropped role) was not traced through codova's `asList().ofObjectsParsedBy`.
   - **Answer:** _pending_
9. `sessions` and `auth_token_service` parsers appear not to call `checkForUnusedAttributes`; confirm, since it changes the mixed-cluster row above.
   - **Answer:** _pending_
