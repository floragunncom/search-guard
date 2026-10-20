# Extension points and module registry

*Inventory pass, generated 2026-10-19 on branch experimental-analysis at commit 374449897. Every claim carries a file reference. Items marked UNVERIFIED were inferred, not read.*

## Summary for maintainers

*Written for human readers. The tables below are the authoritative record; this section is a guide to them and must be re-read whenever `SearchGuardPlugin`, `SearchGuardModule`, the module registry, `SearchGuardFilter` or a settings declaration changes.*

**What this covers.** How the plugin hooks into Elasticsearch, how its own modules plug into the core, in which order authentication, filters and privilege checks run for a request, and every setting the plugin declares.

**What you need to know.**

- Modules are found by hard-coded class names and loaded by reflection. A missing module only produces a warning. On that node, the module's transport actions, its index-level enforcement such as DLS/FLS, and its configuration validators are simply absent, with no error.
- Every plugin setting is node-scoped. None can be changed at runtime, so consistent behaviour depends on every node having an identical `elasticsearch.yml`.
- The request pipeline is: authentication in the HTTP layer, Elasticsearch routing, then per-action authorization in the plugin's action filter. The action filter has fast exits for the admin certificate, internal requests and configuration requests. Multi-tenancy runs before privilege evaluation, DLS/FLS after it.
- A few hooks are dead or inconsistent: an action-filter extension that nothing produces, a fast-lane result that is ignored after privilege evaluation, and registry lookups that recompute on every call.
- The SSL plugin's own transport interceptor is never registered, because the main plugin does not call its parent. Certificate extraction still works through inheritance in the request handler.

**What to review.**

- Section 2b, module order and what disabling a module removes.
- Section 5d, the action filter step order. This is the canonical description of the request pipeline for all six documents.
- Section 6, the two settings that are read but never registered (open questions 5 and 6). Both look like bugs.
- Interface ownership was checked against ES 9.4.4, not the pinned 9.5.5 (open question 10).

## Scope and method

Read-only pass over `*/src/main` (tests, `sgi8/`, `audit/`, `target/` ignored). Every ES `Plugin` subclass, every `SearchGuardModule` implementation, every secondary registry (typed components, filters, listeners, validators, Signals factories) and every `Setting`/`StaticSettings.Attribute` declaration was located with grep and then read. ES interface ownership of the overridden methods was checked with `javap` against the ES **9.4.4** jar in `~/.m2` (the root pom pins 9.5.5, which is not in the local repo; signatures are assumed unchanged — UNVERIFIED for 9.5.5).

Path aliases used below: `SGP` = `security/src/main/java/com/floragunn/searchguard/SearchGuardPlugin.java`, `SSLP` = `ssl/src/main/java/com/floragunn/searchguard/ssl/SearchGuardSSLPlugin.java`, `MOD` = `security/src/main/java/com/floragunn/searchguard/SearchGuardModule.java`, `REG` = `security/src/main/java/com/floragunn/searchguard/SearchGuardModulesRegistry.java`, `SGF` = `security/src/main/java/com/floragunn/searchguard/filter/SearchGuardFilter.java`, `CC` = `security/src/main/java/com/floragunn/searchguard/support/ConfigConstants.java`, `SSLCC` = `ssl/src/main/java/com/floragunn/searchguard/ssl/util/SSLConfigConstants.java`.

## 1. Elasticsearch plugin entry points

Only two classes in `src/main` extend an ES plugin type. The plugin descriptor names `SearchGuardPlugin` as the single entry point and declares `extended.plugins=lang-painless` (`plugin/src/main/descriptor/plugin-descriptor.properties:12,28`). An entitlement policy grants `manage_threads`, in/outbound network, three read-only files and a list of writable system properties (`plugin/src/main/descriptor/entitlement-policy.yaml:1-24`).

| Class | Declaration | ES types |
|---|---|---|
| `SearchGuardSSLPlugin` | `SSLP:81` | `extends Plugin implements ActionPlugin, NetworkPlugin` |
| `SearchGuardPlugin` (final) | `SGP:200` | `extends SearchGuardSSLPlugin implements ClusterPlugin, MapperPlugin, ScriptPlugin` |
| `SignalsPainlessExtension` | `signals/src/main/java/com/floragunn/signals/script/SignalsPainlessExtension.java:17` | `implements PainlessExtension` (SPI, `signals/src/main/resources/META-INF/services/org.elasticsearch.painless.spi.PainlessExtension`); returns one whitelist per `SignalsModule.getContexts()` context (`:19,30-31`) |

### 1a. `SearchGuardSSLPlugin` overrides

| ES method (owner) | Where | Returns |
|---|---|---|
| ctor `(Settings, Path)` → `(settings, configPath, disabled=false)` | `SSLP:94-98` | When `disabled`: all fields null, only sets `es.set.netty.runtime.available.processors=false` (`:100-118`). Else forces `jdk.tls.rejectClientInitiatedRenegotiation=true` unless `searchguard.ssl.allow_client_initiated_renegotiation` (`:128-155`), reads `httpSSLEnabled`/`transportSSLEnabled` (`:170-173`), picks `ExternalSearchGuardKeyStore` if `searchguard.ssl.client.external_context_id` names a registered context, else `DefaultSearchGuardKeyStore` (`:181-185`) |
| `getHttpTransports` (NetworkPlugin) | `SSLP:190-205` | `{"com.floragunn.searchguard.ssl.http.netty.SearchGuardSSLNettyHttpServerTransport" → SearchGuardSSLNettyHttpServerTransport}` if `httpSSLEnabled` (`:201`) |
| `getRestHandlers` (ActionPlugin) | `SSLP:208-214` | `[SearchGuardSSLInfoAction]` → `GET /_searchguard/sslinfo` (`ssl/.../ssl/rest/SearchGuardSSLInfoAction.java:58`) |
| `getTransportInterceptors` (NetworkPlugin) | `SSLP:217-225` | `[SearchGuardSSLTransportInterceptor]` if `transportSSLEnabled` (`:222`); it wraps every handler in `SearchGuardSSLRequestHandler` (`ssl/.../transport/SearchGuardSSLTransportInterceptor.java:47-49`) |
| `getTransports` (NetworkPlugin) | `SSLP:228-240` | `{"...SearchGuardSSLNettyTransport" → SearchGuardSSLNettyTransport}` if `transportSSLEnabled` (`:234`) |
| `createComponents` (Plugin) | `SSLP:243-265` | `[principalExtractor]` — `DefaultPrincipalExtractor` or class named by `searchguard.ssl.transport.principal_extractor_class` via reflection (`:247-260`) |
| `getSettings` (Plugin) | `SSLP:267-331` | 44 SSL settings, see §6b |
| `additionalSettings` (Plugin) | `SSLP:333-353` | `http.compression=false` (if unset) + `http.type` when HTTPS; `transport.type` when transport TLS |
| `getSettingsFilter` (Plugin) | `SSLP:355-360` | `["searchguard.*"]` |

### 1b. `SearchGuardPlugin` overrides

Three node-wide modes are fixed in the constructor: `disabled` (`searchguard.disabled`, `SGP:280-288`), `sslOnly` (`searchguard.ssl_only`, `:290-299`), and normal. In normal mode transport TLS is mandatory (`:311-313`), config-dir permissions are audited (`:316-343`), and modules are registered (`:345-357`, see §2).

| ES method (owner) | Where | Returns / effect |
|---|---|---|
| `close` (Plugin) | `SGP:237-246` | closes `AuditLogRelay` |
| `getRestHandlers` (ActionPlugin) | `SGP:399-454` | `[]` if disabled. Else SSL handlers (`:409`) + (if !sslOnly) 25 explicit handlers `:412-446` (info, kibanainfo, health, permission, enterprise mgmt REST API via `ReflectionHelper.instantiateMngtRestApiHandler` `:421`, SSL reload, component state, bulk/type-level/config-var/internal-users/authc/authz/frontend-authc/license/capabilities/migrate APIs, authc debug, whoami, config update) + `MockHasPrivilegesAction` (`:449`, also in sslOnly) + `moduleRegistry.getRestHandlers(...)` (`:450`) |
| `getActions` (ActionPlugin) | `SGP:456-501` | 33 explicit `ActionHandler`s plus the `AuthorizationConfigApi` and `SearchGuardLicenseKeyApi` handler lists if normal mode (`:459-495`) + `moduleRegistry.getActions()` always (`:498`, even when disabled — registry is empty then) |
| `getContexts` (ScriptPlugin) | `SGP:503-506` | `moduleRegistry.getContexts()` (only Signals contributes) |
| `onIndexModule` (Plugin) | `SGP:508-608` | Normal mode only: `setReaderWrapper(SearchGuardDirectoryReaderWrapper)` (`:522-523`), `forceQueryCacheProvider` iff any `QueryCacheWeightProvider` (`:525-558`), built-in scroll-context user check listener (`:561-598`), then module search/indexing listeners (`:600-606`). Throws if `adminDns == null` (`:513-515`) |
| `getActionFilters` (ActionPlugin) | `SGP:610-632` | `[SearchGuardFilter (order MIN_VALUE), ActionTraceFilter? (order 0), ...moduleRegistry.getActionFilters()]` — `SGF:129-130`, `support/.../diag/DiagnosticContext.java:162-168,187-188` |
| `getTransportInterceptors` (NetworkPlugin) | `SGP:634-665` | One anonymous `TransportInterceptor` delegating to `SearchGuardInterceptor.getHandler` / `sendRequestDecorate` (`:641-660`). **Does not call super** → the SSL-only interceptor is not stacked in normal mode |
| `getTransports` (NetworkPlugin) | `SGP:667-682` | super if sslOnly; else own `SearchGuardSSLNettyTransport` with audit-aware `SslExceptionHandler` |
| `getHttpTransports` (NetworkPlugin) | `SGP:684-731` | super if sslOnly; else `"com.floragunn.searchguard.http.SearchGuardHttpServerTransport"` → `SearchGuardHttpServerTransport` (TLS) or `SearchGuardNonSslHttpServerTransport`, both wrapping `SSLInfoPopulatingDispatcher(searchGuardRestFilter.wrap(dispatcher))` (`:716-729`) |
| `createComponents` (Plugin) | `SGP:733-899` | super if sslOnly; `[guiceDependencies, configModificationValidators]` + 16 core services (`:851-866`) + module components (`:868-870`) + capabilities + rest filter. Builds `BaseDependencies` (`:833-840`) and hands it to `moduleRegistry.createComponents` |
| `additionalSettings` (Plugin) | `SGP:901-917` | super + `transport.type`/`http.type` overrides unless sslOnly |
| `getSettings` (Plugin) | `SGP:919-1138` | super + `searchguard.ssl_only` + (if !sslOnly) ~70 settings, see §6a |
| `getBootstrapChecks` (Plugin) | `SGP:1140-1171` | super + `MultiTenancyChecker` bootstrap check (`alwaysEnforce=true`), unless disabled |
| `getSettingsFilter` (Plugin) | `SGP:1173-1183` | `["searchguard.*"]` unless disabled |
| `onNodeStarted` (ClusterPlugin) | `SGP:1185-1193` | `cr.initOnNodeStart()`, `moduleRegistry.onNodeStarted()`, `protectedConfigIndexService.onNodeStart()` |
| `getFieldFilter` (MapperPlugin) | `SGP:1195-1206` | AND of all module `FieldPredicate`s per index, default `ACCEPT_ALL` |

## 2. `SearchGuardModule` and `SearchGuardModulesRegistry`

### 2a. Interface (`MOD:59-167`)

| Method | Default | Consumed by |
|---|---|---|
| `getRestHandlers(RestHandlerDependencies)` | empty | `SGP:450` via `REG:154-165` |
| `getActions()` | empty | `SGP:498`; also fed into `Actions` so module actions become known `Action`s (`security/.../authz/actions/Actions.java:346-349`) |
| `getContexts()` | empty | `SGP:505` (ScriptPlugin) |
| `createComponents(BaseDependencies)` | empty | `REG:187-200`; also registers `getTypedComponents()` and picks first `MultiTenancyConfigurationProvider`/`TenantAccessMapper` from returned components (`REG:196-197,477-497`) |
| `getSettings()` → `StaticSettings.AttributeSet` | empty | `SGP:1130` via `REG:357-365` |
| `getImplicitHttpAuthenticationDomains()` | empty | `RestAuthenticationProcessor.Default` prepends them to configured authenticators (`security/.../authc/rest/RestAuthenticationProcessor.java:99`) |
| `getTypedComponents()` | empty | `TypedComponentRegistry` (`REG:193`) |
| `getCapabilities()` / `getUiCapabilities()` / `getPublicCapabilities()` | empty | `SearchGuardCapabilities` (`security/.../SearchGuardCapabilities.java:77-81`) |
| `getDirectoryReaderWrappersForNormalOperations()` | empty | `SGP:516-523`; skipped for admin-cert / `_sg_conf_request` (`security/.../authz/indices/SearchGuardDirectoryReaderWrapper.java:62-80,85-97`) |
| `getDirectoryReaderWrappersForAllOperations()` | empty | same; applied even for admin |
| `getSearchOperationListeners()` | empty | `SGP:600-602` |
| `getIndexOperationListeners()` | empty | `SGP:604-606` |
| `getSyncAuthorizationFilters()` | empty | `SGF:346-357` (post-privilege) |
| `getPrePrivilegeEvaluationSyncAuthorizationFilters()` | empty | `SGF:319-333` |
| `getFieldFilters()` → `Function<String,FieldPredicate>` | empty | `SGP:1198` |
| `getQueryCacheWeightProviders()` | empty | `SGP:525-558` |
| `getAuditLog()` | `null` | first non-null wins (`REG:377-387`), set into `AuditLogRelay` (`SGP:876-880`) |
| `onNodeStarted()` | no-op | `SGP:1190` |
| `getActionFilters()` | empty | `SGP:629` |
| `getConfigModificationValidators()` | empty | `SGP:891` → `ConfigurationRepository` validates on every write (`security/.../configuration/ConfigurationRepository.java:811,936,1028,1179`) |

### 2b. Registry (`REG:75-499`)

| Aspect | Detail |
|---|---|
| Discovery | **Not** SPI. Hard-coded FQCN strings passed to `add(String...)` from the plugin constructor (`SGP:345-357`); `Class.forName` + ctor `(Settings)` or no-arg (`REG:428-441`) |
| Enterprise list | `EnterpriseAuthFeaturesModule`, `AuthTokenModule`, `FeMultiTenancyModule`, `DlsFlsModule`, `AuditLogModule` (`SGP:346-348`), only when `searchguard.enterprise_modules_enabled` (default true, `:304`). `FeMultiTenancyModule` removed when `searchguard.unsupported.single_index_mt_enabled=false` (`:349-351`) |
| Order | enterprise set (an `ImmutableSet`, iteration order = insertion order above) → `SchedulerModule` → `SessionModule` → `SignalsModule` (`SGP:352-357`). All aggregating getters iterate `modules` in this order |
| Missing class | `ClassNotFoundException` → `log.warn("Not found")`, node continues (`REG:141-142`); so a community build simply lacks the dlic classes |
| Duplicate | `IllegalStateException` (`REG:121-123`) |
| `searchguard.modules.disabled` | `Setting.listSetting`, NodeScope (`REG:77-78`). Value `all` skips every `add` (`REG:109-112`); otherwise FQCN match skips that class (`REG:116-119`). Disabled modules contribute nothing — no settings, REST handlers, filters, typed components, capabilities |
| Non-module objects | Any registered object that is a `ComponentStateProvider` is also tracked (`REG:135-137`); core services are added directly via `addComponentStateProvider` (`SGP:776-893`) |
| Caching | `getDirectoryReaderWrappers*`, `getSearch/IndexOperationListeners`, `getActionFilters`, `getConfigModificationValidators` memoise. `getSyncAuthorizationFilters`, `getPrePrivilegeSyncAuthorizationFilters`, `getFieldFilters`, `getQueryCacheWeightProviders` recompute on every call because the local result is never stored (`REG:301-355`) — harmless today since they are called once, but inconsistent |
| Built-in typed components | `AuthenticationBackend/noop`; `HttpAuthenticationFrontend/basic,anonymous,trusted_origin,clientcert`; `ApiAuthenticationFrontend/basic,link` (`REG:451-467`); plus `internal_users_db` backend + user-info backend registered from the plugin (`SGP:819-820`) |

## 3. Implementation matrix

Columns: R=getRestHandlers, A=getActions, X=getContexts, C=createComponents, S=getSettings, I=getImplicitHttpAuthenticationDomains, T=getTypedComponents, Cap=get*Capabilities, DRn/DRa=directory reader wrappers normal/all, SL=search listeners, IL=index listeners, F=sync filters, PF=pre-privilege sync filters, FF=field filters, Q=query cache, AL=getAuditLog, N=onNodeStarted, AF=getActionFilters, V=config validators, CS=ComponentStateProvider.

| Class (Maven module) | R | A | X | C | S | I | T | Cap | DRn | DRa | SL | IL | F | PF | FF | Q | AL | N | AF | V | CS |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `EnterpriseAuthFeaturesModule` (dlic-security) `dlic-security/.../enterprise/auth/EnterpriseAuthFeaturesModule.java:33` | | | | 38 | | | 45 | | | | | | | | | | | | | | |
| `AuthTokenModule` (dlic-security) `dlic-security/.../authtoken/AuthTokenModule.java:69` | 78 | 85 | | 97 | 142 | 154 | | 159 | | | | | | | | | | | | | 149 |
| `FeMultiTenancyModule` (dlic-fe-multi-tenancy) `.../femt/FeMultiTenancyModule.java:70` | 218 | 223 | | 103 | 239 | | | 229 | | | | | | 234 | | | | | | 244 | 201 |
| `DlsFlsModule` (dlic-dlsfls) `.../dlsfls/DlsFlsModule.java:53` | 175 | 180 | | 89 | 185 | | | 170 | 140 | | 145 | | 150 | | 155 | 160 | | | | | 165 |
| `AuditLogModule` (dlic-auditlog) `.../auditlog/AuditLogModule.java:43` | | | | 53 | | | | | | 77 | | 83 | | | | | 96 | | 88 | | |
| `SchedulerModule` (scheduler) `scheduler/.../jobs/SchedulerModule.java:31` | | 33 | | | | | | | | | | | | | | | | | | | |
| `SessionModule` (security) `security/.../authc/session/backend/SessionModule.java:58` | 68 | 73 | | 83 | 131 | 141 | | 146 (public) | | | | | | | | | | | | | 136 |
| `SignalsModule` (signals) `signals/.../signals/SignalsModule.java:110` | 129 | 150 | 195 | 205 | 233 | | | 247 | | | | | | | | | | 238 | | | 242 |

Numbers are line numbers in the named file. Notable behaviour per module:

| Module | What it contributes |
|---|---|
| `EnterpriseAuthFeaturesModule` | Only typed components, and only if `searchguard.enterprise_modules_enabled` (`:46-52`): `jwt`, `kerberos` (HTTP frontends), `oidc`, `saml` (API frontends), `external_session`, `ldap` (backends), `oidc_userinfo`, `ldap` (user-info backends) |
| `AuthTokenModule` | 4 REST handlers (`:80-81`), 6 actions + config API (`:86-93`), `AuthTokenService` registered as `SpecialPrivilegesEvaluationContextProvider` (`:117`), implicit `AuthTokenAuthenticationDomain` (`:119,154-156`), capability `auth_tokens`, state requires enterprise licence (`:74`) |
| `FeMultiTenancyModule` | Components `[FeMultiTenancyConfigurationProvider, TenantAccessMapper, AvailableTenantService, MultitenancyActivationService]` (`:169`); pre-privilege filter delegating to `MultiTenancyAuthorizationFilter` only when config `enabled` (`:151-153,186-197`); `FeMultiTenancyEnabledFlagValidator` (`:244-245`); capability `fe_multi_tenancy` |
| `DlsFlsModule` | `DlsFlsValve` (sync filter), `DlsFlsSearchOperationListener`, `FlsFieldFilter`, `FlsQueryCacheWeightProvider`, `DlsFlsDirectoryReaderWrapper` factory (`:94-107`); `DlsFlsConfigApi`; capabilities `dls`,`fls`,`field_masking`; licence-gated state (`:63,124`) |
| `AuditLogModule` | Everything only if `searchguard.audit.type` is set (`:54-55`): `AuditLogImpl`, `ComplianceIndexingOperationListenerImpl`, `ReadLogDirectoryReaderWrapper` (all-operations, i.e. also for admin), `ComplianceIndexActionFilter` (order 1) and `ComplianceIndexTemplateActionFilter` (order 2) (`dlic-auditlog/.../access_log/write/ComplianceIndexActionFilter.java:49`, `...TemplateActionFilter.java:56`) |
| `SchedulerModule` | 2 actions: `CheckForExecutingTriggerAction`, `SchedulerConfigUpdateAction` (`:34-37`) |
| `SessionModule` | REST session API + config API, 4 actions + config handlers, `SessionService`, implicit `SessionTokenAuthenticationDomain`, public capability `login_sessions` |
| `SignalsModule` | Gated on `signals.enabled` (ctor `:116-121`); 20 REST handlers, 35 actions (`:135-187`), 6 script contexts (`:197-198`), `Signals` core; capability `signals` |

## 4. Secondary extension interfaces

| Interface | Registry / lookup | Implementations (module) |
|---|---|---|
| `SyncAuthorizationFilter` (`security/.../authz/SyncAuthorizationFilter.java:26`; results `OK/DENIED/INTERCEPTED/PASS_ON_FAST_LANE` `:30-33,57-58`) | `REG:301-327` → `SGF` | `DlsFlsValve` (dlic-dlsfls, `.../dlsfls/DlsFlsValve.java:78`) post-privilege; `MultiTenancyAuthorizationFilter` (dlic-fe-multi-tenancy, `.../femt/MultiTenancyAuthorizationFilter.java:64`) wrapped by an anonymous delegate (`FeMultiTenancyModule.java:186`) pre-privilege |
| `TypedComponent.Info<T>` (`security/.../TypedComponent.java:27-34`) | `TypedComponentRegistry` (`security/.../TypedComponentRegistry.java:35`); `create(type, name, DocNode, Context)` falls back to `Class.forName(name)` with ctor `(DocNode, Context)` when `name` contains a dot (`:54-80`). Consumed by `StandardAuthenticationDomain.parse` (`security/.../authc/base/StandardAuthenticationDomain.java:170,209,221,294`) | see rows below |
| `HttpAuthenticationFrontend` (`security/.../authc/rest/HttpAuthenticationFrontend.java:45`) | typed registry | `basic` `BasicAuthenticationFrontend`, `anonymous`, `trusted_origin`, `clientcert` (security, `REG:456-460`); `jwt` `JwtAuthenticator` (`dlic-security/.../auth/jwt/JwtAuthenticator.java:353`), `kerberos` `KerberosAuthenticationFrontend` (`.../kerberos/KerberosAuthenticationFrontend.java:277`); not registered by name but used by implicit domains: `SessionTokenAuthenticationDomain.SessionAuthenticator` (security), `AuthTokenHttpJwtAuthenticator` (dlic-security) |
| `ApiAuthenticationFrontend` (`security/.../authc/session/ApiAuthenticationFrontend.java:30`) | typed registry | `basic`, `link` (`REG:462-463`); `oidc` `OidcAuthenticator` (`.../oidc/OidcAuthenticator.java:435`), `saml` `SamlAuthenticator` (`.../saml/SamlAuthenticator.java:540`); `JwtAuthenticator` also implements it but is registered only as HTTP frontend |
| `AuthenticationBackend` (`security/.../authc/AuthenticationBackend.java:29`) | typed registry | `noop` (`REG:454`), `internal_users_db` (`SGP:819`, `security/.../internal_users_db/InternalUsersAuthenticationBackend.java:121`), `ldap` (`.../ldap/LDAPAuthenticationBackend.java:295`), `external_session` (`.../session/ExternalSearchGuardSessionAuthenticationBackend.java:258`) |
| `UserInformationBackend` (`security/.../authc/UserInformationBackend.java:24`) | typed registry | `internal_users_db` (`SGP:820`, `InternalUsersAuthenticationBackend.java:145`), `ldap` (`LDAPAuthenticationBackend.java:311`), `oidc_userinfo` (`.../oidc/OidcUserInfoBackend.java:119`) |
| `AuthenticationDomain<HttpAuthenticationFrontend>` (implicit) | `REG:367-375` | `SessionTokenAuthenticationDomain` (security), `AuthTokenAuthenticationDomain` (dlic-security) |
| `Function<IndexService, CheckedFunction<DirectoryReader,...>>` | `REG:202-232` → `SearchGuardDirectoryReaderWrapper` | normal: `DlsFlsDirectoryReaderWrapper` (dlic-dlsfls); all: `ReadLogDirectoryReaderWrapper` (dlic-auditlog) |
| `Function<String, FieldPredicate>` | `REG:329-341` → `SGP:1196` | `FlsFieldFilter` (dlic-dlsfls, `.../dlsfls/FlsFieldFilter.java`) |
| `SearchGuardModule.QueryCacheWeightProvider` (`MOD:155-157`) | `REG:343-355` → `SGP:525` | `FlsQueryCacheWeightProvider` (dlic-dlsfls, `.../dlsfls/FlsQueryCacheWeightProvider.java:40`) |
| `SearchOperationListener` | `REG:234-249` | built-in scroll guard (`SGP:561`), `DlsFlsSearchOperationListener` (dlic-dlsfls) |
| `IndexingOperationListener` | `REG:251-266` | `ComplianceIndexingOperationListenerImpl` (dlic-auditlog) |
| `ActionFilter` (module-level) | `REG:268-283` | `ComplianceIndexActionFilter`, `ComplianceIndexTemplateActionFilter` (dlic-auditlog) |
| `ConfigModificationValidator<T>` (`security/.../configuration/validation/ConfigModificationValidator.java:30`) | `ConfigModificationValidators` (`...ConfigModificationValidators.java:26`), filled at `SGP:890-891` | `RoleRelationsValidator` (security, core), `FeMultiTenancyEnabledFlagValidator` (dlic-fe-multi-tenancy) |
| `ComponentStateProvider` (`support/.../cstate/ComponentStateProvider.java`) | `REG:135-137,150-152,395-426`; exposed via `ComponentStateRestAction`/`GetComponentStateAction` (`SGP:426,460`) | 28 classes in `src/main` (modules, `PrivilegesEvaluator`, `ConfigurationRepository`, `LicenseRepository`, `AuthenticatingRestFilter`, `LDAPConnectionManager`, `IndexCleanupAgent`, …) — only those explicitly `add`ed are reported |
| `SpecialPrivilegesEvaluationContextProvider` | `SpecialPrivilegesEvaluationContextProviderRegistry.add` (`security/.../privileges/SpecialPrivilegesEvaluationContextProviderRegistry.java:21`) | `InternalAuthTokenProvider::userAuthFromToken` (`SGP:813`), `AuthTokenService` (`AuthTokenModule.java:117`) |
| `AuditLog` | first module non-null (`REG:377-387`) into `AuditLogRelay` | `AuditLogImpl`/`AbstractAuditLog` (dlic-auditlog), `NullAuditLog`, `AuditLogRelay` (security) |
| `MultiTenancyConfigurationProvider` / `TenantAccessMapper` | first matching component (`REG:477-497`), defaults `DEFAULT`/`NO_OP` | `FeMultiTenancyConfigurationProvider`, `FeMultiTenancyTenantAccessMapper` (dlic-fe-multi-tenancy) |
| `InterClusterRequestEvaluator` | class name from `searchguard.cert.intercluster_request_evaluator_class` (`SGP:763-769`, `ReflectionHelper.java:144`) | `DefaultInterClusterRequestEvaluator` (uses `searchguard.cert.oid`, `searchguard.nodes_dn`, `security/.../transport/DefaultInterClusterRequestEvaluator.java:42-44`), `OIDClusterRequestEvaluator` (`:35-36`) |
| `PrincipalExtractor` | `searchguard.ssl.transport.principal_extractor_class` (`SGP:822-827`, `SSLP:247-260`) | `DefaultPrincipalExtractor` (ssl) |
| Enterprise mgmt REST API | reflective `com.floragunn.searchguard.dlic.rest.api.SearchGuardRestApiActions.getHandler` (`security/.../support/ReflectionHelper.java:81-85,98-106`) | 9 handlers: internal users, roles mapping, roles, action groups, flush cache, license, permissions info, tenants, blocks (`dlic-security/.../dlic/rest/api/SearchGuardRestApiActions.java:46-62`) |
| Signals `ActionHandler.Factory` | `ActionHandler.factoryRegistry` (`signals/.../watch/action/handlers/ActionHandler.java:118,155-156`) | `index`, `webhook`, `email`, `slack` (signals); `jira`, `pagerduty` added reflectively by `SignalsEnterpriseFeatures.init()` (`dlic-signals/.../SignalsEnterpriseFeatures.java:12`, called via `Class.forName` from `signals/.../Signals.java:121-123,445-462` when enterprise + `signals.enterprise.enabled`) |
| Signals `Account.Factory` | `Account.factoryRegistry` (`signals/.../accounts/Account.java:181,230`) | `email`, `slack` (signals); `jira`, `pagerduty` (dlic-signals, `SignalsEnterpriseFeatures.java:13`) |
| Signals check types | hard `switch` in `Check.create` (`signals/.../watch/checks/Check.java:48-68`) — not pluggable | `search`, `static`, `http`, `condition`/`condition.script`, `calc`, `transform` |
| Signals `ScriptContext`s | `SignalsModule.getContexts` (`:197-198`) | `ConditionScript`, `TransformScript`, `CalcScript`, `SeverityValueScript`, `SignalsObjectFunctionScript`, `TEMPLATE_CONTEXT` |

## 5. Request pipeline order

### 5a. REST (HTTP) path

| Step | Where |
|---|---|
| 1. Netty HTTP transport (`SearchGuardHttpServerTransport` / `SearchGuardNonSslHttpServerTransport`, `security/.../http/`) hands the request to `SSLInfoPopulatingDispatcher`, which rejects client-supplied `_sg_ssl_*` headers and stores principal/certs/protocol/cipher as transients | `SGP:716-729`; `ssl/.../ssl/http/netty/SSLInfoPopulatingDispatcher.java:77-95,169-178` |
| 2. `AuthenticatingRestFilter.AuthenticatingRestHandler.dispatchRequest` → `handleRequest` | `security/.../authc/rest/AuthenticatingRestFilter.java:113-115,129,138` |
| 3. `checkRequest`: bad `_sg_*` headers → 403 + audit; sets origin `REST` | `:221-236` |
| 4. `isAuthcRequired` false for `OPTIONS`, `/_searchguard/license`, `/_searchguard/health`, `POST /_searchguard/auth/session` → dispatch directly | `:215-219` |
| 5. Admin-cert DN (`SG_SSL_PRINCIPAL` vs `AdminDNs`) short-circuits authentication | `:147-158` |
| 6. `RestAuthenticationProcessor.Default.authenticate`: blocked-IP check, then `RestRequestAuthenticationProcessor` iterates implicit domains + configured `authc` domains in order, skipping disabled / non-accepting ones; blocked-user check per candidate | `RestAuthenticationProcessor.java:99,168,179`; `security/.../authc/base/RequestAuthenticationProcessor.java:138,162,170,435` |
| 7. `PASS` → `SearchGuardContext.setUser`, dispatch to ES `RestController`; else response with status/headers from `AuthcResult` | `AuthenticatingRestFilter.java:173-201` |

### 5b. Transport receive path

| Step | Where |
|---|---|
| 1. `SearchGuardSSLRequestHandler.messageReceived` (final): strips `_sg_ssl_*` headers, extracts principal/certs from the Netty SSL handler into transients | `ssl/.../transport/SearchGuardSSLRequestHandler.java:78-92,133-139` |
| 2. `SearchGuardRequestHandler.messageReceivedDecorate`: sets `SG_CHANNEL_TYPE`, `SG_ACTION_NAME`; direct/local channels pass through | `security/.../transport/SearchGuardRequestHandler.java:78,105-106,126` |
| 3. `internal:*` / shard (`[`) actions from non-server, non-trusted-cluster nodes → `ElasticsearchSecurityException` (except `internal:transport/handshake`) | `:133-142` |
| 4. No SSL principal → reject; else origin `TRANSPORT`; inter-cluster or trusted-cluster request keeps forwarded headers; otherwise only an admin-DN cert user is accepted, everything else "untrusted node denied" | `:146-189` |
| 5. Inter-cluster decision made by `InterClusterRequestEvaluator`, stored as `_sg_ssl_transport_intercluster_request` / `..._trustedcluster_request` transients | `:230-241` |

### 5c. Transport send path

`SearchGuardInterceptor.sendRequestDecorate` (`security/.../transport/SearchGuardInterceptor.java:108`) copies the user/origin/remote-address/DLS/FLS headers to the outgoing request (`:149-166`), strips DLS/FLS headers for cross-cluster targets and replaces them with CCS-specific ones (`:180-206`), adds the diagnostic action stack (`:211`) and restores context + CCS DLS/FLS response headers on reply (`:288-337`). Custom headers are allowed only if they match `searchguard.allow_custom_headers` (`:244-286`).

### 5d. `SearchGuardFilter` (`ActionFilter`, order `Integer.MIN_VALUE`, `SGF:128-131`)

| # | Step | Where |
|---|---|---|
| 1 | `specialPrivilegesEvaluationContextProviderRegistry.provide(...)` (auth tokens may replace the user), then `apply0` in a stored context | `SGF:137-149` |
| 2 | Origin defaults to `LOCAL`; compliance source-field context header attached | `:158-164` |
| 3 | Flags: `userIsAdmin`, `interClusterRequest`, `trustedClusterRequest`, `confRequest` (`_sg_conf_request`), `passThroughRequest` (license info, `indices:admin/seq_no*`, whoami), `internalRequest` | `:166-175` |
| 4 | User may be replaced by special-privileges context; for `SearchRequest` the user is additionally put into the `_sg_user_header` **header** (`ConfigConstants.SG_USER_HEADER`; `_sg_user` is the local transient) (chunked fetch phase workaround, ES ≥ 9.5.2) | `:179-206` |
| 5 | **Fast exit**: admin / conf / internal / pass-through → `chain.proceed` (admin gets a GRANTED audit entry) | `:240-248` |
| 6 | Compliance immutable-index check (may fail the request or wrap the listener) | `:252-277` |
| 7 | **Local-origin exit**: origin `LOCAL` and (inter-cluster or direct request without special context needing evaluation) → proceed | `:279-285` |
| 8 | No user: `cluster:monitor/state*` proceeds, anything else → 500 "No user found" | `:287-299` |
| 9 | Evaluator not initialised → 503 SG11 | `:303-309` |
| 10 | Mapped roles + `PrivilegesEvaluationContext` | `:315-317` |
| 11 | **Pre-privilege `SyncAuthorizationFilter`s** (FeMT): `OK` continue, `DENIED` fail, `INTERCEPTED` stop, `PASS_ON_FAST_LANE` proceed without evaluation | `:319-333` |
| 12 | `PrivilegesEvaluator.evaluate` | `:335-336` |
| 13 | On OK: audit GRANTED; **post-privilege `SyncAuthorizationFilter`s** (DLS/FLS) — `PASS_ON_FAST_LANE` is *not* handled here (falls through as if OK) | `:338-357` |
| 14 | Additional action filters from the evaluation result wrap the chain (`ExtendedActionFilterChain`) — no production code ever calls `PrivilegesEvaluationResult.with(ActionFilter)`, so this branch is dead today | `:359-361`; `security/.../authz/PrivilegesEvaluationResult.java:147` |
| 15 | Well-known actions needing special processing go through `ExtendedActionHandlingService` (resource-owner handling); else `chain.proceed` | `:363-370` |
| 16 | On failure: audit MISSING_PRIVILEGES + security exception | `:372-374` |

Module `ActionFilter`s run *after* `SearchGuardFilter` (`ActionTraceFilter` order 0, compliance filters order 1 and 2) because ES sorts filters by `order()`.

## 6. Settings

No setting anywhere in `src/main` carries `Property.Dynamic` (grep over all modules: zero hits) — every Search Guard setting is `NodeScope`. `StaticSettings.Attribute.toPlatformProperties` always yields `NodeScope` plus `Filtered` when flagged (`support/.../StaticSettings.java:102-112`). `F` below = `Property.Filtered`, `D` = `Property.Deprecated`.

### 6a. `SearchGuardPlugin.getSettings` (`SGP:919-1138`; keys from `CC`)

| Key | Type / default | Props | Where |
|---|---|---|---|
| `searchguard.ssl_only` | bool `false` | F | `SGP:924` (registered even in sslOnly mode) |
| `searchguard.authcz.admin_dn` | list `[]` | | `:927` |
| `searchguard.cert.oid` | string | F | `:930` |
| `searchguard.cert.intercluster_request_evaluator_class` | string | F | `:932` |
| `searchguard.nodes_dn` | list `[]` | | `:934` |
| `searchguard.disabled` | bool `false` | F | `:936` (read in ctor before registration, `:258`) |
| `searchguard.enterprise_modules_enabled` | bool `true` | F | `:939` |
| `searchguard.authcz.rest_impersonation_user.` | group | | `:941` |
| `searchguard.audit.type` | string | F | `:944` (presence enables `AuditLogModule`) |
| `searchguard.audit.routes.` / `searchguard.audit.endpoints.` | group | | `:945-946` |
| `searchguard.audit.threadpool.size` / `.max_queue_len` | int `10` / `100000` | F | `:947-949` |
| `searchguard.audit.log_request_body` | bool `true` | F | `:950` |
| `searchguard.audit.ignore_request_bodies` | list `[BulkRequest, indices:data/write/bulk, */_bulk*]` | | `:951-955` |
| `searchguard.audit.resolve_indices` / `.enable_rest` / `.enable_transport` | bool `true` | F | `:956-958` |
| `searchguard.audit.config.disabled_transport_categories` / `.disabled_rest_categories` | list `[AUTHENTICATED, GRANTED_PRIVILEGES]` | | `:959-965` |
| `searchguard.audit.ignore_users` | list `[kibanaserver]` | | `:966-968` |
| `searchguard.audit.ignore_requests` | list `[]` | | `:969` |
| `searchguard.audit.resolve_bulk_requests` | bool `false` | F | `:971` |
| `searchguard.audit.exclude_sensitive_headers` | bool `true` | F | `:972-973` |
| `searchguard.audit.config.disabled_fields` | list `[]` | | `:974-975` |
| `searchguard.audit.config.index` / `.type` | string | F | `:978-982` |
| `searchguard.audit.config.custom_attributes.` | group | | `:983-986` |
| `searchguard.audit.config.http_endpoints` | list `[localhost:9200]` | | `:989-991` |
| `searchguard.audit.config.{username,password,pemcert_content,pemcert_filepath,pemkey_content,pemkey_filepath,pemkey_password,pemtrustedcas_content,pemtrustedcas_filepath,cert_alias}` | string | F | `:992-1030` |
| `searchguard.audit.config.enable_ssl` / `.enable_ssl_client_auth` | bool `false` | F | `:998-1006` |
| `searchguard.audit.config.verify_hostnames` | bool `true` | F | `:1001-1003` |
| `searchguard.audit.config.enabled_ssl_ciphers` / `.enabled_ssl_protocols` | list `[]` | | `:1031-1036` |
| `searchguard.audit.config.webhook.url` / `.webhook.format` / `.webhook.ssl.pemtrustedcas_filepath` / `.webhook.ssl.pemtrustedcas_content` | string | F | `:1039-1052` |
| `searchguard.audit.config.webhook.ssl.verify` | bool `true` | F | `:1044-1046` |
| `searchguard.audit.config.log4j.logger_name` / `.log4j.level` | string | F | `:1055-1059` |
| `searchguard.kerberos.krb5_filepath` / `.acceptor_keytab_filepath` / `.acceptor_principal` | string | F | `:1062-1064` |
| `searchguard.restapi.roles_enabled` | list `[]` | | `:1067` |
| `searchguard.restapi.endpoints_disabled.` | group | | `:1069` |
| `searchguard.unsupported.restapi.accept_invalid_license` | bool `false` | F | `:1070` |
| `searchguard.restapi.password_validation_regex` / `_error_message` | string | F | `:1073-1075` |
| `searchguard.compliance.history.write.watched_indices` / `.read.watched_fields` / `.read.ignore_users` / `.write.ignore_users` | list `[]` | | `:1078-1081,1092-1095` |
| `searchguard.compliance.history.write.metadata_only` / `.read.metadata_only` / `.write.log_diffs` / `.external_config_enabled` / `.internal_config_enabled` | bool `false` | F | `:1082-1089,1101` |
| `searchguard.compliance.history.external_config.env_vars.enabled` | bool `true` | F | `:1090` |
| `searchguard.compliance.disable_anonymous_authentication` / `.local_hashing_enabled` | bool `false` | F | `:1096,1103` |
| `searchguard.compliance.immutable_indices` | list `[]` | | `:1098` |
| `searchguard.compliance.salt` / `.mask_prefix` | string | F | `:1100,1105` |
| `searchguard.allow_custom_headers` | list `[]` | | `:1107` |
| `searchguard.dfm_empty_overrides_all` | bool `false` | F, D | `:1109` |
| `searchguard.unsupported.allow_now_in_dls` / `.restapi.allow_sgconfig_modification` | bool `false` | F | `:1112-1115` |
| `searchguard.unsupported.load_static_resources` | bool `true` | F | `:1117-1118` |
| `searchguard.dls.mode` | string | F | `:1120` |
| `searchguard.resource_owner_handling.retry_owner_check.max` / `.delay_ms` | int `1` / `10` | F | `ResourceOwnerService.java:63-66` via `:1122` |
| `searchguard.resource_owner_handling.cleanup_interval` / `.resource.default_lifetime` | time `1h` / `7d` (min 1m) | F | `ResourceOwnerService.java:67-71` |
| `searchguard.resource_owner_handling.index.refresh_on_write` | string `IMMEDIATE` | F | `ResourceOwnerService.java:72-73` |
| `searchguard.ssl.cert_reload_enabled` | bool `false` | F | `:1124` |
| `searchguard.multi_tenancy_bootstrap_check_enabled` | bool `false` | F | `:1125`; `security/.../MultiTenancyChecker.java:44` |
| `searchguard.modules.disabled` | list `[]` | | `REG:77` via `:1127` |
| `searchguard.config_vars.encryption_keys.` | group | | `security/.../configuration/variables/EncryptionKeys.java:58` via `:1128` |
| `searchguard.config_index_name` / `searchguard.config_repository.index_name` | string (defaults `OLD_INDEX_NAME_DEFAULT` / `NEW_INDEX_NAME_DEFAULT`) | | `ConfigurationRepository.java:130-133,143` via `:1129` |
| `searchguard.allow_default_init_sgindex` / `searchguard.background_init_if_sgindex_not_exist` | bool `false` / `true` | | `ConfigurationRepository.java:134-141` |
| `searchguard.diagnosis.action_stack.enabled` / `searchguard.logging.context.extended` | bool `false` | | `support/.../diag/DiagnosticContext.java:43-49` via `:1131` |
| `searchguard.admin_only_actions` | pattern `cluster:admin:searchguard:config/*, cluster:admin:searchguard:internal/*` | | `PrivilegesEvaluator.java:86-87` via `:1132` |
| `searchguard.admin_only_indices` | pattern `searchguard, .searchguard, …` | | `PrivilegesEvaluator.java:89-91` |
| `searchguard.check_snapshot_restore_write_privileges` | bool `true` | | `PrivilegesEvaluator.java:93` |
| `searchguard.unsupported.restore.sgindex.enabled` | bool `false` | | `PrivilegesEvaluator.java:96` |
| `searchguard.privileges_evaluation.precomputed_privileges.max_heap_size` | bytes `10mb` | | `RoleBasedActionAuthorization.java:74-76` via `PrivilegesEvaluator.java:100` |
| `searchguard.roles_mapping_resolution` | string `MAPPING_ONLY` | | `AuthorizationService.java:44-48` via `:1133` |
| Module attributes (`:1130`): `searchguard.sessions.index.name` `.searchguard_sessions`, `searchguard.sessions.cleanup_interval` `1h` | | | `SessionService.java:130-131` |
| `searchguard.unsupported.single_index_mt_enabled` `true` | | | `FeMultiTenancyModule.java:78-79` (also read raw in `SGP:349`) |
| `searchguard.dls_fls.provide_thread_context_authz_hash` `false` | | | `DlsFlsModule.java:61` |
| `searchguard.authtokens.index.name` `.searchguard_authtokens`, `searchguard.authtokens.cleanup_interval` `1h`, `searchguard.config_history.index.name` `.searchguard_config_history`, `searchguard.config_history.cache.ttl` `2880`, `.cache.max_size` `100`, `.model.cache.ttl` `2880`, `.model.cache.max_size` `100` | | | `AuthTokenService.java:126-129`, `ConfigHistoryService.java:68-77`, registered at `AuthTokenModule.java:143-145` |
| `signals.enabled` `true`, `signals.enterprise.enabled` `true`, `signals.worker_threads.pool.max_size` `5`, `.pool.keep_alive` `100m`, `signals.worker_threads.prio` `5`, `signals.all_tenants_active_by_default` `true`, `signals.watch_log.refresh_policy` `null`, `.sync_indexing` `false`, `.mapping_total_fields_limit` `1000`, `signals.index_names.{watches,watches_state,watches_trigger_state,accounts,log}` | | | `SignalsSettings.java:527-551`, set at `:586-589`. **`signals.index_names.settings` (`:550`) is defined but not in the registered set** |

Not registered as ES settings although read from `Settings`: `searchguard.compliance.history.read.watched_fields` is additionally defined as a `StaticSettings.Attribute` in `AuditLogConfig.java:54` (not in any module `AttributeSet`); `searchguard.ssl.allow_client_initiated_renegotiation` is read at `SSLP:128-129` but absent from `SSLP.getSettings` — UNVERIFIED whether ES rejects it as unknown (`Filtered` settings must be registered; an unregistered `searchguard.*` key normally fails node start).

Signals per-tenant runtime settings (`SignalsSettings.java:93-109`, e.g. `active`, `execution.default_throttle_period`) are `Setting` objects used to parse the `.signals_settings` index document, not ES cluster settings.

### 6b. `SearchGuardSSLPlugin.getSettings` (`SSLP:267-331`; keys from `SSLCC`)

All `NodeScope`; all `Filtered` except the four `enabled_ciphers`/`enabled_protocols` lists.

| Key group | Keys | Default | Where |
|---|---|---|---|
| HTTP keystore | `searchguard.ssl.http.{clientauth_mode,keystore_alias,keystore_filepath,keystore_password,keystore_keypassword,keystore_type,truststore_alias,truststore_filepath,truststore_password,truststore_type}` | string | `SSLP:270-279` |
| Enable flags | `searchguard.ssl.http.enabled` / `searchguard.ssl.transport.enabled` | bool `true` / `true` (`SSLCC:34,50`) | `:280-283` |
| Hostname verification | `searchguard.ssl.transport.enforce_hostname_verification` / `.resolve_hostname` | bool `true` | `:284-287` |
| Transport keystore | `searchguard.ssl.transport.{keystore_alias,keystore_filepath,keystore_password,keystore_keypassword,keystore_type,truststore_alias,truststore_filepath,truststore_password,truststore_type}` | string | `:288-296` |
| Ciphers/protocols | `searchguard.ssl.{http,transport}.enabled_ciphers` / `.enabled_protocols` | list `[]` (not filtered) | `:297-304` |
| External context / extractor | `searchguard.ssl.client.external_context_id`, `searchguard.ssl.transport.principal_extractor_class` | string | `:305-307` |
| PEM transport | `searchguard.ssl.transport.{pemcert_filepath,pemkey_filepath,pemkey_password,pemtrustedcas_filepath}` | string | `:309-313` |
| PEM HTTP | `searchguard.ssl.http.{pemcert_filepath,pemkey_filepath,pemkey_password,pemtrustedcas_filepath}` | string | `:315-318` |
| CRL | `searchguard.ssl.http.crl.file_path` (string), `.validate` `false`, `.prefer_crlfile_over_ocsp` `false`, `.check_only_end_entities` `true`, `.disable_crldp` `false`, `.validation_date` long `-1` | | `:320-327` |
| Demo certs | `searchguard.allow_unsafe_democertificates` | bool `false` | `:328-329` |

## Mixed-cluster and compatibility relevance

| Concern | Detail |
|---|---|
| Transport headers between nodes | `SearchGuardInterceptor` forwards `_sg_user_header`, `_sg_origin_header`, `_sg_remote_address_header` (`ConfigConstants.java:28,59,68`; the un-suffixed `_sg_user`, `_sg_origin`, `_sg_remote_address` are node-local transients; full list in wire-surface.md §4), DLS/FLS query/field/mode/filter-level headers and the diagnostic action stack (`SearchGuardInterceptor.java:149-166,211`); receiving nodes trust them only for inter-cluster or trusted-cluster requests (`SearchGuardRequestHandler.java:162-170`). Any change to header names or serialisation (`Base64Helper.serializeObject(user)`, `SGF:205`) breaks mixed-version clusters. The `_sg_user_header` header is now also set locally for `SearchRequest` (`SGF:204-206`) because of the ES 9.5.2 chunked fetch phase — nodes on older SG builds would not set it |
| Inter-cluster recognition | `DefaultInterClusterRequestEvaluator` compares certificate OID `1.2.3.4.5.5` / `searchguard.nodes_dn` (`DefaultInterClusterRequestEvaluator.java:42-44`); `searchguard.cert.intercluster_request_evaluator_class` can swap it per node, so nodes with differing settings may classify each other differently |
| Transport actions registered per node | Core: 33 explicit + 2 lists (`SGP:459-495`); + Scheduler 2, Session 4+config, AuthToken 6+config, FeMT config (3) + migration (2) + `multitenancy/activate` + `femt:user/available_tenants/get` (`FeMultiTenancyConfigApi.java:48-54`), DlsFls config, Signals 35 (catalogue: wire-surface.md §1). A node where a module is disabled (`searchguard.modules.disabled`) or absent (community build → `ClassNotFoundException` → warn, `REG:141`) does **not** register that module's `TransportAction`s; a request routed to it fails with "no handler for action". Signals additionally gates on `signals.enabled` (`SignalsModule.java:116-121`) and `AuditLogModule` on `searchguard.audit.type` (`AuditLogModule.java:55`) |
| Config validation | `ConfigModificationValidator`s run only on the node that receives the config write (`ConfigurationRepository.java:811,936,1028,1179`); a node without `dlic-fe-multi-tenancy` skips `FeMultiTenancyEnabledFlagValidator` |
| Index-level enforcement | `DlsFlsDirectoryReaderWrapper`, `FlsFieldFilter`, query cache provider and `ReadLogDirectoryReaderWrapper` are installed per node in `onIndexModule` (`SGP:508-608`). A data node missing `dlic-dlsfls` serves unfiltered shards even if the coordinating node computed restrictions |
| Signals enterprise actions | `jira`/`pagerduty` handlers exist only where `dlic-signals` is on the classpath and `signals.enterprise.enabled` (`Signals.java:121-123`); a watch using them fails validation on other nodes |
| Capabilities | `SearchGuardCapabilities` aggregates module capabilities cluster-wide via a cluster-state listener (`SearchGuardCapabilities.java:77-87`) — the mechanism by which the frontend learns about heterogeneous nodes |
| Settings | All `searchguard.*` are node-scope, so every node must carry identical `elasticsearch.yml` for consistent behaviour; nothing is reconcilable at runtime via cluster settings |
| Typed component fallback | `TypedComponentRegistry.create` accepts a fully-qualified class name as `type` (`TypedComponentRegistry.java:54-59`) — an undocumented extension point that loads arbitrary classes from the plugin classloader |

## Open questions for human review

1. `SearchGuardPlugin.getTransportInterceptors` (`SGP:634-665`) does not call `super`, so in normal mode `SearchGuardSSLTransportInterceptor` is not registered; TLS principal extraction then relies on `SearchGuardRequestHandler extends SearchGuardSSLRequestHandler` (`SearchGuardRequestHandler.java:54`). Intentional, but worth a comment.
   - **Answer:** _pending_
2. Four registry getters never memoise (`REG:301-355`); `getSyncAuthorizationFilters` is called once from `getActionFilters` (`SGP:618`) so this is latent, but `getFieldFilters` is called on **every** `getFieldFilter` invocation (`SGP:1198`) — per index, per mapping/field-caps request. Cheap today (one module), but a loop over all modules each time.
   - **Answer:** _pending_
3. `PrivilegesEvaluationResult.with(ActionFilter)` / `ExtendedActionFilterChain` (`SGF:359-361`) has no producer in `src/main`. Dead extension point or reserved for a future module?
   - **Answer:** _pending_
4. `PASS_ON_FAST_LANE` is honoured only for pre-privilege filters (`SGF:329-332`); a post-privilege filter returning it is treated as `OK` by omission (`SGF:349-356`). Is that intended?
   - **Answer:** _pending_
5. `signals.index_names.settings` (`SignalsSettings.java:550`) is defined but not included in `getAvailableSettings` (`:586-589`); if set in `elasticsearch.yml` ES should reject it as unknown — UNVERIFIED.
   - **Answer:** _pending_
6. `searchguard.ssl.allow_client_initiated_renegotiation` is read (`SSLP:128`) but never registered via `getSettings` — UNVERIFIED whether this works at all on ES 9.
   - **Answer:** _pending_
7. `searchguard.compliance.history.read.watched_fields` is registered twice: as a plain `Setting` (`SGP:1080`) and as a `StaticSettings.Attribute` (`AuditLogConfig.java:54`); only the former reaches ES. Harmless unless the attribute is ever added to a module `AttributeSet` (duplicate registration would fail).
   - **Answer:** _pending_
8. `moduleRegistry.getActions()` is appended even when `searchguard.disabled=true` (`SGP:498`); the registry is empty in that mode, but the asymmetry with `getRestHandlers` (`:407`) is fragile.
   - **Answer:** _pending_
9. The enterprise module list is an `ImmutableSet` (`SGP:346`); the registration order therefore depends on `ImmutableSet` preserving insertion order — UNVERIFIED against the `fluent-collections` implementation. Order matters for `getAuditLog()` (first non-null) and for filter ordering.
   - **Answer:** _pending_
10. ES interface ownership (§1) was verified against ES 9.4.4, not the pinned 9.5.5.
   - **Answer:** _pending_
