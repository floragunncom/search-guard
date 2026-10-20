# Test infrastructure inventory

*Inventory pass, generated 2026-10-19 on branch experimental-analysis at commit 374449897. Every claim carries a file reference. Items marked UNVERIFIED were inferred, not read.*

## Summary for maintainers

*Written for human readers. The tables below are the authoritative record; this section is a guide to them and must be re-read whenever `LocalCluster`, the external-process cluster classes, surefire configuration or the CI test jobs change.*

**What this covers.** The test harness and its two cluster modes, the older legacy harness, how many tests each module has and which harness they use, the surefire and CI configuration, whether anything tests upgrades, and the integration tests that live in other repositories.

**What you need to know.**

- Nothing tests mixed versions, rolling restarts or wire compatibility. Every cluster starts all nodes from one Elasticsearch version and one plugin zip. There is a way to stop a node but none to start it again. The only compatibility test deserializes a single message captured on ES 8.18.
- The embedded cluster loads only the plugin modules on the test module's classpath, so enterprise features may be silently missing. The external-process cluster runs a real Elasticsearch, but only 22 test classes force it, and the CI switch that would enable it for everything is never set anywhere in the repository.
- About 246 test classes exist. Around 108 use the current harness. The legacy harness is still used by 19 files in the enterprise security and audit-log modules, and the SSL module keeps private copies of it.
- Surefire retries a failing test three times, except in the DLS/FLS module. A flaky test that passes on retry counts as a pass and is only visible as a flake count in the report.
- The document sketches the shortest path to a mixed-version test: a version and plugin per node, a restart operation built from the existing stop and start, and one test that rolls a cluster from version A to B.

**What to review.**

- Section 5, the proposed path to a mixed-version test. Whether to build it is a planning decision and the most valuable single item in all six documents.
- Section 4.2, whether the external-cluster switch is set as a GitLab variable (open question 2), and whether the external integration-test repository covers upgrades (open question 3).
- Section 3, the counts are file-name heuristics, not surefire output.

## Scope and method

Read-only pass over `security/src/test` (harness), `security-legacy`, `ssl/src/test`, every module's `pom.xml`, `.gitlab-ci.yml`, `.gitlab-ci-branch-specific.yml`, `ci/*.yml`, `ci/run_tests.sh`, `ci/resource-diagnostics.sh` and `setup_test_instance.sh`.
Counts in section 3 were produced with the grep/find commands shown there; they are file-name heuristics (`*Test.java`, `*Tests.java`, `*IT.java`), not surefire output.
`audit/`, `target/` and the absent `sgi8/` checkout were ignored. No build or test was run.
CLAUDE.md was verified; two drifts are noted in section 8 (ES version, `TEST_CLUSTER_TYPE` comment).

## 1. Test harness classes

### 1.1 `LocalCluster` (security module)

| Item | Fact | Reference |
|---|---|---|
| Class | `LocalCluster extends ExternalResource implements AutoCloseable, EsClientProvider`; JUnit4 rule, 1135 lines | `security/src/test/java/com/floragunn/searchguard/test/helper/cluster/LocalCluster.java:146` |
| Mode switch | `externalProcessCluster = builderFlag || "true".equals(System.getProperty("sg.tests.use_ep_cluster"))` | `LocalCluster.java:148`, `:208` |
| Default-disabled modules | `AuthTokenModule`, `SignalsModule` are disabled unless `enableModule(...)` | `LocalCluster.java:156-157`, `:633` |
| Default init dir | `sg.default_init.dir` forced to `./sgconfig` of the test module | `LocalCluster.java:152-154`; consumer `security/src/main/java/com/floragunn/searchguard/configuration/ConfigurationRepository.java:557-559` |
| Start order | `before()` starts `dependsOn` clusters, injects `cluster.remote.<name>.seeds` for `remote(...)`, then `start()` | `LocalCluster.java:213-230` |
| Dispatch | `start()` -> `startExternalProcessEsCluster()` or `startEmbeddedEsCluster()`, then `waitForComponents()` | `LocalCluster.java:340-353` |
| Teardown | single idempotent `destroyCluster()`; comment explains `reuseForks=true` leak hazard | `LocalCluster.java:355-376` |
| Embedded subclass | `LocalCluster.Embedded` exposes `getInjectable`, config-index name via `ConfigurationRepository` | `LocalCluster.java:990`, `:1028-1031` |

Builder options (`LocalCluster.Builder`, `LocalCluster.java:525`):

| Builder method | Effect | Line |
|---|---|---|
| `sslEnabled()` / `sslEnabled(TestCertificates)` | generated PEM certs; without it JKS resources are used | `:546`, `:553` |
| `dependsOn(Object)` / `remote(name, cluster)` | start-order dependency; CCS remote seed wiring | `:558`, `:638` |
| `resources(folder)` | classpath folder for `sg_*.yml` | `:566` |
| `clusterConfiguration(...)` / `singleNode()` | node topology (see enum below) | `:572`, `:577` |
| `doNotInitializeSgConfig()` / `sgConfig(TestSgConfig)` | skip or replace config seeding | `:582`, `:587` |
| `ignoreUnauthorizedIndices`, `authzDebug`, `roleMappingResolutionMode`, `nodeSettings(...)` | node settings overrides | `:592-621` |
| `enterpriseModulesEnabled()` / `disableModule` / `enableModule` | writes `searchguard.enterprise_modules_enabled` and `searchguard.modules.disabled` | `:623-636`, `:790-796` |
| `indices`, `dataStreams`, `aliases`, `componentTemplates`, `indexTemplates` | fixtures created after start | `:644-668` |
| `users`, `user`, `roles`, `roleMapping`, `roleToRoleMapping`, `authc`, `frontendMultiTenancy`, `tenants`, `dlsFls`, `authTokenService`, `var` | delegate into `TestSgConfig` | `:678-745` |
| `clusterName`, `logRequests`, `waitForComponents(...)` | naming; request log on teardown; component-state wait | `:747`, `:756`, `:766` |
| `embedded()` | returns `Builder.Embedded` (adds `plugin(Class)`, `configIndexName`) | `:752`, `:800-816` |
| `useExternalProcessCluster()` | forces external mode for this cluster | `:761-764` |
| `build()` / `start()` | `preBuild()` appends `_<n>` to cluster name | `:771-798` |

Topologies (`ClusterConfiguration.java:29-38`): `DEFAULT` 1 master + 2 data; `SINGLENODE`; `CLIENTNODE` 1m+2d+1 coordinating; `THREE_MASTERS` 3m+2d.

### 1.2 The two cluster modes

| Aspect | JVM-embedded (`JvmEmbeddedEsCluster`) | External process (`ExternalProcessEsCluster`) |
|---|---|---|
| Node type | `PluginAwareNode extends Node` in the test JVM | real `bin/elasticsearch` process per node |
| Where | `JvmEmbeddedEsCluster.java:195`; `security/src/test/java/org/elasticsearch/node/PluginAwareNode.java:37-50` | `EsInstallation.java:145-185` |
| Plugins loaded | `SearchGuardPlugin` + `MainRestPlugin` + Painless (if on classpath) + `Netty4`, `Mustache`, `ParentJoin`, `Percolator`, `Reindex` + test `plugin(Class)` additions | full ES distribution + the SG plugin zip |
| | `security/src/test/java/org/elasticsearch/plugins/SgAwarePluginsService.java:24-29`, `:48-67` | `ExternalProcessEsCluster.java:109` |
| SG modules | only those on the test module's Maven test classpath (javadoc) | all modules in the zip; `-Pquick` adds every `dlic-*` module |
| | `LocalCluster.java:140-142` | `plugin/pom.xml:240-262` |
| ES features missing | data streams rejected explicitly; no x-pack modules (SQL, ESQL, transform, downsample, LogsDB, failure store) since only the plugins above are loaded | `xpack.security.enabled: false`, geoip downloader off, ILM/SLM history off |
| | `LocalCluster.java:393-398`; `SgAwarePluginsService.java:24-29` | `ExternalProcessEsCluster.java:114-120` |
| Bootstrap checks | `es.enforce.bootstrap.checks=true` | ES defaults |
| | `JvmEmbeddedEsCluster.java:62-64` | - |
| Heap | surefire fork `-Xmx4g` shared by all nodes | `-Xms1g -Xmx1g` per node appended to `jvm.options` |
| | `pom.xml:824` | `ExternalProcessEsCluster.java:111-112` |
| Config seeding | `TestSgConfig.initIndex(Client)` via node client | `testSgConfig.initByConfigRestApi(client)` after SG init log line |
| | `LocalCluster.java:465-470`; `TestSgConfig.java:337-372` | `ExternalProcessEsCluster.java:385` |
| Readiness | cluster health GREEN, 60 s | log line "Search Guard configuration has been successfully initialized", then poll until no 503; `STARTUP_TIMEOUT` 3 min; GREEN via `/_cluster/health?wait_for_status=green&timeout=30s` |
| | `JvmEmbeddedEsCluster.java:106-111` | `ExternalProcessEsCluster.java:78`, `:394-428`, `:464` |
| Node client / injectables | yes (`getInternalNodeClient`, `getInjectable`) | no, REST only |
| | `JvmEmbeddedEsCluster.java:232`; `LocalCluster.java:1028` | - |
| Per-node stop | `Node.stop()` exists in both; no per-node (re)start API | `LocalEsCluster.java:289-298` |

Shared base `LocalEsCluster` (`LocalEsCluster.java:42`): port allocation keyed by surefire `forkno` (`:96-104`, `:279-287`), masters started before non-masters (`:110-116`), up to 10 retries on port collision (`:257-271`).

### 1.3 How external mode obtains ES and the plugin

| Step | Fact | Reference |
|---|---|---|
| ES version | `org.elasticsearch.Version.CURRENT.toString()` of the test classpath, i.e. the single `elasticsearch.version` of the build | `ExternalProcessEsCluster.java:191-193`; `pom.xml:49` |
| Download cache | `-Dsg.tests.es_download_cache.dir`, default `~/searchguard-test/download-cache/` (same scheme as `setup_test_instance.sh`) | `EsDownload.java:64-68`; `setup_test_instance.sh:5-6` |
| Archive | `elasticsearch-<ver>-<os>-<arch>.tar.gz` from `https://artifacts.elastic.co/downloads/elasticsearch/` | `EsDownload.java:71-87` |
| Extract | `tar xfz ... --strip-components 1` into `<clusterHome>/es`, one installation per cluster, retried on truncated archives | `EsDownload.java:139-140`, `:151-158`; `ExternalProcessEsCluster.java:95`, `:102` |
| `EsDownload` registry | `instancesByVersion` map, `get(version)` - version-keyed, but only ever called with `Version.CURRENT` | `EsDownload.java:40`, `:50`; `ExternalProcessEsCluster.java:102` |
| Plugin zip | `-Dsg.tests.sg_plugin.file=...`; otherwise an in-process `MavenCli` run of `install -DskipTests=true -Pquick` and the first `plugin/target/releases/search-guard-flx-elasticsearch-plugin*` file | `SgPluginPackage.java:35-46`, `:83-95` |
| Plugin singleton | one static `CompletableFuture<SgPluginPackage>` per JVM - one plugin build/zip per test run | `SgPluginPackage.java:11-12` |
| Install | `bin/elasticsearch-plugin install -v -b file:///<zip>` | `EsInstallation.java:58-62` |
| Node launch | `bin/elasticsearch -Ehttp.port -Etransport.port -E<setting>... -Enode.roles=...` | `EsInstallation.java:158-179` |
| Certificates | `TestCertificates.at(configDir)` copies PEMs into ES config | `ExternalProcessEsCluster.java:129` |

### 1.4 System properties

| Property | Read in | Effect |
|---|---|---|
| `sg.tests.use_ep_cluster` | `LocalCluster.java:148` | `true` forces external mode for every `LocalCluster`; `LocalCluster.Embedded` is immune because its `start()` override always calls `startEmbeddedEsCluster()` (`LocalCluster.java:1003-1005`), even though its constructor passes `true` for `externalProcessCluster` (`:999`) |
| `sg.tests.sg_plugin.file` | `SgPluginPackage.java:35` | use prebuilt zip, skip MavenCli |
| `sg.tests.es_download_cache.dir` | `EsDownload.java:64` | ES archive cache dir |
| `forkno` | `LocalEsCluster.java:280`; set in `pom.xml:830` | port range per surefire fork |
| `sg.default_init.dir` | set `LocalCluster.java:153`; read `ConfigurationRepository.java:557` | default `sg_*.yml` directory |

### 1.5 Supporting helpers

| Helper | What it is | Reference |
|---|---|---|
| `TestSgConfig` (2060 lines) | Java builder for the SG config index; `initIndex` writes `roles`, `internalusers`, `rolesmapping`, `actiongroups`, `tenants`, `blocks`, `frontend_authc`, `frontend_multi_tenancy`, `authc`, `authz`, `sessions`, `authz_dlsfls` from YAML resources or builders | `security/src/test/java/com/floragunn/searchguard/test/TestSgConfig.java:116`, `:337-372` |
| nested builders | `UserPassword :649`, `User :683`, `TenantPermission :825`, `Role :857`, `RoleMapping :991`, `IndexLikePermission :1052`, `Tenant :1124`, `Authc :1171` (+`Domain :1199`, `UserMapping :1373`), `FrontendAuthc :1535`, `FrontendMultiTenancy :1620`, `DlsFls :1662`, `JwtDomain :1700`, `AuthTokenService :1825`, `AuthFailureListener :1925`, `Authz :1955`, `Sessions :1995` | same file |
| `GenericRestClient` | Apache HttpClient wrapper: `get/head/options/put/putJson/putNdJson/delete/post/postJson/postSql/patch`, `HttpResponse.getBodyAsDocNode()`, `trackResources()`, `deleteWhenClosed()` | `security/src/test/java/com/floragunn/searchguard/test/GenericRestClient.java:102-271`, `:319-336`, `:501` |
| `TestCertificates` | generates CA/node/client/admin certs at runtime; `builder().ca().addNodes().addClients().addAdminClients()`; `at(dir)` writes PEMs; `getSgSettings()` | `security/src/test/java/com/floragunn/searchguard/test/helper/certificate/TestCertificates.java:103-108`, `:134-241`; factory `TestCertificateFactory.java` (186 lines) |
| JKS fallback | when no `TestCertificates`, `node-0-keystore.jks`/`truststore.jks` from resources, admin DN `CN=kirk,...` | `MinimumSearchGuardSettingsSupplierFactory.java:57-69` |
| `TestCertificateBasedSSLContextProvider` | client SSLContext from generated certs | `.../helper/cluster/TestCertificateBasedSSLContextProvider.java:9-20` |
| `TestIndex`, `TestDataStream`, `TestAlias`, `TestIndexTemplate`, `TestComponentTemplate`, `TestData` | fixture builders; `TestData` deterministic with `DEFAULT_SEED=1234` | `security/src/test/java/com/floragunn/searchguard/test/TestData.java:60-67` |
| `ClusterHelper` | older embedded starter used by the legacy harness and the scheduler; `startCluster`, `stopCluster`, `waitForCluster` | `.../helper/cluster/ClusterHelper.java:100-105`, `:239`, `:295` |
| `LocalClusterTest` | tests the harness itself (config backup/restore) on an embedded cluster | `.../helper/cluster/LocalClusterTest.java:42-86` |

### 1.6 The scheduler's unrelated `LocalCluster`

| Fact | Reference |
|---|---|
| `com.floragunn.searchsupport.jobs.LocalCluster extends ExternalResource`, wraps the security module's `ClusterHelper("lc_utest_n", 0)`; embedded only, no builder, no external mode | `scheduler/src/test/java/com/floragunn/searchsupport/jobs/LocalCluster.java:14-17`, `:19-28` |
| Depends on `search-guard-flx-security:tests` | `scheduler/pom.xml:58-59` |
| Used by 1 test class (`JobExecutionEngineTest`, UNVERIFIED by name only) | section 3 count |

## 2. Legacy harness (`security-legacy`)

| Item | Fact | Reference |
|---|---|---|
| Artifact | `search-guard-flx-legacy-test-framework`, test sources only | `security-legacy/pom.xml:19`; tree has no `src/main` |
| `AbstractSGUnitTest` | JUnit rules (`TestName`, `TemporaryFolder`, `SGTestWatcher`), JKS-based minimum settings, `initialize(client, DynamicSgConfig)` | `security-legacy/src/test/java/com/floragunn/searchguard/legacy/test/AbstractSGUnitTest.java:63`, `:88-95`, `:107`, `:155-159` |
| `SingleClusterTest` | `ClusterHelper("utest_n",0)`; `setup(...)` overloads; second `ClusterHelper("crl2_n",1)` for CCS; `restHelper()` | `.../SingleClusterTest.java:37`, `:40-115`, `:78-82`, `:118` |
| `DynamicSgConfig` | names of `sg_*.yml` files to load into the index (`sg_roles.yml`, `sg_roles_tenants.yml`, ...), `getDynamicConfig(folder)` -> `IndexRequest`s | `.../DynamicSgConfig.java:33-39`, `:84` |
| Other classes | `RestHelper`, `SGTestWatcher` | `security-legacy/src/test/java/.../legacy/test/` |
| Consumers (pom) | `dlic-security/pom.xml:323-324`, `dlic-auditlog/pom.xml:73-74` | - |
| `ssl` module | has its *own* copies: `ssl/.../ssl/test/SingleClusterTest.java` (125 lines), `AbstractSGUnitTest.java` (118), `helper/cluster/ClusterHelper.java` (409), `org/elasticsearch/node/PluginAwareNode.java` (90); no dependency on `security-legacy` | `ssl/pom.xml:60-61` (depends only on `support:tests`) |

Users (command: `grep -rl "import com.floragunn.searchguard.legacy.test\|extends SingleClusterTest\|extends AbstractSGUnitTest" --include='*.java' <module>/src/test`):

| Module | Files | Names |
|---|---|---|
| dlic-security | 10 | `dlic/rest/api/{AbstractRestApiUnitTest, GetConfigurationApiTest, IndexMissingTest, RoleBasedAccessTest, FlushCacheApiTest, ActionGroupsApiTest, LicenseTest, RolesMappingApiTest, TenantsApiTest, RestApInfoEndpointTest}` |
| dlic-auditlog | 9 | `httpclient/HttpClientTest`, `auditlog/{AbstractAuditlogiUnitTest, impl/TracingTests, impl/AuditlogTest, impl/DisabledCategoriesTest, integration/SSLAuditlogTest, integration/BasicAuditlogTest, compliance/RestApiComplianceAuditlogTest, compliance/ComplianceAuditlogTest}` |
| ssl | 3 | `BksTest`, `SSLTest` (via ssl's own `SingleClusterTest`) |
| security, dlic-dlsfls, dlic-fe-multi-tenancy, dlic-signals, signals, scheduler, support | 0 | - |

## 3. Test inventory per module

Commands (run from repo root, zsh):

```
for m in security security-legacy ssl support scheduler signals dlic-security dlic-dlsfls dlic-auditlog dlic-fe-multi-tenancy dlic-signals; do
  T=$(find $m/src/test \( -name '*Test.java' -o -name '*Tests.java' -o -name '*IT.java' \) | wc -l)
  L=$(grep -rlE "LocalCluster(\.Embedded)?\b" --include='*.java' $m/src/test | grep -E 'Test(s)?\.java$|IT\.java$' | wc -l)
  G=$(grep -rlE "extends (SingleClusterTest|AbstractSGUnitTest)" --include='*.java' $m/src/test | grep -E 'Test(s)?\.java$' | wc -l)
  E=$(grep -rl "useExternalProcessCluster()" --include='*.java' $m/src/test | grep -E 'Test(s)?\.java$' | wc -l)
  S=$(grep -rl "@RunWith(Suite.class)" --include='*.java' $m/src/test | wc -l)
  P=$(grep -rl "@RunWith(Parameterized.class)" --include='*.java' $m/src/test | wc -l)
  EM=$(grep -rl "\.embedded()" --include='*.java' $m/src/test | grep -E 'Test(s)?\.java$' | wc -l)
  echo "$m $T $L $G $E $S $P $EM"; done
```

| Module | Test classes | Use `LocalCluster` | Legacy `extends` | Force external | `Suite` | `Parameterized` | Call `.embedded()` | Plain unit (derived) |
|---|---|---|---|---|---|---|---|---|
| security | 81 | 49 | 0 | 13 | 5 | 13 | 21 | ~32 |
| security-legacy | 2 (harness classes, not tests) | 0 | 1 | 0 | 0 | 0 | 0 | 0 |
| ssl | 6 | 0 | 3 (own copy) | 0 | 0 | 0 | 0 | 3 |
| support | 6 | 1 | 0 | 1 | 0 | 0 | 0 | 5 |
| scheduler | 5 | 1 (scheduler's own) | 0 | 0 | 0 | 0 | 0 | 4 |
| signals | 31 | 15 | 0 | 0 | 0 | 3 | 14 | ~16 |
| dlic-security | 35 | 17 | 1 (+9 via helper base) | 2 | 0 | 1 | 9 | ~8 |
| dlic-dlsfls | 23 | 13 | 0 | 6 | 3 | 7 | 5 | ~10 |
| dlic-auditlog | 23 | 1 | 3 (+6 via helper base) | 0 | 0 | 0 | 1 | ~13 |
| dlic-fe-multi-tenancy | 32 | 10 | 0 | 0 | 0 | 4 | 10 | ~22 |
| dlic-signals | 2 | 2 | 0 | 0 | 0 | 0 | 2 | 0 |

"Plain unit (derived)" = test classes minus cluster users; approximate because helper/base classes match the name filter. Legacy "+n via helper base" counts files that import the legacy package through an abstract base (section 2 table).

Classes forcing `useExternalProcessCluster()` (22, excluding `LocalCluster.java` itself): `security`: `authz/IgnoreUnauthorizedCcsWithDisabledSkipUnavailableRemoteClusterIntTest`, `authz/int_tests/{AsyncSqlIntTests, DataStreamAddRemoveIndicesTest, DataStreamAuthorizationReadOnlyIntTests, DataStreamAuthorizationReadWriteIntTests, DataStreamDownsampleIntTest, DataStreamFailureStoreAuthorizationReadOnlyIntTests, DataStreamFailureStoreAuthorizationReadWriteIntTests, DataStreamFailureStoreSpecificOperationsIntTests, DataStreamsFailureStoreIntTests, EsqlViewIntTest, SqlAuthorizationReadOnlyIntTests, TransformIntTest}`; `dlic-dlsfls`: `{DlsIntTest, DlsTest, FlsIntTest, FmIntTest, FmLogsDbIntTest, int_tests/DlsReadOnlyIntTests}`; `dlic-security`: `authtoken/{AuthTokenDlsIntTest, AuthTokenIntegrationTest}`; `support`: `meta/MetaIntTest`. These are the data-stream / failure-store / SQL / ESQL / transform / downsample / LogsDB cases the embedded cluster cannot run (section 1.2).

`@RunWith(Suite.class)` classes with nested suite classes (8): `dlic-dlsfls/.../{RoleBasedDocumentAuthorizationTest, RoleBasedFieldAuthorizationTest, RoleBasedFieldMaskingTest}`; `security/.../authz/RoleBasedActionAuthorizationTests`, `authz/int_tests/{BasicRoleMappingIntTests, DataStreamFailureStoreSpecificOperationsIntTests, SqlAuthorizationReadOnlyIntTests}`, `int_tests/FoundationalTests`. The `security` pom excludes `**/*$*` so inner classes are not run twice (`security/pom.xml:130-131`); other modules have no such exclude (UNVERIFIED whether that matters for the dlic-dlsfls suites).

## 4. Surefire and CI configuration

### 4.1 Surefire

| Setting | Value | Reference |
|---|---|---|
| Plugin version | 3.6.0 (pluginManagement, inherited) | `pom.xml:819-820` |
| `argLine` | `-Xmx4g -Xms4g` + `--add-opens` list | `pom.xml:824-825` |
| `rerunFailingTestsCount` | 3 | `pom.xml:826` |
| `forkCount` / `reuseForks` | 3 / true | `pom.xml:827-828` |
| `forkno` system property | `fork_${surefire.forkNumber}` | `pom.xml:830` |
| includes | `**/*.java` | `pom.xml:833-835` |
| `trimStackTrace` | false | `pom.xml:836` |
| dlic-dlsfls override | `-Xmx3072m`, `rerunFailingTestsCount=0`, forkCount 3 | `dlic-dlsfls/pom.xml:79-92` |
| signals override | `forkCount=2` (comment: multi-node embedded clusters starve each other) | `signals/pom.xml:151-158` |
| security excludes | `**/SslHostnameVerificationTest.java`, `**/*$*` | `security/pom.xml:125-134` |
| security profile `test-ssl-hostname-verification` | includes only `SslHostnameVerificationTest`, clears excludes | `security/pom.xml:139-157` |
| tests classifier | `maven-jar-plugin` 3.5.1 `test-jar` goal in pluginManagement, applied to every module | `pom.xml:781-782`, `:799-804` |

### 4.2 Unit-test CI

| Item | Fact | Reference |
|---|---|---|
| Build image / default timeout | `floragunncom/ubuntu-openjdk25:251126`; 90 m | `.gitlab-ci.yml:14`, `:27` |
| `ES_VERSION` | parsed from `pom.xml` (no tag) or from tag `sg-flx-<sg>-es-<es>` | `.gitlab-ci.yml:37-56` |
| `build` job | `mvn ... -Delasticsearch.version=$ES_VERSION -DskipTests clean deploy`; artifacts `plugin/target/releases/` + `snapshot.env` (dotenv) | `.gitlab-ci.yml:84-116` |
| `.be_unit_test` template | stage "Backend Unit Tests", timeout 15 m, retry 2, tag `test`, skipped on `DOCKER_ONLY`/`SKIP-UT`/`SKIP-TESTS` | `ci/backend-unit-tests.yml:1-11` |
| Reports | JUnit from `*/target/surefire-reports/TEST-*.xml` and `failsafe-reports`; `ci-diagnostics/` kept always, 1 week | `ci/backend-unit-tests.yml:12-21` |
| Caches | `.m2/repository` (pull), `.es-download` keyed `es-download` (pull-push) | `ci/backend-unit-tests.yml:24-34` |
| Jobs | `test_security` (30 m), `test_security_SslHostnameVerificationTest`, `test_ssl`, `test_signals`, `test_scheduler`, `test_support`, `test_dlic_security`, `test_dlic_dlsfls`, `test_dlic_femt`, `test_dlic_auditlog`, `test_dlic_signals` | `ci/backend-unit-tests.yml:36-104` |
| No job | `security-legacy` (no tests of its own), `plugin` | - |
| `ci/run_tests.sh` | `mvn -pl $MODULE test` with `-Dsg.tests.es_download_cache.dir`, `-Dsg.tests.sg_plugin.file=plugin/target/releases/*SNAPSHOT*.zip`, `-Drevision`, `-Delasticsearch.version`; optional `-P $PROFILE`; runs as `es_test` via `su` | `ci/run_tests.sh:13-26`, `:41` |
| `TEST_CLUSTER_TYPE` | `full` -> `-Dsg.tests.use_ep_cluster=true`, else `false`; **not assigned anywhere in the repo** (`grep -rnE "^\s*TEST_CLUSTER_TYPE\s*:" .gitlab-ci*.yml ci/*.yml` -> no hits) | `ci/run_tests.sh:28-32` |
| Diagnostics | `resource-diagnostics.sh` sourced; `diag_init/snapshot/sampler`; docker section only in dind jobs | `ci/resource-diagnostics.sh:1-21`; `ci/run_tests.sh:10-11`, `:34-46` |
| SslHostnameVerificationTest job | same template, `ci/run_tests.sh security test-ssl-hostname-verification`; split because its log exceeded the GitLab limit | `ci/backend-unit-tests.yml:43-49` |

## 5. Mixed-version and upgrade testing

**Result: nothing in this repository starts nodes of two ES versions or two plugin versions, performs a rolling restart/upgrade, or exercises wire compatibility between versions.**

Searches performed (test sources of all modules, `ci/`, `dev/`, `plugin/`, `pom.xml`, `setup_test_instance.sh`):

```
grep -rniE "rolling|mixed[-_ ]?version|mixed[-_ ]?cluster|\bupgrade|\bbwc\b|backward[s]?[ -]?compat|wire[ -]?compat|oldVersion|previous[ -]?version|minimumCompat" ...
grep -rnE "TransportVersion|Version\.V_[0-9]|Version\.fromString|Version\.CURRENT" --include='*.java' */src/test
grep -rnE "restartNode|restart\(|stopNode|startNode\(" --include='*.java' */src/test
grep -rnE "elasticsearch\.version|ES_VERSION" --include='*.xml' --include='*.yml' --include='*.sh' .
```

| Finding | What it is | Reference |
|---|---|---|
| Single ES version per build | one `elasticsearch.version` (9.5.5); CI passes exactly that value | `pom.xml:49`; `.gitlab-ci.yml:39-41`, `:98` |
| External cluster version | hard-wired to `Version.CURRENT` of the test classpath | `ExternalProcessEsCluster.java:191-193` |
| One installation per cluster | `esDir = <clusterHome>/es`, extracted once, all nodes launched from it | `ExternalProcessEsCluster.java:95`, `:102`, `:146-150` |
| One plugin per JVM | `SgPluginPackage` static singleton; `sg.tests.sg_plugin.file` takes one path | `SgPluginPackage.java:11-12`, `:35` |
| Node stop exists, restart does not | `LocalEsCluster.Node.stop()`; `startNode(...)` is protected and only used by `startNodes` during cluster start | `LocalEsCluster.java:83`, `:140-150`, `:297` |
| Only test that stops a node | `SignalsStressTests` stops one embedded node and asserts a watch moves; same version, no restart | `signals/src/test/java/com/floragunn/signals/SignalsStressTests.java:61-65`, `:142` |
| Only wire-compat test | `SearchAuthTokensRequestTest.deserializationBackwardsCompatibilityTest`: a base64 blob serialized by SG 3.1.0 on ES 8.18 is deserialized by the current `SearchAuthTokensRequest` (checks `Scroll` -> `TimeValue` change) | `dlic-security/src/test/java/com/floragunn/searchguard/authtoken/api/SearchAuthTokensRequestTest.java:30-43` |
| `TransportVersion` in tests | none; `Version.CURRENT` appears only in the harness and in `AuditTestToXContentObjectImplTest` | `dlic-auditlog/.../impl/AuditTestToXContentObjectImplTest.java:52` |
| "upgrade" hits | Kibana saved-object/MT index migration tests (`MultiTenancyMigrationTest`, `datamigration880/*`, comment "Perform upgrade to version 8.7.0"); these migrate *data* on one running version | `dlic-fe-multi-tenancy/.../datamigration880/service/steps/MigrationStepsTest.java:205-215` |
| MT data-migration CI | downstream Kibana pipeline fed an archived data dir (`DATA_DIR_ARCHIVE_NAME`, e.g. `mt-test-data-9.0.1.tar.gz`) and the current plugin - data upgrade, not node upgrade | `ci/mt-data-migration-tests.yml:43-57`, `:60-87` |
| `EsDownload` | version-keyed registry; the only latent hook for a second version | `EsDownload.java:40`, `:50` |
| sgi8 | content unknown locally; job names (`community`, `enterprise_*`, `ccs_*`, `compliance_*`, `signals_*`) contain no upgrade/rolling group | `ci/backend-int-tests.yml:2-79` |

What the external process cluster would need for a mixed-version test (all UNVERIFIED as design, derived from the code above):

| Gap | Change |
|---|---|
| Version per node | move `esVersion`/`EsInstallation` from cluster scope (`ExternalProcessEsCluster.java:95-109`) to `NodeSettings` (`ClusterConfiguration.java:68-78`) and call `EsDownload.get(version)` per node |
| Plugin per version | replace the `SgPluginPackage` singleton (`:11-12`) with a map version -> zip; accept several `sg.tests.sg_plugin.file.<ver>` paths or download released zips from `maven.search-guard.com` (as `docker/Dockerfile` does, UNVERIFIED) |
| Rolling restart | add `restartNode(name, newVersion)` to `LocalEsCluster`: `Node.stop()` (`:297`) then `startNode(...)` (`:83`) reusing the node's ports and `path.data` (`EsInstallation.java:150`, `:176`) and the already-computed `seedHosts` (`LocalEsCluster.java:105-106`) |
| Readiness | current readiness depends on the SG log line (`ExternalProcessEsCluster.java:394`); an upgraded node must be re-polled individually |
| CI | a job that passes `-Dsg.tests.use_ep_cluster=true` (today never set, section 4.2) and caches two archives (`.es-download` cache is filename-keyed, so this already works - `ci/backend-unit-tests.yml:29-33`) |

## 6. Integration tests outside this repo

| Job file | Pulls | Branch/ref | Groups / inputs | Trigger | Reference |
|---|---|---|---|---|---|
| `ci/backend-int-tests.yml` | `git.floragunn.com/private/sgi8.git` (depth 1) into `sgi8/`; copies `plugin/target/releases/...-${SNAPSHOT_REVISION}.zip` to `sgi8/server/docker/elasticsearch/search-guard.zip`; runs `sgi8/server/run_test_definition.sh "$TEST_DEFINITIONS"` | `IT_BRANCH` or `<ES major>.<minor>.x` from `ES_VERSION` (today `9.5.x`) | 13 jobs: `community`, `enterprise_1..3`, `ccs_1..3`, `compliance_1..3`, `compliance_ccs`, `signals_1..2` | MR pipelines, schedules, release tags `sg-flx-*`, or `FORCE_SGI`; skipped on `DOCKER_ONLY`, `SKIP-TESTS`, `SKIP-SGI`; dind image, tag `test-large`, 30 m, retry 2 | `:2-79`, `:81-99`, `:116-127`, `:133` |
| reports | JUnit `sgi8/client/test/testresults**/junit/Test*.xml`; heavy artifacts pruned on success unless `KEEP_TEST_ARTIFACTS` | - | - | - | `:138-158` |
| `ci/frontend-int-tests.yml` | `check_frontend_prerequisites` (manual) resolves snapshot URLs on `maven.search-guard.com` for the ES plugin (`${CI_COMMIT_REF_SLUG}-SNAPSHOT`) and the Kibana plugin (`b-<SG_KI_BRANCH>-SNAPSHOT`) via `ci/artifact_uri.sh`; `frontend_int_tests` triggers the child pipeline from `search-guard/search-guard-kibana-plugin` (`.gitlab-ci-branch-specific.yml`, `ci/frontend-int-tests.yml`) | `SG_KI_BRANCH` = `main` | `SG_ES_PLUGIN`, `SG_KI_PLUGIN` | manual prerequisite job gates it; skipped on tags | `:1-41`, `:44-69`; `.gitlab-ci-branch-specific.yml:2`; `ci/artifact_uri.sh:11-21` |
| `ci/mt-data-migration-tests.yml` | same prerequisite pattern plus `DATA_DIR_ARCHIVE_NAME` check against `maven.search-guard.com/test-data/com/floragunn/mt-test-data/`; triggers `ci/mt_data_migration_tests.yml` in the Kibana repo with `SF_VERSION=$ES_VERSION`, `KIBANA_DATA_DIR_ARCHIVE_NAME` | `SG_KI_BRANCH` | archived Kibana/MT data dir | manual job "Please set DATA_DIR_ARCHIVE_NAME" | `:1-57`, `:60-87` |
| `IT_BRANCH` override | commented placeholder only | - | - | - | `.gitlab-ci-branch-specific.yml:3-4` |
| release tag side effect | `deploy_release` tags sgi8 at `<major.minor>.x` with the same tag name | - | - | tags | `.gitlab-ci.yml:146-150` |

`FORCE_SGI`, `IT_BRANCH`, `KEEP_TEST_ARTIFACTS`, `DOCKER_ONLY` and `TEST_CLUSTER_TYPE` are read but never assigned in the repo; they must be GitLab project/pipeline variables (UNVERIFIED).

## 7. Test data and fixtures

Counts (`find <module>/src/test/resources -name "*.<ext>" | wc -l`):

| Module | yml | jks | p12 | pem | json | txt | `log4j2-test.properties` | Notable |
|---|---|---|---|---|---|---|---|---|
| security | 19 | 8 | 0 | 0 | 2 | 0 | yes | `sg_{action_groups,authc,internal_users,roles,roles_mapping,tenants}.yml`, dirs `frontend_config/`, `rest/`, `session/`, `sg-plugin-security.policy` |
| security-legacy | 28 | 10 | 2 | 16 | 10 | 0 | yes | `sg_roles_*.yml` variants, `sg_blocks*.yml`, `node-{0,1,2}-keystore.jks`, `kirk-keystore.jks`, `license{1,2,3}.json`, `truststore_fail.jks` |
| ssl | 0 | 6 | 8 | 10 | 0 | 0 | yes | keystores/PEMs for the TLS tests |
| support | 0 | 5 | 0 | 0 | 0 | 0 | yes | JKS only |
| scheduler | 0 | 5 | 0 | 0 | 0 | 0 | yes | JKS only |
| signals | 24 | 14 | 0 | 11 | 0 | 0 | yes | `sg_config/no-tenants` etc. (`SignalsStressTests.java:61`) |
| dlic-security | 51 | 26 | 0 | 4 | 45 | 16 | yes | largest fixture set (REST API, auth backends) |
| dlic-dlsfls | 3 | 0 | 0 | 0 | 0 | 0 | yes | config built in Java |
| dlic-auditlog | 22 | 5 | 3 | 13 | 9 | 0 | yes | - |
| dlic-fe-multi-tenancy | 8 | 4 | 0 | 0 | 0 | 0 | yes | mappings embedded in Java (`TestMappings.java`) |
| dlic-signals | 24 | 14 | 0 | 8 | 0 | 0 | yes | - |

| Sharing mechanism | Fact | Reference |
|---|---|---|
| Default config dir | `security/sgconfig/` (`sg_action_groups, sg_authc, sg_authz, sg_frontend_authc, sg_frontend_multi_tenancy, sg_internal_users, sg_roles, sg_roles_mapping, sg_tenants`, `elasticsearch.yml.example`) is what `sg.default_init.dir` points at; only `security` has such a directory | `LocalCluster.java:153`; `ls */sgconfig` |
| `tests` classifier | every module builds a test-jar (`pom.xml:799-804`); consumers: `security:tests` <- `scheduler/pom.xml:58`, `signals/pom.xml:133`, `security-legacy/pom.xml:70`, `dlic-security/pom.xml:308`, `dlic-dlsfls/pom.xml:40`, `dlic-auditlog/pom.xml:66`, `dlic-fe-multi-tenancy/pom.xml:79`, `dlic-signals/pom.xml:75`; `support:tests` <- `security/pom.xml:105`, `ssl/pom.xml:60` and all dlic modules; `signals:tests` <- `dlic-signals/pom.xml:82`; `legacy-test-framework:tests` <- `dlic-security/pom.xml:323`, `dlic-auditlog/pom.xml:73` | - |
| What travels | the harness classes, `PluginAwareNode`/`SgAwarePluginsService`, and (standard `test-jar` behaviour, UNVERIFIED per file) `src/test/resources` incl. JKS keystores and `sg_*.yml` of `security` | `pom.xml:802` |
| Runtime certs | `TestCertificates` generates PEMs per cluster, so most new tests carry no certificate files; 109 test files call `sslEnabled(`, 13 build custom `TestCertificates` | `grep -rl "sslEnabled(" */src/test`; `TestCertificates.java:134` |
| Legacy YAML | `DynamicSgConfig` loads `sg_*.yml` by name from the test module's resource folder | `DynamicSgConfig.java:33-39`, `:84` |
| Local dev cluster | `setup_test_instance.sh` builds `-Pquick`, downloads the pom's ES version into `~/searchguard-test`, installs the zip, copies `sg_*.yml` args into `plugins/search-guard-flx/sgconfig/` | `setup_test_instance.sh:14-60` |

## Mixed-cluster and compatibility relevance

Covered today: single-version clusters only. Both harness modes start every node from one ES version (`Version.CURRENT`, `ExternalProcessEsCluster.java:191-193`) and one plugin zip (`SgPluginPackage.java:11-12`). Node loss is exercised once (`SignalsStressTests.java:142`), never node return. One unit test checks that a request serialized on ES 8.18 still deserializes (`SearchAuthTokensRequestTest.java:35-43`). The Kibana-side MT data-migration pipeline upgrades *data*, not nodes.

Not covered: rolling restart or rolling upgrade of ES or of the plugin, mixed `TransportVersion` clusters, cluster-state/config-index compatibility across SG versions, CCS between different versions, and anything that depends on `sg.tests.use_ep_cluster=true` in CI (the switch exists but `TEST_CLUSTER_TYPE` is never set in the repo).

Shortest path to a first mixed-version test: external process mode is already a real multi-process cluster with per-node directories, ports and `path.data`. Give `ClusterConfiguration.NodeSettings` an optional ES version + plugin path, let `ExternalProcessEsCluster.startNode` pick the matching `EsDownload`/`EsInstallation` per node, add `restartNode(name, version)` built from the existing `Node.stop()` and `startNode(...)`, and write one `@Test` that starts N-1 nodes on version A, one on version B, then rolls the rest, asserting `/_searchguard/health` and a DLS/FLS query after each step. The only new external input is a second plugin zip, which `ci/run_tests.sh` would have to supply alongside the snapshot zip.

## Open questions for human review

1. `CLAUDE.md` states ES 9.4.4 on `main`; `pom.xml:49` is 9.5.5 (commit 5a0d3aaae). Which is the intended statement?
   - **Answer:** _pending_
2. `TEST_CLUSTER_TYPE` is never set in the repo. Is it set as a GitLab variable, i.e. do CI unit tests run external-process clusters at all, or only the 22 classes that force it?
   - **Answer:** _pending_
3. Does `sgi8` (branch `9.5.x`) contain any rolling-upgrade or mixed-version definition? Only its job names were visible here.
   - **Answer:** _pending_
4. `ssl` keeps private copies of `ClusterHelper`, `PluginAwareNode`, `SingleClusterTest`, `AbstractSGUnitTest`. Is the duplication intentional (to avoid a cycle on `security`)?
   - **Answer:** _pending_
5. Is the single wire-compat test (`SearchAuthTokensRequestTest`) the only SG transport message with a serialized-blob regression check by design, or an accident of one bug fix?
   - **Answer:** _pending_
6. `security-legacy` has no CI job and no tests of its own; is keeping it as a test-jar for `dlic-security`/`dlic-auditlog` still wanted, given the stated policy to write new tests against `LocalCluster`?
   - **Answer:** _pending_
7. `rerunFailingTestsCount=3` is disabled only in `dlic-dlsfls`. Is the intention to extend that to other modules once flakes are understood?
   - **Answer:** _pending_
