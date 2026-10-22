---
paths:
  - "**/src/test/**"
---

# Tests and the test cluster

Loaded when you work with test sources. The test rules in the root `CLAUDE.md` ("Tests") still apply.

## The test cluster: two very different modes

`LocalCluster` (in `security/src/test/.../test/helper/cluster/`, shipped to other modules via the
`search-guard-flx-security:tests` artifact) can start a cluster two ways. Read its class javadoc before debugging
cluster-related test failures.

- **JVM-embedded (default locally).** Fast, debuggable, but only loads the SG modules that are on the *test
  module's* dependency list — enterprise modules or Signals may simply be absent. Not a full ES.
- **External process** (`.useExternalProcessCluster()` on the builder, or `-Dsg.tests.use_ep_cluster=true`).
  Downloads a real ES distribution and runs real nodes. Required for features the embedded cluster lacks (LogsDB,
  data streams), and the only mode that catches JPMS export errors and plugin-descriptor problems. Additionally, only this mode enforces proper classloader isolation

External-process tests need a plugin zip. Without one, every test class builds its own plugin, which is slow, and
parallel surefire forks collide. Build the zip once (`mvn install -DskipTests`, or only the changed modules plus
`-pl plugin`) and pass it with an absolute path:

- `-Dsg.tests.sg_plugin.file=/abs/path/plugin/target/releases/search-guard-flx-elasticsearch-plugin-<version>.zip`
- `-Dsg.tests.es_download_cache.dir=<repo>/.ai-runs/es-downloads` to reuse the downloaded ES distributions
- `-DforkCount=1` only if node starts time out on a busy machine (with a prebuilt zip the default forks work)
- `-Dsg.tests.use_ep_cluster=true` forces external-process mode for any test class

Multi-node DLS/FLS/field-masking behaviour (for example in the fetch phase) only shows on external-process clusters
with several shards: `DlsIntTest`, `FlsIntTest`, `FmIntTest`, `SqlAuthorizationReadOnlyIntTests`.

Test configuration is built in Java, not YAML: `TestSgConfig` with nested `Role`, `User`, `Authc`, `DlsFls`
builders, handed to `LocalCluster.Builder`; tests drive the cluster over REST with `GenericRestClient`.
`scheduler/src/test/java/com/floragunn/searchsupport/jobs/LocalCluster.java` is an unrelated class of the same
name.
