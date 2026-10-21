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
