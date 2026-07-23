#!/bin/bash
set -e

MODULE=$1
PROFILE=$2

# Diagnostics are written to this dir (and uploaded as a CI artifact via .be_unit_test) AND streamed to the job log.
# They exist to track down flaky test-cluster startups (YELLOW cluster, "node not connected", "startup timed out"),
# which are almost always CPU/memory/IO contention when several heavy jobs land on the same bare-metal runner.
DIAG_DIR="$(pwd)/ci-diagnostics"
mkdir -p "$DIAG_DIR"
DIAG_FILE="$DIAG_DIR/${MODULE//\//_}-diagnostics.log"

# Prints a snapshot of the host/container resources and settings that matter for embedded/external ES test clusters.
# Everything is best-effort: individual probes swallow errors so a minimal container image cannot break the build.
# Output goes to both the job log and the artifact file.
print_diagnostics() {
  local label="$1"
  {
    echo "=================== RESOURCE DIAGNOSTICS: ${label} ==================="
    echo "date            : $(date -u +%FT%TZ)"
    echo "hostname        : $(cat /proc/sys/kernel/hostname 2>/dev/null || echo '?')"
    echo "job             : ${CI_JOB_NAME:-?} #${CI_JOB_ID:-?} on ${CI_RUNNER_DESCRIPTION:-?} (${CI_RUNNER_TAGS:-?})"
    echo "cpu count       : $(nproc 2>/dev/null || echo '?')"
    echo "loadavg         : $(cat /proc/loadavg 2>/dev/null || echo '?')   (1m 5m 15m; compare against cpu count)"
    echo "--- memory (host-wide) ---"
    free -h 2>/dev/null || awk '/MemTotal|MemAvailable|SwapTotal|SwapFree/{print $0}' /proc/meminfo 2>/dev/null || echo '?'
    echo "--- memory (cgroup limit for THIS container - what actually triggers OOM/GC death) ---"
    if [ -f /sys/fs/cgroup/memory.max ]; then
      echo "cgroup v2 memory.max     : $(cat /sys/fs/cgroup/memory.max 2>/dev/null || echo '?')"
      echo "cgroup v2 memory.current : $(cat /sys/fs/cgroup/memory.current 2>/dev/null || echo '?')"
      echo "cgroup v2 memory.swap.max: $(cat /sys/fs/cgroup/memory.swap.max 2>/dev/null || echo '?')"
      awk '{print "cgroup v2 "$0}' /sys/fs/cgroup/memory.events 2>/dev/null || true
    elif [ -f /sys/fs/cgroup/memory/memory.limit_in_bytes ]; then
      echo "cgroup v1 memory.limit_in_bytes : $(cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null || echo '?')"
      echo "cgroup v1 memory.usage_in_bytes : $(cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null || echo '?')"
      echo "cgroup v1 memory.failcnt        : $(cat /sys/fs/cgroup/memory/memory.failcnt 2>/dev/null || echo '?')  (>0 means the limit was hit)"
    else
      echo "no cgroup memory files found"
    fi
    echo "--- pressure stall info (PSI; 'some'/'full' avg = % of time stalled on a resource; the clearest contention signal) ---"
    for p in cpu memory io; do
      echo "$p : $(cat /proc/pressure/$p 2>/dev/null | tr '\n' ' ' || echo 'n/a')"
    done
    echo "--- disk (cwd + /tmp; FSTYPE reveals local SSD vs network/overlay/tmpfs) ---"
    df -hT . /tmp 2>/dev/null || df -h . /tmp 2>/dev/null || echo '?'
    echo "--- ES-relevant kernel settings ---"
    echo "vm.max_map_count     : $(cat /proc/sys/vm/max_map_count 2>/dev/null || echo '?')   (ES needs >= 262144)"
    echo "vm.swappiness        : $(cat /proc/sys/vm/swappiness 2>/dev/null || echo '?')      (low is better; swapping causes node drops)"
    echo "vm.overcommit_memory : $(cat /proc/sys/vm/overcommit_memory 2>/dev/null || echo '?')"
    echo "--- ulimits for the es_test user that runs ES (nofile/memlock/nproc are ES bootstrap checks) ---"
    su - es_test -c 'ulimit -a' 2>/dev/null || echo '?'
    echo "--- co-located load (other java/mvn processes = other jobs sharing this host) ---"
    echo "java processes  : $(pgrep -c -x java 2>/dev/null || echo '?')"
    echo "mvn processes   : $(pgrep -c -f 'org.apache.maven' 2>/dev/null || echo '?')"
    echo "--- top 10 processes by memory (RSS) ---"
    ps -eo pid,ppid,user,rss,pcpu,comm --sort=-rss 2>/dev/null | head -n 11 || echo '?'
    echo "--- recent OOM-killer activity (best effort; often unavailable in unprivileged containers) ---"
    dmesg -T 2>/dev/null | grep -iE 'killed process|out of memory|oom' | tail -n 5 || echo 'dmesg unavailable'
    echo "====================================================================="
  } 2>&1 | tee -a "$DIAG_FILE"
}

MAVEN_CLI_OPTS="--batch-mode -s settings.xml"
RUN_TESTS_COMMAND="mvn $MAVEN_CLI_OPTS -pl $MODULE test \
  -Dmaven.repo.local=$(pwd)/.m2/repository \
  -Dsg.tests.es_download_cache.dir=${ES_DL_CACHE:-$(pwd)} \
  -Dsg.tests.sg_plugin.file=$(realpath ./plugin/target/releases/search-guard-flx-elasticsearch-plugin-*SNAPSHOT*.zip) \
  -Drevision=$SNAPSHOT_REVISION \
  -Delasticsearch.version=$ES_VERSION"

if [ -n "$PROFILE" ]; then
  RUN_TESTS_COMMAND="$RUN_TESTS_COMMAND -P $PROFILE"
fi
useradd -m es_test

chown -R es_test .

if [ "$TEST_CLUSTER_TYPE" == "full" ]; then
  RUN_TESTS_COMMAND="$RUN_TESTS_COMMAND -Dsg.tests.use_ep_cluster=true"
else
  RUN_TESTS_COMMAND="$RUN_TESTS_COMMAND -Dsg.tests.use_ep_cluster=false"
fi

print_diagnostics "BEFORE TESTS"

# Sample load/memory/pressure during the run so we can see the contention spike at the moment a cluster fails to
# start. Written to both the log and the artifact file. Disable with CI_RESOURCE_SAMPLING=false.
SAMPLER_PID=""
if [ "${CI_RESOURCE_SAMPLING:-true}" == "true" ]; then
  (
    while true; do
      cpu_psi="$(awk '/some/{print $2}' /proc/pressure/cpu 2>/dev/null)"
      mem_psi="$(awk '/some/{print $2}' /proc/pressure/memory 2>/dev/null)"
      io_psi="$(awk '/some/{print $2}' /proc/pressure/io 2>/dev/null)"
      if [ -f /sys/fs/cgroup/memory.current ]; then
        cg_mem="$(cat /sys/fs/cgroup/memory.current 2>/dev/null)"
      else
        cg_mem="$(cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null)"
      fi
      line="[resource-sample $(date -u +%FT%TZ)] loadavg=$(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null) mem_available_kb=$(awk '/MemAvailable/{print $2}' /proc/meminfo 2>/dev/null) swap_free_kb=$(awk '/SwapFree/{print $2}' /proc/meminfo 2>/dev/null) cgroup_mem_bytes=${cg_mem:-?} psi_cpu=${cpu_psi:-n/a} psi_mem=${mem_psi:-n/a} psi_io=${io_psi:-n/a}"
      echo "$line"
      echo "$line" >> "$DIAG_FILE"
      sleep "${CI_RESOURCE_SAMPLING_INTERVAL:-30}"
    done
  ) &
  SAMPLER_PID=$!
  trap 'if [ -n "$SAMPLER_PID" ]; then kill "$SAMPLER_PID" 2>/dev/null || true; fi' EXIT
fi

echo "RUN TEST COMMAND $RUN_TESTS_COMMAND"

# Run the tests without letting set -e abort before we print the closing diagnostics, then propagate the real exit code.
set +e
su - es_test -c "cd `pwd` && $RUN_TESTS_COMMAND"
TEST_EXIT=$?
set -e

if [ -n "$SAMPLER_PID" ]; then
  kill "$SAMPLER_PID" 2>/dev/null || true
  SAMPLER_PID=""
fi

print_diagnostics "AFTER TESTS"

echo "TEST EXIT CODE: $TEST_EXIT (see $DIAG_FILE, uploaded as a ci-diagnostics artifact)"
exit $TEST_EXIT
