#!/bin/bash
# Shared, best-effort resource/contention diagnostics for CI jobs.
#
# Sourced by:
#   - ci/run_tests.sh              (backend unit tests; ES runs embedded/external in the job container)
#   - ci/backend-int-tests.yml     (integration tests; ES/Kibana/test run as sibling containers via docker compose in dind)
#
# Every probe swallows errors so a minimal image cannot break the build. Output goes to the job log AND to an artifact
# file under ci-diagnostics/ (uploaded on success and failure). The docker section only produces output when a docker
# daemon is reachable, so it is a no-op for the unit-test path and the key per-container signal for the int-test path.
#
# Usage:
#   source ci/resource-diagnostics.sh
#   diag_init "$CI_JOB_NAME"       # -> sets DIAG_FILE under ci-diagnostics/
#   diag_snapshot "BEFORE TESTS"
#   diag_sampler_start             # background sampler; auto-stopped on EXIT, or call diag_sampler_stop
#   ... run tests ...
#   diag_sampler_stop
#   diag_snapshot "AFTER TESTS"
#
# Env knobs: CI_RESOURCE_SAMPLING=false disables the sampler; CI_RESOURCE_SAMPLING_INTERVAL sets the period (default 30s).

DIAG_DIR="${DIAG_DIR:-$(pwd)/ci-diagnostics}"
DIAG_FILE="${DIAG_FILE:-}"
DIAG_SAMPLER_PID=""

diag_init() {
  local name="${1:-diagnostics}"
  mkdir -p "$DIAG_DIR"
  # sanitise the name into a safe file name (POSIX-portable; avoids bash-only ${//} so the file also sources under sh)
  local safe
  safe="$(printf '%s' "$name" | tr -c 'A-Za-z0-9._-' '_')"
  DIAG_FILE="$DIAG_DIR/${safe}-diagnostics.log"
}

# One-shot snapshot of host (and, when present, docker) resources.
diag_snapshot() {
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
    echo "--- ulimits (nofile/memlock/nproc are ES bootstrap checks) ---"
    echo "current user ($(id -un 2>/dev/null || echo '?')): nofile soft=$(ulimit -Sn 2>/dev/null || echo '?') hard=$(ulimit -Hn 2>/dev/null || echo '?')"
    echo "    (if hard is high but soft is 1024, run_tests.sh raises soft in-container; if hard is ALSO 1024, the runner"
    echo "     host config is required: /etc/docker/daemon.json default-ulimits, or [runners.docker.ulimit] on GL Runner >=15.7)"
    ulimit -a 2>/dev/null || echo '?'
    if id es_test >/dev/null 2>&1; then
      echo "es_test: nofile soft=$(su - es_test -c 'ulimit -Sn' 2>/dev/null || echo '?') hard=$(su - es_test -c 'ulimit -Hn' 2>/dev/null || echo '?')"
      su - es_test -c 'ulimit -a' 2>/dev/null || echo '?'
    fi
    echo "--- process counts (NOTE: this container has its own PID namespace, so these see only THIS job;"
    echo "    use host-wide loadavg/PSI above as the cross-job contention signal) ---"
    echo "java processes  : $(pgrep -x java 2>/dev/null | wc -l)"
    echo "threads (tasks) : $(ps -eLf 2>/dev/null | tail -n +2 | wc -l)"
    echo "--- file descriptors (the nofile=1024 canary; a single java proc nearing 1024 = FD exhaustion) ---"
    echo "system-wide fs.file-nr (allocated unused max): $(cat /proc/sys/fs/file-nr 2>/dev/null || echo '?')"
    for pid in $(pgrep -x java 2>/dev/null); do
      soft="$(cat /proc/$pid/limits 2>/dev/null | awk '/Max open files/{print $4}')"
      count="$(ls /proc/$pid/fd 2>/dev/null | wc -l)"
      echo "  java pid $pid : open_fds=$count soft_limit=${soft:-?}"
    done
    echo "--- cgroup tasks / cpu throttling (throttling stays 0 until a cpus limit is set) ---"
    echo "pids.current/max : $(cat /sys/fs/cgroup/pids.current 2>/dev/null || echo '?') / $(cat /sys/fs/cgroup/pids.max 2>/dev/null || echo '?')"
    awk '{print "cpu.stat "$0}' /sys/fs/cgroup/cpu.stat 2>/dev/null || echo 'cpu.stat unavailable'
    echo "--- top 10 processes by memory (RSS) ---"
    ps -eo pid,ppid,user,rss,pcpu,comm --sort=-rss 2>/dev/null | head -n 11 || echo '?'
    diag_docker_section
    echo "--- recent OOM-killer activity (best effort; often unavailable in unprivileged containers) ---"
    dmesg -T 2>/dev/null | grep -iE 'killed process|out of memory|oom' | tail -n 5 || echo 'dmesg unavailable'
    echo "====================================================================="
  } 2>&1 | tee -a "$DIAG_FILE"
}

# Per-container view. Only emits when a docker daemon is reachable (i.e. the dind integration jobs), where ES/Kibana/the
# test runner are sibling containers whose CPU/mem/PIDs the job container's own /proc cannot see.
diag_docker_section() {
  command -v docker >/dev/null 2>&1 || return 0
  docker info >/dev/null 2>&1 || return 0
  echo "--- docker containers (int tests run ES/Kibana/test as sibling containers via docker compose) ---"
  docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' 2>/dev/null || echo '?'
  echo "--- docker stats (per-container CPU/mem/PIDs; the real per-node contention signal for int tests) ---"
  docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.PIDs}}' 2>/dev/null || echo '?'
  echo "--- docker disk usage ---"
  docker system df 2>/dev/null || echo '?'
}

# One sampler tick: a host line, plus (when docker is present) one compact line per running container.
diag_sample_line() {
  local cpu_psi mem_psi io_psi cg_mem java_procs fd_java_max fd_sys threads cpu_throttled_us n pid line
  cpu_psi="$(awk '/some/{print $2}' /proc/pressure/cpu 2>/dev/null)"
  mem_psi="$(awk '/some/{print $2}' /proc/pressure/memory 2>/dev/null)"
  io_psi="$(awk '/some/{print $2}' /proc/pressure/io 2>/dev/null)"
  if [ -f /sys/fs/cgroup/memory.current ]; then
    cg_mem="$(cat /sys/fs/cgroup/memory.current 2>/dev/null)"
  else
    cg_mem="$(cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null)"
  fi
  java_procs=0; fd_java_max=0
  for pid in $(pgrep -x java 2>/dev/null); do
    n="$(ls /proc/$pid/fd 2>/dev/null | wc -l)"
    java_procs=$((java_procs + 1))
    [ "$n" -gt "$fd_java_max" ] && fd_java_max=$n
  done
  fd_sys="$(awk '{print $1}' /proc/sys/fs/file-nr 2>/dev/null)"
  threads="$(ps -eLf 2>/dev/null | tail -n +2 | wc -l)"
  cpu_throttled_us="$(awk '/throttled_usec/{print $2}' /sys/fs/cgroup/cpu.stat 2>/dev/null)"
  line="[resource-sample $(date -u +%FT%TZ)] loadavg=$(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null) mem_available_kb=$(awk '/MemAvailable/{print $2}' /proc/meminfo 2>/dev/null) swap_free_kb=$(awk '/SwapFree/{print $2}' /proc/meminfo 2>/dev/null) cgroup_mem_bytes=${cg_mem:-?} psi_cpu=${cpu_psi:-n/a} psi_mem=${mem_psi:-n/a} psi_io=${io_psi:-n/a} java=${java_procs} fd_java_max=${fd_java_max} fd_sys_alloc=${fd_sys:-?} threads=${threads} cpu_throttled_us=${cpu_throttled_us:-0}"
  echo "$line"
  echo "$line" >> "$DIAG_FILE"

  # Per-container docker stats (int tests): one greppable line per running container. Piped (not process-substituted)
  # so this also works when the script is sourced under a POSIX sh (e.g. busybox in the dind image).
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    local ts
    ts="$(date -u +%FT%TZ)"
    docker stats --no-stream --format '{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}} mem_pct={{.MemPerc}} pids={{.PIDs}}' 2>/dev/null \
      | while IFS= read -r dline; do
          [ -n "$dline" ] || continue
          echo "[docker-sample $ts] $dline"
          echo "[docker-sample $ts] $dline" >> "$DIAG_FILE"
        done
  fi
}

diag_sampler_start() {
  [ "${CI_RESOURCE_SAMPLING:-true}" == "true" ] || return 0
  (
    # Best-effort sampling: never let a missing probe path (awk exits 2 on a missing file, etc.) kill the loop.
    set +e
    while true; do
      diag_sample_line
      sleep "${CI_RESOURCE_SAMPLING_INTERVAL:-30}"
    done
  ) &
  DIAG_SAMPLER_PID=$!
  trap 'diag_sampler_stop' EXIT
}

diag_sampler_stop() {
  if [ -n "$DIAG_SAMPLER_PID" ]; then
    kill "$DIAG_SAMPLER_PID" 2>/dev/null || true
    DIAG_SAMPLER_PID=""
  fi
}
