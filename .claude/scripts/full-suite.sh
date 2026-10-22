#!/usr/bin/env bash
# Run the full test suite the way CI does, in the background, and return immediately:
#   1. one build of all modules including the plugin zip (no clean, so it is incremental)
#   2. the tests of every module with that zip, so external-process clusters reuse it instead of each test class
#      building its own plugin (parallel forks collide when they do), continuing past failing modules
#   3. a per-module summary in .ai-runs/full-suite/summary.txt, ending with ALL_DONE
# The stop hook reads .ai-runs/full-suite/started and summary.txt.
set -euo pipefail
cd "${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel)}"
root=$PWD
out=$root/.ai-runs/full-suite

if [ "${1:-}" != --foreground ]; then
  rm -rf "$out" && mkdir -p "$out" "$root/.ai-runs/es-downloads"
  [ -f "$root/.ai-runs/.gitignore" ] || echo '*' > "$root/.ai-runs/.gitignore"
  touch "$out/started"
  if command -v setsid >/dev/null 2>&1; then
    setsid nohup bash "$0" --foreground > "$out/run.log" 2>&1 < /dev/null &
  else # macOS has no setsid
    nohup bash "$0" --foreground > "$out/run.log" 2>&1 < /dev/null &
  fi
  echo "Full suite started (pid $!). Watch $out/summary.txt until it contains ALL_DONE."
  echo "Logs: $out/build.log, $out/test.log"
  exit 0
fi

summary=$out/summary.txt
if ! mvn install -DskipTests > "$out/build.log" 2>&1; then
  printf 'BUILD FAILED, no tests were run. See %s\nALL_DONE\n' "$out/build.log" > "$summary"
  exit 0
fi

zip=$(find "$root/plugin/target/releases" -name 'search-guard-flx-elasticsearch-plugin-*.zip' 2>/dev/null | head -1)
plugin_arg=()
[ -n "$zip" ] && plugin_arg=("-Dsg.tests.sg_plugin.file=$zip")

# maven.test.failure.ignore: failing tests in one module must not stop or skip the modules after it.
mvn test -fae -Dmaven.test.failure.ignore=true ${plugin_arg[@]+"${plugin_arg[@]}"} \
  "-Dsg.tests.es_download_cache.dir=$root/.ai-runs/es-downloads" > "$out/test.log" 2>&1 || true

python3 -I - "$root" "$out/started" "$out/test.log" > "$summary" <<'EOF' || echo "Summary failed; read $out/test.log" > "$summary"
import glob, os, re, sys
import xml.etree.ElementTree as ET

root, started, log = sys.argv[1], os.path.getmtime(sys.argv[2]), sys.argv[3]
for reports in sorted(glob.glob(os.path.join(root, '*', 'target', 'surefire-reports'))):
    module = reports.split(os.sep)[-3]
    tests = failures = errors = skipped = flakes = 0
    failing = set()
    for path in glob.glob(os.path.join(reports, 'TEST-*.xml')):
        if os.path.getmtime(path) < started:
            continue  # left over from an earlier run
        try:
            suite = ET.parse(path).getroot()
        except ET.ParseError:
            failing.add(os.path.basename(path) + ' (unreadable report)')
            continue
        tests += int(suite.get('tests', 0))
        failures += int(suite.get('failures', 0))
        errors += int(suite.get('errors', 0))
        skipped += int(suite.get('skipped', 0))
        for case in suite.iter('testcase'):
            flakes += len(case.findall('flakyFailure')) + len(case.findall('flakyError'))
            if case.find('failure') is not None or case.find('error') is not None:
                failing.add(case.get('classname') or suite.get('name'))
    if tests or failing:
        line = f'{module}: run={tests} failures={failures} errors={errors} skipped={skipped} flakes={flakes}'
        if failing:
            line += ' failing=' + ','.join(sorted(failing))
        print(line)
with open(log, errors='replace') as f:
    for entry in f:
        if re.search(r'\.{3,}\s*(FAILURE|SKIPPED)', entry):
            print('reactor:', re.sub(r'^\[(INFO|ERROR)\]\s*', '', entry.strip()))
EOF
echo ALL_DONE >> "$summary"
