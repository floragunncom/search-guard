#!/usr/bin/env bash
# Stop hook. Runs its checks only when Claude writes the final report (the message contains the report heading
# "Separate findings"), not on the many turns where Claude just waits for background work. Checks:
#   1. temporary debug code (tagged SGTRACE) left in changed Java files
#   2. production code changed, but no test file changed
#   3. no full-suite run completed during this work (one run is enough; later fixes only need the failing
#      classes re-run, see CLAUDE.md "Tests")
# It never blocks twice in a row (stop_hook_active), so Claude can do the step or say why it skipped it.
set -uo pipefail

input=$(cat)
if printf '%s' "$input" | grep -Eq '"stop_hook_active"[[:space:]]*:[[:space:]]*true'; then exit 0; fi
proj=${CLAUDE_PROJECT_DIR:-$PWD}
top=$(git -C "$proj" rev-parse --show-toplevel 2>/dev/null) || exit 0
runs="$proj/.ai-runs"

cd "$top" || exit 0
main=() tests=() debug=()
while IFS= read -r f; do
  [ -e "$f" ] || continue # deleted
  case "$f" in
    */src/main/* | src/main/*) main+=("$f") ;;
    */src/test/* | src/test/*) tests+=("$f") ;;
  esac
  case "$f" in *.java) grep -q SGTRACE "$f" 2>/dev/null && debug+=("$f") ;; esac
done < <({
  git -c core.quotePath=false diff --name-only
  git -c core.quotePath=false diff --name-only --cached
  git -c core.quotePath=false ls-files --others --exclude-standard
} 2>/dev/null | sort -u)

# Remember when the current work began: the time of the oldest uncommitted production change, recorded the first
# time this hook sees one and kept until the tree is clean again. A full run must have started after it.
work_started="$runs/work-started"
if [ ${#main[@]} -eq 0 ]; then
  rm -f "$work_started"
elif [ ! -f "$work_started" ]; then
  oldest=${main[0]}
  for f in "${main[@]}"; do [ "$f" -ot "$oldest" ] && oldest=$f; done
  mkdir -p "$runs" && touch -r "$oldest" "$work_started"
  [ -f "$runs/.gitignore" ] || echo '*' > "$runs/.gitignore"
fi

printf '%s' "$input" | grep -q 'Separate findings' || exit 0 # not the final report

problems=()
if [ ${#debug[@]} -gt 0 ]; then
  problems+=("Temporary debug code (SGTRACE) is still in ${#debug[@]} file(s), e.g. ${debug[0]}. Remove it; keep the task's real changes in those files.")
fi

if [ ${#main[@]} -gt 0 ]; then
  if [ ${#tests[@]} -eq 0 ]; then
    problems+=("You changed ${#main[@]} production file(s) but no test file. Per CLAUDE.md \"Tests\": cover every new or changed execution path, or name the existing test for each changed method in the coverage table.")
  fi
  started="$runs/full-suite/started" summary="$runs/full-suite/summary.txt"
  if ! { [ -f "$started" ] && [ ! "$work_started" -nt "$started" ] && [ -f "$summary" ] && grep -q ALL_DONE "$summary"; }; then
    problems+=("No full-suite run completed during this work. Run bash .claude/scripts/full-suite.sh and wait for ALL_DONE in .ai-runs/full-suite/summary.txt (see CLAUDE.md \"Tests\").")
  fi
fi

[ ${#problems[@]} -eq 0 ] && exit 0
{
  echo "Before you finish:"
  for p in "${problems[@]}"; do echo "- $p"; done
  echo "If you skipped this on purpose (for example, you are stopping to ask the user for a decision), say so in one"
  echo "line and stop."
} >&2
exit 2
