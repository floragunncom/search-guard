#!/usr/bin/env bash
# Stop hook: before Claude finishes, check the steps it most often skips.
#   1. temporary debug code (tagged SGTRACE) left in changed Java files
#   2. production code changed, but no test file changed
#   3. production code changed after the last completed full-suite run started
# It objects once per change set (quiet until a file changes again) and never twice in a row
# (stop_hook_active), so Claude can either do the step or say in one line why it skipped it.
set -uo pipefail

input=$(cat)
if printf '%s' "$input" | grep -Eq '"stop_hook_active"[[:space:]]*:[[:space:]]*true'; then exit 0; fi
proj=${CLAUDE_PROJECT_DIR:-$PWD}
top=$(git -C "$proj" rev-parse --show-toplevel 2>/dev/null) || exit 0
runs="$proj/.ai-runs"

session_id=$(printf '%s' "$input" | sed -n 's/.*"session_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
marker="$runs/sessions/${session_id:-none}" # written by session-start.sh when the task starts
nagged="$marker.nagged"

cd "$top" || exit 0
changed=() main=() tests=()
while IFS= read -r f; do
  [ -e "$f" ] || continue                                            # deleted: no timestamp to compare
  if [ -f "$marker" ] && [ ! "$f" -nt "$marker" ]; then continue; fi # changed before this task started
  changed+=("$f")
  case "$f" in
    */src/main/* | src/main/*) main+=("$f") ;;
    */src/test/* | src/test/*) tests+=("$f") ;;
  esac
done < <({
  git -c core.quotePath=false diff --name-only
  git -c core.quotePath=false diff --name-only --cached
  git -c core.quotePath=false ls-files --others --exclude-standard
} 2>/dev/null | sort -u)

[ ${#changed[@]} -eq 0 ] && exit 0

# Already reminded, and nothing changed since then: stay quiet.
if [ -f "$nagged" ]; then
  changed_since=0
  for f in "${changed[@]}"; do [ "$f" -nt "$nagged" ] && { changed_since=1; break; }; done
  [ $changed_since -eq 0 ] && exit 0
fi

problems=()
debug=()
for f in "${changed[@]}"; do
  case "$f" in *.java) grep -q SGTRACE "$f" 2>/dev/null && debug+=("$f") ;; esac
done
if [ ${#debug[@]} -gt 0 ]; then
  problems+=("Temporary debug code (SGTRACE) is still in ${#debug[@]} file(s), e.g. ${debug[0]}. Remove it; keep the task's real changes in those files.")
fi

if [ ${#main[@]} -gt 0 ]; then
  if [ ${#tests[@]} -eq 0 ]; then
    problems+=("You changed ${#main[@]} production file(s) but no test file. Per CLAUDE.md \"Tests\": add or extend a test for every new or changed execution path, or name the existing test that covers each changed method in the report's coverage table.")
  fi

  started="$runs/full-suite/started" summary="$runs/full-suite/summary.txt"
  if [ -f "$started" ] && [ -f "$summary" ] && grep -q ALL_DONE "$summary"; then
    for f in "${main[@]}"; do
      if [ "$f" -nt "$started" ]; then
        problems+=("Production code changed after the last full-suite run started (e.g. $f).")
        break
      fi
    done
  else
    problems+=("No completed full unit-test suite run in .ai-runs/full-suite/ (about 10 minutes; if one is running, wait for ALL_DONE). See CLAUDE.md \"Tests\".")
  fi
fi

[ ${#problems[@]} -eq 0 ] && exit 0
mkdir -p "$runs/sessions" && touch "$nagged"
{
  echo "Before you finish:"
  for p in "${problems[@]}"; do echo "- $p"; done
  echo "If you skipped this on purpose (you only re-ran the tests that failed in the last full run, or you are"
  echo "stopping to ask the user for a decision), say so in one line and stop."
} >&2
exit 2
