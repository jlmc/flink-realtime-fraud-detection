#!/bin/bash
# Shared helpers for the Flink operation scripts. Source it, do not run it.
# Requires: curl, python3 (both ship with macOS developer tools).

FLINK_URL="${FLINK_URL:-http://localhost:8081}"
JOB_NAME="${JOB_NAME:-transaction-fraud-risk}"

die() { echo "ERROR: $*" >&2; exit 1; }

command -v curl >/dev/null || die "curl is required"
command -v python3 >/dev/null || die "python3 is required"

# json <expression over d>: evaluates a Python expression on the JSON read from stdin
json() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }

flink_get()  { curl -fsS --connect-timeout 5 "$FLINK_URL$1"; }
flink_post() { curl -fsS --connect-timeout 5 -X POST -H 'Content-Type: application/json' -d "${2:-{\}}" "$FLINK_URL$1"; }

wait_for_cluster() {
  local i overview slots
  for i in $(seq 1 40); do
    overview=$(curl -s --connect-timeout 3 "$FLINK_URL/overview" 2>/dev/null || true)
    slots=$(echo "$overview" | python3 -c "import json,sys; print(json.load(sys.stdin).get('slots-total',0))" 2>/dev/null || echo 0)
    if [ "${slots:-0}" -gt 0 ]; then
      echo "Flink at $FLINK_URL is ready ($slots slots)."
      return 0
    fi
    echo "waiting for Flink and a TaskManager ($i/40)..."; sleep 3
  done
  die "no TaskManager registered at $FLINK_URL (is the cluster up? docker compose ps)"
}

# ids of jobs in the given states (default RUNNING), optionally filtered by name
job_ids() {
  local states="${1:-RUNNING}"
  flink_get /jobs/overview | python3 -c "
import json,sys
states=set('$states'.split(','))
for j in json.load(sys.stdin)['jobs']:
    if j['state'] in states and ('$JOB_NAME' in ('', j['name'])): print(j['jid'])"
}

# the single running job of ours; dies when there is none or several and no id was given
resolve_job() {
  local explicit="${1:-}"
  if [ -n "$explicit" ]; then echo "$explicit"; return; fi
  local ids; ids=$(job_ids RUNNING)
  local count; count=$(echo "$ids" | grep -c . || true)
  [ "$count" -eq 1 ] || die "expected exactly one running '$JOB_NAME' job, found $count. Pass the job id explicitly (scripts/flink/list-jobs.sh)."
  echo "$ids"
}

# Polls an asynchronous operation (savepoint) until it completes. $1 = path of the status resource
wait_for_savepoint() {
  local path="$1" i status
  for i in $(seq 1 60); do
    status=$(flink_get "$path")
    case "$(echo "$status" | json "d['status']['id']")" in
      COMPLETED)
        local failure; failure=$(echo "$status" | json "(d.get('operation') or {}).get('failure-cause') or ''")
        [ -z "$failure" ] || die "savepoint failed: $failure"
        echo "$status" | json "d['operation']['location']"
        return 0 ;;
    esac
    sleep 2
  done
  die "savepoint did not complete within 2 minutes"
}
