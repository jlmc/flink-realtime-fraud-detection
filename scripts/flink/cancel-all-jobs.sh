#!/bin/bash
# Cancels every running job WITHOUT a savepoint (retained checkpoints stay in S3).
# Do this before recreating the Flink containers: with JobManager HA a job left running is recovered on the next start.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
JOB_NAME="" # every job, not only ours
IDS=$(job_ids RUNNING,RESTARTING,CREATED || true)
[ -n "$IDS" ] || { echo "no running jobs"; exit 0; }
for id in $IDS; do
  curl -fsS -X PATCH "$FLINK_URL/jobs/$id?mode=cancel" >/dev/null && echo "cancel requested: $id"
done
