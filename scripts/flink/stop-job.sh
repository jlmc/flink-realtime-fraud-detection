#!/bin/bash
# Stops the running job with a savepoint and prints the savepoint location (use it with upload-job.sh --from-savepoint).
#   scripts/flink/stop-job.sh [job-id]
#
# "Stop with savepoint" takes the savepoint and only then finishes the job, so nothing processed after the snapshot is left
# half-done. Output to Kafka is committed as part of it.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
JOB=$(resolve_job "${1:-}")
TRIGGER=$(flink_post "/jobs/$JOB/stop" '{"drain":false}' | json "d['request-id']")
echo "stopping job $JOB with a savepoint ($TRIGGER)..." >&2
LOCATION=$(wait_for_savepoint "/jobs/$JOB/savepoints/$TRIGGER")
echo "Job stopped. Restore with:" >&2
echo "  scripts/flink/upload-job.sh --from-savepoint $LOCATION" >&2
echo "$LOCATION"
