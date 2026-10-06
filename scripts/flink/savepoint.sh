#!/bin/bash
# Triggers a savepoint of the running job WITHOUT stopping it and prints its location.
#   scripts/flink/savepoint.sh [job-id]
#
# Savepoint vs checkpoint: a checkpoint is automatic, incremental and owned by Flink for failure recovery; a savepoint is taken
# on demand, in a portable format, and is meant for planned operations (upgrade, rescale, migration).
# Both contain operator state and the Kafka offsets read so far; the offsets are what makes the restored job resume exactly
# where the snapshot was taken.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
JOB=$(resolve_job "${1:-}")
TRIGGER=$(flink_post "/jobs/$JOB/savepoints" '{"cancel-job":false}' | json "d['request-id']")
echo "savepoint requested ($TRIGGER)..." >&2
wait_for_savepoint "/jobs/$JOB/savepoints/$TRIGGER"
