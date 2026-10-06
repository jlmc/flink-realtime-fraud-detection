#!/bin/bash
# Uploads the job JAR to a Flink SESSION cluster and starts it.
#
#   scripts/flink/upload-job.sh [--parallelism N] [--from-savepoint PATH] [--allow-non-restored-state]
#                               [--allow-multiple] [-- job args, e.g. --postgres.pool-size 2]
#
# Notes
#  - Passes the parallelism explicitly: submitting through the REST API did not pick up the cluster's parallelism.default
#    (see docs/spikes/S1-plugin-classloading.md).
#  - Refuses to start a second copy of the job: both would join the same Kafka consumer group and split the partitions.
#  - The plugin JARs are installed into lib/ when the Flink containers START, so after changing a plugin run
#    scripts/stage-dist.sh and restart the Flink containers first.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../.." && pwd)"
source "$DIR/common.sh"

PARALLELISM="${FLINK_PARALLELISM:-3}"
SAVEPOINT=""
ALLOW_NON_RESTORED=false
ALLOW_MULTIPLE=false
JOB_ARGS=()

while [ $# -gt 0 ]; do
  case "$1" in
    --parallelism) PARALLELISM="$2"; shift 2 ;;
    --from-savepoint) SAVEPOINT="$2"; shift 2 ;;
    --allow-non-restored-state) ALLOW_NON_RESTORED=true; shift ;;
    --allow-multiple) ALLOW_MULTIPLE=true; shift ;;
    --) shift; JOB_ARGS=("$@"); break ;;
    -h|--help) sed -n '2,13p' "$0"; exit 0 ;;
    *) die "unknown option: $1 (see --help)" ;;
  esac
done

JAR=$(ls "$ROOT"/dist/job/flink-job-*-shaded.jar 2>/dev/null | head -n 1 || true)
if [ -z "$JAR" ]; then
  echo "No job JAR in dist/job. Building and staging..."
  (cd "$ROOT" && mvn -q -B clean package -DskipTests && ./scripts/stage-dist.sh >/dev/null)
  JAR=$(ls "$ROOT"/dist/job/flink-job-*-shaded.jar | head -n 1)
fi

wait_for_cluster

if [ "$ALLOW_MULTIPLE" = false ] && [ -n "$(job_ids RUNNING,RESTARTING)" ]; then
  die "'$JOB_NAME' is already running. Stop it first (scripts/flink/stop-job.sh) or pass --allow-multiple."
fi

echo "Uploading $(basename "$JAR") ..."
UPLOAD=$(curl -fsS -X POST -H "Expect:" -F "jarfile=@$JAR" "$FLINK_URL/jars/upload")
JAR_ID=$(echo "$UPLOAD" | json "d['filename'].split('/')[-1]")

# JSON body built with python so quoting of arguments is always right
BODY=$(PARALLELISM="$PARALLELISM" SAVEPOINT="$SAVEPOINT" NON_RESTORED="$ALLOW_NON_RESTORED" python3 - ${JOB_ARGS[@]+"${JOB_ARGS[@]}"} <<'PY'
import json, os, sys
body = {"entryClass": "io.github.jlmc.fraud.bootstrap.FraudJob", "parallelism": int(os.environ["PARALLELISM"])}
if sys.argv[1:]:
    body["programArgsList"] = sys.argv[1:]
if os.environ["SAVEPOINT"]:
    body["savepointPath"] = os.environ["SAVEPOINT"]
    body["allowNonRestoredState"] = os.environ["NON_RESTORED"] == "true"
print(json.dumps(body))
PY
)

RESPONSE=$(curl -sS -X POST -H 'Content-Type: application/json' -d "$BODY" "$FLINK_URL/jars/$JAR_ID/run")
echo "$RESPONSE" | grep -q '"jobid"' || die "could not start the job: $RESPONSE"
JOB_ID=$(echo "$RESPONSE" | json "d['jobid']")

echo "=================================================="
echo "Job started: $JOB_ID  (parallelism $PARALLELISM${SAVEPOINT:+, restored from $SAVEPOINT})"
echo "Web UI:      $FLINK_URL/#/job/running/$JOB_ID"
echo "=================================================="
