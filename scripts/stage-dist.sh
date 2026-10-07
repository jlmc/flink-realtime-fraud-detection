#!/bin/bash
# Copies the built artifacts into dist/ so Docker Compose can mount them into the Flink containers.
#   dist/usrlib : validation-api + validation-rule-* and fraud-rule-* JARs (plugins; NOT inside the job JAR)
#   dist/job    : the flink-job fat JAR (once it exists)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
rm -f "$ROOT"/dist/usrlib/*.jar "$ROOT"/dist/job/*.jar
cp "$ROOT"/validation-api/target/validation-api-*-SNAPSHOT.jar "$ROOT"/dist/usrlib/
for m in "$ROOT"/validation-rules/validation-rule-*/ "$ROOT"/fraud-rules/fraud-rule-*/; do cp "$m"/target/*-SNAPSHOT.jar "$ROOT"/dist/usrlib/; done
if ls "$ROOT"/flink-job/target/flink-job-*-shaded.jar >/dev/null 2>&1; then
  cp "$ROOT"/flink-job/target/flink-job-*-shaded.jar "$ROOT"/dist/job/
fi
ls -1 "$ROOT"/dist/usrlib "$ROOT"/dist/job
