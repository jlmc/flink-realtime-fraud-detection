#!/bin/sh
# Idempotent: waits for MinIO, creates the bucket and the prefixes if missing, then exits 0.
set -eu

: "${S3_ENDPOINT:?}" "${S3_ACCESS_KEY:?}" "${S3_SECRET_KEY:?}" "${S3_BUCKET:?}"

i=0
until mc alias set local "$S3_ENDPOINT" "$S3_ACCESS_KEY" "$S3_SECRET_KEY" >/dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -ge 60 ] && { echo "MinIO not reachable at $S3_ENDPOINT" >&2; exit 1; }
  echo "waiting for MinIO ($i/60)"; sleep 2
done

mc mb --ignore-existing "local/$S3_BUCKET"
# S3 has no real folders; zero-byte markers make the prefixes visible in the console.
for prefix in checkpoints savepoints ha; do
  echo -n | mc pipe "local/$S3_BUCKET/$prefix/.keep" >/dev/null
done
echo "bucket '$S3_BUCKET' ready:"; mc ls "local/$S3_BUCKET"
