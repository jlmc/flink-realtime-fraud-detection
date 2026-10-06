#!/bin/bash
# Lists the jobs of the Flink cluster with their state.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
flink_get /jobs/overview | python3 -c "
import json,sys
jobs=json.load(sys.stdin)['jobs']
if not jobs: print('no jobs')
for j in jobs: print(j['jid'], j['state'].ljust(10), j['name'])"
