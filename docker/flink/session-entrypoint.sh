#!/bin/bash
# Session Mode: plugin JARs must be on Flink's own classpath (lib/), see docs/spikes/S1-plugin-classloading.md.
# usrlib/ is NOT on the classpath in Session Mode, so the JARs are copied into lib/ before Flink starts.
# Changing a plugin therefore means: scripts/stage-dist.sh, then restart the Flink containers (intended, PLAN section 5).
set -euo pipefail

PLUGINS=/opt/fraud/plugins
if compgen -G "$PLUGINS/*.jar" >/dev/null; then
  cp "$PLUGINS"/*.jar /opt/flink/lib/
  echo "fraud-poc: installed plugin JARs into lib/: $(cd "$PLUGINS" && ls *.jar | tr '\n' ' ')"
else
  echo "fraud-poc: WARNING no plugin JARs in $PLUGINS (run scripts/stage-dist.sh, then restart Flink). The job will refuse to start without validation rules." >&2
fi

exec /docker-entrypoint.sh "$@"
