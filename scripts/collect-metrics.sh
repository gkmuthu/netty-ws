#!/bin/sh
# Snapshot server and client Prometheus text. Pass base URLs as arguments.
# Example: scripts/collect-metrics.sh http://127.0.0.1:8080 http://127.0.0.1:9101
set -eu
out="${METRICS_DIR:-metrics}"
mkdir -p "$out"
stamp=$(date +%Y%m%dT%H%M%S)
i=0
for base in "$@"; do
  i=$((i + 1))
  curl --fail --silent --show-error --max-time 5 "$base/metrics" > "$out/${stamp}-${i}.prom" || echo "failed $base" >&2
  echo "wrote $out/${stamp}-${i}.prom"
done

