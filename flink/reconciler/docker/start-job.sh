#!/usr/bin/env bash
# JobManager entrypoint: starts the reconciler job in application mode and resumes it from the latest
# completed checkpoint of this flow, so pending transfers survive a JobManager restart or "compose up".
#
# Without HA a restarted JobManager starts the job with a new job id and empty state; Flink itself only
# restores checkpoints after task failures inside a running job. This script fills that gap:
#   /opt/flink/checkpoints/<FLOW>/<job-id>/chk-<n>/_metadata   (newest wins)
# NO_CLAIM: the new job takes a full first checkpoint and never references the restored files, so the
# job directories of older runs (all except the one restored from) are deleted here. At most two job
# directories exist: the one restored from and the running job's.
#
#   RESTORE_FROM_CHECKPOINT=false   start with empty state (e.g. after an incompatible state change)
#   CHECKPOINT_ROOT                 default /opt/flink/checkpoints/$FLOW
set -euo pipefail

flow="${FLOW:-fast}"
root="${CHECKPOINT_ROOT:-/opt/flink/checkpoints/${flow}}"
args=(standalone-job --job-classname fast.reconciler.flink.FlinkReconcilerJob)

latest=""
if [[ "${RESTORE_FROM_CHECKPOINT:-true}" == "true" && -d "$root" ]]; then
  # _metadata is written last, so only completed checkpoints have it.
  latest="$(find "$root" -mindepth 3 -maxdepth 3 -path '*/chk-*/_metadata' -printf '%T@ %h\n' 2>/dev/null \
            | sort -n | tail -n 1 | cut -d' ' -f2-)"
fi

if [[ -n "$latest" ]]; then
  echo "start-job: flow=${flow} restoring from ${latest}"
  keep="$(dirname "$latest")"
  for dir in "$root"/*/; do
    dir="${dir%/}"
    # Only job directories (32 hex characters) of older runs.
    if [[ "$dir" != "$keep" && "$(basename "$dir")" =~ ^[0-9a-f]{32}$ ]]; then
      echo "start-job: deleting checkpoints of an older run: ${dir}"
      rm -rf -- "$dir"
    fi
  done
  # allowNonRestoredState: tolerate state of operators that no longer exist in the new job.
  args+=(--fromSavepoint "$latest" --restoreMode NO_CLAIM --allowNonRestoredState)
else
  echo "start-job: flow=${flow} no completed checkpoint under ${root}; starting with empty state"
fi

exec /docker-entrypoint.sh "${args[@]}" "$@"
