#!/usr/bin/env bash
# Job supervisor for the shared Flink session cluster: keeps one reconciler job per flow running.
#
# Every SUPERVISE_INTERVAL seconds, for each flow in FLOWS:
#   - a job "flink-<flow>-reconciler" is active (running, starting, restarting) -> nothing to do
#   - its newest job was CANCELED (e.g. from the Flink UI)                      -> left stopped
#   - there is no such job, or it FAILED / FINISHED (e.g. after a JobManager restart, which loses
#     all jobs without HA)                                                       -> submitted again,
#     resuming from the flow's newest completed checkpoint (/opt/flink/checkpoints/<flow>/<job-id>/chk-N)
#
#   supervise-jobs.sh                 run the loop (container entrypoint)
#   supervise-jobs.sh start <flow>    submit a flow now unless it is active (e.g. after a cancel)
#
# Environment: JOBMANAGER (flink-jobmanager:8081), FLOWS ("fast havale eft"), PARALLELISM (3),
# SUPERVISE_INTERVAL (30), CHECKPOINT_ROOT (/opt/flink/checkpoints), RESTORE_FROM_CHECKPOINT (true),
# plus the job settings (KAFKA_BROKERS, MATCH_TTL, ...) which are passed on to the job.
set -uo pipefail

jobmanager="${JOBMANAGER:-flink-jobmanager:8081}"
flows="${FLOWS:-fast havale eft}"
parallelism="${PARALLELISM:-3}"
interval="${SUPERVISE_INTERVAL:-30}"
checkpoint_root="${CHECKPOINT_ROOT:-/opt/flink/checkpoints}"
jar=/opt/flink/usrlib/flink-reconciler.jar

log() { echo "$(date -u +%FT%TZ) supervise-jobs: $*"; }

# Prints "<start-time> <state>" for every job named $1, newest last.
job_states() {
  curl -fsS "http://${jobmanager}/jobs/overview" 2>/dev/null \
    | tr '{' '\n' \
    | grep "\"name\":\"$1\"" \
    | sed -E 's/.*"start-time":([0-9-]+).*"state":"([A-Z_]+)".*/\1 \2/' \
    | sort -n
}

latest_checkpoint() {
  find "${checkpoint_root}/$1" -mindepth 3 -maxdepth 3 -path '*/chk-*/_metadata' -printf '%T@ %h\n' 2>/dev/null \
    | sort -n | tail -n 1 | cut -d' ' -f2-
}

submit() {
  local flow="$1" args=() latest="" keep="" dir
  if [[ "${RESTORE_FROM_CHECKPOINT:-true}" == "true" ]]; then
    latest="$(latest_checkpoint "$flow")"
  fi
  if [[ -n "$latest" ]]; then
    # NO_CLAIM: the new job takes a full first checkpoint and never reads the old files again, so
    # the directories of older runs (all except the one restored from) can be deleted.
    keep="$(dirname "$latest")"
    for dir in "${checkpoint_root}/${flow}"/*/; do
      dir="${dir%/}"
      if [[ "$dir" != "$keep" && "$(basename "$dir")" =~ ^[0-9a-f]{32}$ ]]; then
        rm -rf -- "$dir"
      fi
    done
    args+=(--fromSavepoint "$latest" --restoreMode NO_CLAIM --allowNonRestoredState)
    log "flow=${flow} submitting, restoring from ${latest}"
  else
    log "flow=${flow} submitting with empty state (no completed checkpoint)"
  fi
  mkdir -p "${checkpoint_root}/${flow}"
  # main() runs here (session mode), so FLOW and the other settings are read from this environment.
  FLOW="$flow" CHECKPOINT_DIR="file://${checkpoint_root}/${flow}" \
    /opt/flink/bin/flink run --detached --jobmanager "$jobmanager" --parallelism "$parallelism" \
    "${args[@]}" "$jar" 2>&1 | grep -E "Job has been submitted|JobID|Exception|Error" | sed "s/^/  /"
}

ensure() {
  local flow="$1" force="${2:-false}" name="flink-$1-reconciler" newest state
  newest="$(job_states "$name" | tail -n 1)"
  state="${newest#* }"
  case "$state" in
    CREATED|INITIALIZING|RUNNING|RESTARTING|FAILING|CANCELLING|RECONCILING|SUSPENDED)
      [[ "$force" == "true" ]] && log "flow=${flow} is already ${state}"
      return ;;
    CANCELED)
      if [[ "$force" != "true" ]]; then return; fi ;;
  esac
  submit "$flow"
}

wait_for_jobmanager() {
  until curl -fsS "http://${jobmanager}/overview" >/dev/null 2>&1; do sleep 2; done
}

if [[ "${1:-}" == "start" ]]; then
  [[ -n "${2:-}" ]] || { echo "usage: $0 start <flow>" >&2; exit 2; }
  wait_for_jobmanager
  ensure "$2" true
  exit 0
fi

log "supervising flows: ${flows} on ${jobmanager} every ${interval}s"
while true; do
  wait_for_jobmanager
  for flow in $flows; do
    ensure "$flow"
  done
  sleep "$interval"
done
