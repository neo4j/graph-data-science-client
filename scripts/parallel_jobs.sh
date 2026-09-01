# Job-control helpers for bash recipes that run commands concurrently.
#
# Source this from a recipe script (`. scripts/parallel_jobs.sh`), launch tracked
# background jobs with `spawn CMD...`, and block on all of them with `wait_jobs`.
# Traps are installed on source so that SIGINT/SIGTERM (which `just` exits on /
# forwards to this script) and the `set -e` failure path stop and reap any jobs
# still running instead of orphaning them (e.g. stray `docker pull` clients).

PARALLEL_JOB_PIDS=()
PARALLEL_JOB_CMDS=()

spawn() {
    PARALLEL_JOB_CMDS+=("$*")
    "$@" & PARALLEL_JOB_PIDS+=($!)
}

wait_jobs() {
    [ "${#PARALLEL_JOB_PIDS[@]}" -gt 0 ] || return 0
    local pid index=0 status
    for pid in "${PARALLEL_JOB_PIDS[@]}"; do
        status=0
        wait "${pid}" || status=$?
        if [ "${status}" -ne 0 ]; then
            echo "parallel job failed (exit ${status}): ${PARALLEL_JOB_CMDS[$index]}" >&2
            return "${status}"
        fi
        index=$((index + 1))
    done
}

_parallel_jobs_cleanup() {
    [ "${#PARALLEL_JOB_PIDS[@]}" -gt 0 ] || return 0
    kill "${PARALLEL_JOB_PIDS[@]}" 2>/dev/null || true
    sleep 0.5
    kill -9 "${PARALLEL_JOB_PIDS[@]}" 2>/dev/null || true
    local pid
    for pid in "${PARALLEL_JOB_PIDS[@]}"; do
        wait "${pid}" 2>/dev/null || true
    done
}
trap _parallel_jobs_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
