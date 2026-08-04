#!/usr/bin/env bash
# Runs every end-to-end test file in its own pytest process.
#
# Discovery is derived from the filesystem: a hand-enumerated file list
# silently stops covering whatever is added next, and the workflow stays
# green when it happens. Each file gets its own process because e2e files
# cannot share one — worker fleets and module state leak across files.
# Worker cleanup runs after every file, passing or failing, so a failed
# file cannot leak live workers into the next one.
#
# Usage: scripts/run_e2e_suite.sh [pytest args...]
#   Every argument is passed through to each per-file pytest invocation
#   (coverage supplies --cov-append and friends this way).
#
# Exit status: nonzero if any file failed; failures are accumulated, not
# fail-fast, so one broken file does not hide the state of the rest.

set -u

E2E_DIR="tests/e2e"

# Excluded from the normal-topology run. Each entry documents the job that
# owns it instead. Existence is asserted so a renamed or deleted file fails
# here loudly instead of silently vanishing from the job that runs it.
EXCLUDED=(
    "tests/e2e/test_pgbouncer_smoke.py"  # PgBouncer topology; dedicated job
)

for excluded in "${EXCLUDED[@]}"; do
    if [[ ! -f "${excluded}" ]]; then
        echo "error: excluded path ${excluded} does not exist; update the" \
            "exclusion list in ${BASH_SOURCE[0]}" >&2
        exit 1
    fi
done

is_excluded() {
    local candidate="$1"
    local excluded
    for excluded in "${EXCLUDED[@]}"; do
        if [[ "${candidate}" == "${excluded}" ]]; then
            return 0
        fi
    done
    return 1
}

cleanup_workers() {
    pkill -TERM -f "horsies worker" 2>/dev/null || true
    sleep 5
}

shopt -s nullglob
discovered=("${E2E_DIR}"/test_*.py)
shopt -u nullglob

if [[ "${#discovered[@]}" -eq 0 ]]; then
    echo "error: no e2e test files discovered under ${E2E_DIR}" >&2
    exit 1
fi

failed=()
ran=0
for file in "${discovered[@]}"; do
    if is_excluded "${file}"; then
        echo "excluded: ${file} (runs in its dedicated job)"
        continue
    fi
    echo "::group::${file}"
    uv run pytest "${file}" "$@"
    status=$?
    cleanup_workers
    echo "::endgroup::"
    if [[ "${status}" -ne 0 ]]; then
        failed+=("${file}")
        echo "FAILED: ${file} (exit ${status})"
    fi
    ran=$((ran + 1))
done

echo "e2e suite: ${ran} files run, ${#failed[@]} failed, ${#EXCLUDED[@]} excluded"
if [[ "${#failed[@]}" -ne 0 ]]; then
    printf 'failed: %s\n' "${failed[@]}"
    exit 1
fi
