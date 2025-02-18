#!/usr/bin/env bash

# TODO: Consider rewriting all of these test drivers into a single python script
# to share names and logic.

set -euo pipefail

readonly TESTS=(
    'unit-tests'
    'integration-tests'
)

function main() {
    #echo "/workspace contents:"
    #ls -R /workspace
    local failed_tests=()
    local sentinel=""
    for test in "${TESTS[@]}" ; do
        # sentinel="/tmp/${test}.SUCCESS"
        sentinel="/workspace/${test}.SUCCESS"
        if [ ! -f "$sentinel" ] ; then
            failed_tests+=("$test")
        fi
    done
    if [ "${#failed_tests[@]}" -gt 0 ] ; then
        echo "failed tests: ${failed_tests[@]}"
        return 1
    fi
    # echo ""
    # echo "BUILD RESULTS:"
    # describe_build
    # describe_build | verify_statuses
    # jq_program
}

function describe_build() {
    gcloud builds describe \
        --project "$GOOGLE_CLOUD_PROJECT" \
        --format=json \
        "$BUILD_ID"
}

function verify_statuses() {
    jq -f <(jq_program)
}

function jq_program() {
    cat <<'EOF'
reduce
    (.steps[] | select(.allowFailure)) as $step
    (
        [];
        if $step.status != "SUCCESS" then . + [$step.id] else . end
    ) |
    if length > 0 then error("the following steps failed: " + (join(", "))) else empty end
EOF
}

main "$@"
