#!/usr/bin/env bash

set -euo pipefail

function main() {
    describe_build | verify_statuses
    jq_program
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
    .steps[] as $step
    (
        [];
        if $step.status != "SUCCESS" then . + [$step.id] else . end
    ) |
    if length > 0 then error("the following steps failed: " + (join(", "))) else empty end
EOF
}

main "$@"
