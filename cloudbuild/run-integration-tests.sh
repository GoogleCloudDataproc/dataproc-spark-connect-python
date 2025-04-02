#!/usr/bin/env bash

function main() {
    run_tests && touch /workspace/integration-tests.SUCCESS
}

function run_tests() {
    pytest -n 10 --log-cli-level=DEBUG tests/integration
}

main "$@"
