#!/usr/bin/env bash

function main() {
    run_tests && touch /workspace/unit-tests.SUCCESS
}

function run_tests() {
    pytest -n 10 tests/unit
}

main "$@"
