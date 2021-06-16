#!/usr/bin/env bash

set -exo pipefail

cleanup() {
  kill -2 -- "-${emulator_pid}"
  wait "${emulator_pid}" || true
}

setup_pubsub_emulator() {
  gcloud beta emulators pubsub start --project emulator-project --quiet > /dev/null 2>&1 &
  emulator_pid=$!
  trap cleanup EXIT

  $(gcloud beta emulators pubsub env-init)
}

setup_pubsub_emulator
go test -cover -coverprofile coverage.txt -v -race ./...
