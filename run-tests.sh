#!/usr/bin/env bash

set -exo pipefail

cleanup() {
  kill -2 -- "-${emulator_pid}"
  wait "${emulator_pid}" || true
}

setup_pubsub_emulator() {
  gcloud components install beta pubsub-emulator
  gcloud beta emulators pubsub start --project emulator-project > /dev/null 2>&1 &
  emulator_pid=$!
  trap cleanup EXIT

  # wait for emulator
  sleep 5

  $(gcloud beta emulators pubsub env-init)
}

setup_pubsub_emulator
go test -cover -coverprofile coverage.txt -v -race ./...
