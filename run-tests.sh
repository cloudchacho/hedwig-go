#!/usr/bin/env bash

set -exo pipefail

cleanup() {
  kill -2 -- "-${emulator_pid}"
  wait "${emulator_pid}" || true
}

setup_pubsub_emulator() {
  gcloud beta emulators pubsub start --project emulator-project > /dev/null 2>&1 &
  emulator_pid=$!
  trap cleanup EXIT

  $(gcloud beta emulators pubsub env-init)
}

go test -cover -coverprofile coverage.txt -v -race ./...
cd aws && go test -cover -coverprofile coverage.txt -v -race ./... && cd -
cd jsonschema && go test -cover -coverprofile coverage.txt -v -race ./... && cd -
cd protobuf && go test -cover -coverprofile coverage.txt -v -race ./... && cd -
setup_pubsub_emulator
cd gcp && go test -cover -coverprofile coverage.txt -v -race ./... && cd -
