#!/bin/bash

set -e

export DEFAULT_NATS_SERVER_VERSION=v2.10.26

export NATS_SERVER_VERSION="${NATS_SERVER_VERSION:=$DEFAULT_NATS_SERVER_VERSION}"

mkdir -p $HOME/nats-server
curl -sf https://binaries.nats.dev/nats-io/nats-server/v2@$NATS_SERVER_VERSION | PREFIX=$HOME/nats-server/ sh
$HOME/nats-server/nats-server -v
