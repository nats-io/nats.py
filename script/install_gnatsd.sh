#!/bin/bash

set -e

export DEFAULT_NATS_SERVER_VERSION=v1.0.4
export NATS_SERVER_VERSION="${NATS_SERVER_VERSION:=$DEFAULT_NATS_SERVER_VERSION}"

# check to see if gnatsd folder is empty
if [ ! "$(ls -A $HOME/nats-server)" ]; then
    (
	mkdir -p $HOME/nats-server
	cd $HOME/nats-server
	wget https://github.com/nats-io/gnatsd/releases/download/$NATS_SERVER_VERSION/gnatsd-$NATS_SERVER_VERSION-linux-amd64.zip -O nats-server.zip
	unzip nats-server.zip
	cp gnatsd-$NATS_SERVER_VERSION-linux-amd64/gnatsd $HOME/nats-server/gnatsd
    )
else
  echo 'Using cached directory.';
fi
