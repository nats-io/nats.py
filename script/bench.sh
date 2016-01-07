#!/bin/sh
#
# Simple script to generate table with benchmarks.
#
# Requires running NATS server with `gnatsd -m 8225 -p 4225`
#
#
echo "*** PUB using Asyncio NATS client."
echo
echo "| messages | bytes | duration | msgs/sec | total_written | timeouts | errors | varz in msgs| varz in bytes|"
for nbytes in 1 10 100 1000 10000 10000; do 
  for messages in 100 1000 10000 100000 ; do
    python benchmarks/publish.py $messages $nbytes;  2> /dev/null
  done;
done
echo
echo

echo "*** PUB using Asyncio NATS client (larger messages)"
echo
echo "| messages | bytes | duration | msgs/sec | total_written | timeouts | errors | varz in msgs| varz in bytes|"
for nbytes in 100000 1000000; do 
  for messages in 100 1000; do
    python benchmarks/publish.py $messages $nbytes;  2> /dev/null
  done;
done
echo
echo

echo "*** Request/Response using Asyncio NATS client."
echo
echo "| messages | bytes | duration | msgs/sec | total_written | timeouts | errors | varz in msgs| varz in bytes|"
for nbytes in 1 10 100 1000 10000 10000 100000; do 
  for messages in 100 1000 10000; do
    python benchmarks/request-response.py $messages $nbytes;  2> /dev/null
  done;
done
echo
echo

echo "*** Server roundtrip (ping/pong) latency using Asyncio NATS client."
echo
echo "| messages | max_latency | duration | msgs/sec | total_written | timeouts | errors | varz in msgs| varz in bytes|"
for max_latency in 0.5 0.4 0.3 0.2 0.1 0.05 0.04 0.03 0.02 0.01 0.005 0.004 0.003 0.002 0.001 0.0005 0.0004 0.0003 0.0002 0.0001 0.00005; do
  for messages in 100 1000 10000; do
    python benchmarks/flush-timeout.py $messages $max_latency;  2> /dev/null
  done;
done
echo
echo
