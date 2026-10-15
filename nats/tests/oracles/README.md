# PullSubscribe oracle

The expected cases are inline parameters in `../test_js_pull_subscribe.py`,
measured by `pull_subscribe.go` against unmodified nats.go commit `f666d4365aaf7b1d9e598bd04f8a5048790ba2ec`, using nats-server
2.15.0-dev. They cover 14 constructor cases, consumer configuration, and retention
after unsubscribe. The Python tests compare complete observations for these
cases, normalizing Go's acknowledgment enum names and consumer-not-found sentinel
to Python's wire values and native API exception.

To reproduce, start an isolated JetStream server with a fresh store, then run
from a nats.go checkout (the probe creates and deletes a stream named `PULL`):

```sh
go run /absolute/path/to/pull_subscribe.go nats://127.0.0.1:44222 > /tmp/pull_subscribe.json
```

Compare the probe's `cases` with the inline test parameters, translating
`AckNone`/`AckExplicit` in errors to `none`/`explicit`. Tests do not read a JSON
fixture or run Go at collection time.

The Python-only tests additionally cover preflight without network I/O, config
reuse, binding aliases and unavailable lookup, setup failure cleanup, fetch and
acknowledgment, timeout, unsubscribe, and connection-close ownership. These tests
do not establish full cross-client fetch parity, concurrent creation parity,
or parity for every configuration field. Legacy Python compatibility remains:
matching positional/config durable names are accepted; Python name validation is
stricter; supplied ConsumerConfig policy defaults count as requested settings;
API failures retain Python exception types. Bound subscriptions have no subject
argument, so they validate consumer type but do not compare a filter subject.
