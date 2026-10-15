// Run from a checkout of nats.go: go run /path/to/pull_subscribe.go nats://127.0.0.1:4222
// This probe uses only the upstream client; never derive its output from nats.py.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	nats "github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(os.Args[1])
	must(err)
	defer nc.Close()
	js, err := nc.JetStream()
	must(err)
	results := map[string]any{}
	for _, label := range []string{"ephemeral", "durable", "config_durable", "named", "existing", "bound", "all_subjects", "subject_required", "subject_mismatch", "push_config", "push_existing", "ack_mismatch", "max_waiting_mismatch", "missing_bound"} {
		_, err = js.AddStream(&nats.StreamConfig{Name: "PULL", Subjects: []string{"events", "other"}})
		must(err)
		subject, durable := "events", ""
		opts := []nats.SubOpt{nats.BindStream("PULL")}
		switch label {
		case "ephemeral":
		case "durable":
			durable = "worker"
		case "config_durable":
			opts = append(opts, nats.Durable("worker"))
		case "named":
			opts = append(opts, nats.ConsumerName("named"))
		case "all_subjects":
			subject = ""
		case "subject_required":
			subject = ""
			opts = nil
		case "push_config":
			opts = append(opts, nats.DeliverSubject("delivery"))
		case "missing_bound":
			opts = append(opts, nats.Bind("PULL", "missing"))
		default:
			cfg := &nats.ConsumerConfig{Durable: "worker", FilterSubject: "events", AckPolicy: nats.AckExplicitPolicy, MaxWaiting: 20}
			if label == "push_existing" {
				cfg.DeliverSubject = "delivery"
				cfg.MaxWaiting = 0
			}
			_, err = js.AddConsumer("PULL", cfg)
			must(err)
			durable = "worker"
			switch label {
			case "bound":
				durable = ""
				opts = append(opts, nats.Bind("PULL", "worker"))
			case "subject_mismatch":
				subject = "other"
			case "ack_mismatch":
				opts = append(opts, nats.AckNone())
			case "max_waiting_mismatch":
				opts = append(opts, nats.PullMaxWaiting(10))
			}
		}
		sub, err := js.PullSubscribe(subject, durable, opts...)
		observation := map[string]any{}
		if err != nil {
			observation["error"] = err.Error()
			observation["consumer_not_found"] = errors.Is(err, nats.ErrConsumerNotFound)
		} else {
			info, err := sub.ConsumerInfo()
			must(err)
			observation["durable"] = info.Config.Durable
			observation["ack_policy"] = info.Config.AckPolicy
			observation["filter_subject"] = info.Config.FilterSubject
			if label == "named" {
				observation["name"] = info.Name
			}
			must(sub.Unsubscribe())
			_, err = js.ConsumerInfo("PULL", info.Name)
			if err != nil && !errors.Is(err, nats.ErrConsumerNotFound) {
				must(err)
			}
			observation["retained"] = err == nil
		}
		results[label] = observation
		must(js.DeleteStream("PULL"))
	}
	out, err := json.MarshalIndent(map[string]any{"client": "nats.go", "server": nc.ConnectedServerVersion(), "cases": results}, "", "  ")
	must(err)
	fmt.Println(string(out))
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
