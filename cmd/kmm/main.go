package main

import (
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/bruth/kmm"
	"github.com/bruth/rita"
	"github.com/bruth/rita/testutil"
	"github.com/bruth/rita/types"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func decodeUserCredsToFile(s string) (string, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}
	f, err := ioutil.TempFile("", "")
	if err != nil {
		return "", err
	}
	_, err = f.Write(b)
	if err != nil {
		return "", err
	}
	return f.Name(), f.Close()
}

func run() error {
	var (
		natsUrl     string
		natsCreds   string
		natsContext string
		natsEmbed   bool
		httpAddr    string
	)

	flag.StringVar(&natsUrl, "nats.url", "", "NATS connection URL.")
	flag.StringVar(&natsCreds, "nats.creds", "", "NATS user credentials file.")
	flag.StringVar(&natsContext, "nats.context", "", "NATS context.")
	flag.BoolVar(&natsEmbed, "nats.embed", false, "NATS embedded mode.")
	flag.StringVar(&httpAddr, "http.addr", "0.0.0.0:8080", "HTTP address.")

	flag.Parse()

	var (
		nc  *nats.Conn
		err error
	)

	if natsEmbed {
		ns := testutil.NewNatsServer()
		defer ns.Shutdown()

		nc, err = nats.Connect(ns.ClientURL())
	} else {
		// Setup NATS connection depending on the values available.
		if natsUrl == "" {
			natsUrl = os.Getenv("NATS_URL")
		}
		if natsCreds == "" {
			// Hack to get the get the creds file content as a Fly.io secret..
			var err error
			natsCreds, err = decodeUserCredsToFile(os.Getenv("NATS_CREDS_B64"))
			if err != nil {
				return err
			}
		}

		var copts []nats.Option
		if natsCreds != "" {
			copts = append(copts, nats.UserCredentials(natsCreds))
		}

		if natsContext != "" {
			nc, err = natscontext.Connect(natsContext, copts...)
		} else {
			nc, err = nats.Connect(natsUrl, copts...)
		}
	}

	if err != nil {
		return err
	}
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		return err
	}

	// Initialize the type registry with the application/domain types.
	tr, err := types.NewRegistry(kmm.Types)
	if err != nil {
		return err
	}

	// Initialize a new Rita instance.
	rt, err := rita.New(nc, rita.TypeRegistry(tr))
	if err != nil {
		return err
	}

	// Create an event store. (this is idempotent)
	es := rt.EventStore("kmm")
	if natsEmbed {
		es.Delete()
	}
	err = es.Create(&rita.EventStoreConfig{Subjects: []string{"kmm.events.>"}})
	if err != nil {
		return err
	}

	// Emulate taking a private events and re-publishing them via a public subject.
	// Typically a new type/payload can be used with more enrichment.
	sub, err := js.QueueSubscribe("kmm.events.accounts.*", "live-ledger", func(msg *nats.Msg) {
		defer msg.Ack()

		idx := strings.LastIndexByte(msg.Subject, '.')
		account := msg.Subject[idx+1:]

		event, err := es.UnpackEvent(msg)
		if err != nil {
			log.Print(err)
			return
		}

		switch event.Data.(type) {
		case *kmm.FundsDeposited:
		case *kmm.FundsWithdrawn:
		default:
			return
		}

		subject := fmt.Sprintf("kmm.streams.%s.ledger", account)
		nc.Publish(subject, msg.Data)
	}, nats.BindStream("kmm"))
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	handleCommand := func(ctx context.Context, msg *nats.Msg, account, operation string) (any, error) {
		// Unmarshal the command based on the type.
		cmd, err := tr.UnmarshalType(msg.Data, operation)
		if err != nil {
			return nil, err
		}

		if v, ok := cmd.(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return nil, err
			}
		}

		subject := fmt.Sprintf("kmm.events.accounts.%s", account)

		// Initialize the aggregate and evolve the state.
		a := kmm.NewAccount()
		seq, err := es.Evolve(ctx, subject, a)
		if err != nil {
			return nil, err
		}

		// Decide if accepted and the resulting events.
		// TODO: extract out additional headers as command fields, e.g. rita-command-id
		events, err := a.Decide(&rita.Command{
			Data: cmd,
		})
		if err != nil {
			return nil, err
		}

		// Append new events.
		_, err = es.Append(ctx, subject, events, rita.ExpectSequence(seq))
		if err != nil {
			return nil, err
		}

		return nil, nil
	}

	handleCurrentFundsQuery := func(ctx context.Context, msg *nats.Msg, account string) (any, error) {
		var s kmm.CurrentFunds

		subject := fmt.Sprintf("kmm.events.accounts.%s", account)
		_, err := es.Evolve(ctx, subject, &s)
		if err != nil {
			return nil, err
		}

		return &s, nil
	}

	handlePeriodSummaryQuery := func(ctx context.Context, msg *nats.Msg, account string) (any, error) {
		var s kmm.PeriodSummary

		subject := fmt.Sprintf("kmm.events.accounts.%s", account)
		_, err := es.Evolve(ctx, subject, &s)
		if err != nil {
			return nil, err
		}

		return &s, nil
	}

	respondMsg := func(msg *nats.Msg, result any, err error) {
		if err != nil {
			msg.Respond([]byte(err.Error()))
			return
		}

		if result != nil {
			b, err := tr.Marshal(result)
			if err != nil {
				msg.Respond([]byte(err.Error()))
			} else {
				msg.Respond(b)
			}
			return
		}

		msg.Respond(nil)
	}

	// Service to handle commands.
	sub1, err := nc.QueueSubscribe("kmm.commands.*.*", "commands", func(msg *nats.Msg) {
		ctx := context.Background()

		// Extract out account and command from subject.
		toks := strings.Split(msg.Subject, ".")

		// Parse out the account ID and operation.
		account := toks[2]
		operation := toks[3]

		result, err := handleCommand(ctx, msg, account, operation)

		// Respond with result, error, or nil.
		respondMsg(msg, result, err)
	})
	if err != nil {
		return err
	}
	defer sub1.Unsubscribe()

	// Service to handle queries.
	sub2, err := nc.QueueSubscribe("kmm.queries.*.*", "commands", func(msg *nats.Msg) {
		ctx := context.Background()

		// Extract out account and command from subject.
		toks := strings.Split(msg.Subject, ".")

		// Parse out the account ID and operation.
		account := toks[2]
		operation := toks[3]

		var (
			result any
			err    error
		)

		switch operation {
		// Queries.
		case "current-funds":
			result, err = handleCurrentFundsQuery(ctx, msg, account)

		case "period-summary":
			result, err = handlePeriodSummaryQuery(ctx, msg, account)

		default:
			err = errors.New("unknown query")
		}

		// Respond with result, error, or nil.
		respondMsg(msg, result, err)
	})
	if err != nil {
		return err
	}
	defer sub2.Unsubscribe()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		msg := fmt.Sprintf(`Kids Money Manager - hosted on Fly.io, connected with Synadia's NGS
	Connect %s
`, nc.ConnectedUrl())
		w.Write([]byte(msg))
	})

	return http.ListenAndServe(httpAddr, nil)
}
