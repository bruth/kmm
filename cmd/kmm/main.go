package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/bruth/kmm"
	"github.com/bruth/rita"
	"github.com/bruth/rita/testutil"
	"github.com/bruth/rita/types"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"github.com/urfave/cli/v2"
)

var (
	defaultRequestTimeout = 5 * time.Second

	// Initialize the type registry with the application/domain types.
	tr, _ = types.NewRegistry(kmm.Types)

	app = &cli.App{
		Name:  "kmm",
		Usage: "Kids money manager.",
		Commands: []*cli.Command{
			serve,
			deposit,
			withdraw,
			setBudget,
			removeBudget,
			currentBalance,
			lastBudgetPeriod,
			ledger,
		},
	}

	natsFlags = []cli.Flag{
		&cli.StringFlag{
			Name:    "nats.url",
			Value:   "",
			Usage:   "NATS server URL(s).",
			EnvVars: []string{"NATS_URL"},
		},
		&cli.StringFlag{
			Name:    "nats.creds",
			Value:   "",
			Usage:   "NATS credentials file.",
			EnvVars: []string{"NATS_CREDS"},
		},
		&cli.StringFlag{
			Name:    "nats.context",
			Value:   natscontext.SelectedContext(),
			Usage:   "NATS context name.",
			EnvVars: []string{"NATS_CONTEXT"},
		},
	}

	serve = &cli.Command{
		Name:  "serve",
		Usage: "Run the server.",
		Flags: append([]cli.Flag{
			&cli.BoolFlag{
				Name:    "nats.embed",
				Value:   false,
				Usage:   "Run NATS as an embedded server for testing.",
				EnvVars: []string{"NATS_EMBED"},
			},
			&cli.StringFlag{
				Name:    "http.addr",
				Value:   "127.0.0.1:8080",
				Usage:   "HTTP bind address.",
				EnvVars: []string{"HTTP_ADDR"},
			},
		}, natsFlags...),
		Action: func(c *cli.Context) error {
			return runServer(c)
		},
	}

	deposit = &cli.Command{
		Name:      "deposit",
		Usage:     "Deposit money into an account.",
		Flags:     natsFlags,
		ArgsUsage: "<account> <amount> [<description>]",
		Action: func(c *cli.Context) error {
			n := c.NArg()
			if n < 2 {
				return fmt.Errorf("account and amount are required")
			} else if n > 3 {
				return fmt.Errorf("at most three arguments are supported")
			}

			account := c.Args().Get(0)
			amount := c.Args().Get(1)
			description := c.Args().Get(2)

			nc, err := connectNats(c)
			if err != nil {
				return err
			}
			defer nc.Drain() //nolint

			subject := fmt.Sprintf("kmm.services.%s.deposit-funds", account)
			data, _ := json.Marshal(map[string]string{
				"Amount":      amount,
				"Description": description,
			})

			rep, err := nc.Request(subject, data, defaultRequestTimeout)
			if err != nil {
				return err
			}
			if len(rep.Data) > 0 {
				fmt.Println(string(rep.Data))
			}
			return nil
		},
	}

	withdraw = &cli.Command{
		Name:      "withdraw",
		Usage:     "Withdraw money from an account.",
		Flags:     natsFlags,
		ArgsUsage: "<account> <amount> [<description>]",
		Action: func(c *cli.Context) error {
			n := c.NArg()
			if n < 2 {
				return fmt.Errorf("account and amount are required")
			} else if n > 3 {
				return fmt.Errorf("at most three arguments are supported")
			}

			account := c.Args().Get(0)
			amount := c.Args().Get(1)
			description := c.Args().Get(2)

			nc, err := connectNats(c)
			if err != nil {
				return err
			}
			defer nc.Drain() //nolint

			subject := fmt.Sprintf("kmm.services.%s.withdraw-funds", account)
			data, _ := json.Marshal(map[string]string{
				"Amount":      amount,
				"Description": description,
			})

			rep, err := nc.Request(subject, data, defaultRequestTimeout)
			if err != nil {
				return err
			}
			if len(rep.Data) > 0 {
				fmt.Println(string(rep.Data))
			}
			return nil
		},
	}

	setBudget = &cli.Command{
		Name:      "set-budget",
		Usage:     "Set a budget on an account.",
		Flags:     natsFlags,
		ArgsUsage: "<account> <amount> <period>",
		Action: func(c *cli.Context) error {
			n := c.NArg()
			if n != 3 {
				return fmt.Errorf("account, amount, and period are required")
			}

			account := c.Args().Get(0)
			amount := c.Args().Get(1)
			period := c.Args().Get(2)

			nc, err := connectNats(c)
			if err != nil {
				return err
			}
			defer nc.Drain() //nolint

			subject := fmt.Sprintf("kmm.services.%s.set-budget", account)
			data, _ := json.Marshal(map[string]string{
				"MaxAmount": amount,
				"Period":    period,
			})

			rep, err := nc.Request(subject, data, defaultRequestTimeout)
			if err != nil {
				return err
			}
			if len(rep.Data) > 0 {
				fmt.Println(string(rep.Data))
			}
			return nil
		},
	}

	removeBudget = &cli.Command{
		Name:      "remove-budget",
		Usage:     "Removes a budget from an account.",
		Flags:     natsFlags,
		ArgsUsage: "<account>",
		Action: func(c *cli.Context) error {
			n := c.NArg()
			if n != 1 {
				return fmt.Errorf("account required")
			}

			account := c.Args().Get(0)

			nc, err := connectNats(c)
			if err != nil {
				return err
			}
			defer nc.Drain() //nolint

			subject := fmt.Sprintf("kmm.services.%s.remove-budget", account)
			rep, err := nc.Request(subject, []byte{}, defaultRequestTimeout)
			if err != nil {
				return err
			}
			if len(rep.Data) > 0 {
				fmt.Println(string(rep.Data))
			}
			return nil
		},
	}

	currentBalance = &cli.Command{
		Name:      "balance",
		Usage:     "Gets the current balance for an account.",
		Flags:     natsFlags,
		ArgsUsage: "<account>",
		Action: func(c *cli.Context) error {
			n := c.NArg()
			if n != 1 {
				return fmt.Errorf("account required")
			}

			account := c.Args().Get(0)

			nc, err := connectNats(c)
			if err != nil {
				return err
			}
			defer nc.Drain() //nolint

			subject := fmt.Sprintf("kmm.services.%s.balance", account)
			rep, err := nc.Request(subject, []byte{}, defaultRequestTimeout)
			if err != nil {
				return err
			}
			v, err := tr.UnmarshalType(rep.Data, "current-funds")
			if err != nil {
				return err
			}
			funds, _ := v.(*kmm.CurrentFunds)
			fmt.Println(funds.Amount)
			return nil
		},
	}

	ledger = &cli.Command{
		Name:      "ledger",
		Usage:     "Subscribes to the account ledger.",
		Flags:     natsFlags,
		ArgsUsage: "<account>",
		Action: func(c *cli.Context) error {
			n := c.NArg()
			if n != 1 {
				return fmt.Errorf("account required")
			}

			account := c.Args().Get(0)

			nc, err := connectNats(c)
			if err != nil {
				return err
			}
			defer nc.Drain() //nolint

			rt, _ := rita.New(nc, rita.TypeRegistry(tr))

			streamID := nuid.Next()
			streamSubject := fmt.Sprintf("kmm.streams.%s", streamID)

			sub, err := nc.Subscribe(streamSubject, func(msg *nats.Msg) {
				event, err := rt.UnpackEvent(msg)
				if err != nil {
					log.Print(err)
					return
				}

				switch e := event.Data.(type) {
				case *kmm.FundsDeposited:
					if e.Description == "" {
						fmt.Printf("+%s | %s\n", e.Amount, e.Time.Format(time.ANSIC))
					} else {
						fmt.Printf("+%s | %s | %s\n", e.Amount, e.Time.Format(time.ANSIC), e.Description)
					}
				case *kmm.FundsWithdrawn:
					if e.Description == "" {
						fmt.Printf("-%s | %s\n", e.Amount, e.Time.Format(time.ANSIC))
					} else {
						fmt.Printf("-%s | %s | %s\n", e.Amount, e.Time.Format(time.ANSIC), e.Description)
					}
				}
			})
			if err != nil {
				return fmt.Errorf("ledger-subscribe: %w", err)
			}
			defer sub.Unsubscribe() //nolint

			subject := fmt.Sprintf("kmm.services.%s.ledger", account)
			_, err = nc.Request(subject, []byte(fmt.Sprintf(`{"id": "%s"}`, streamID)), defaultRequestTimeout)
			if err != nil {
				return fmt.Errorf("ledger-request: %w", err)
			}

			sigch := make(chan os.Signal, 1)
			signal.Notify(sigch, os.Interrupt)
			<-sigch

			return nil
		},
	}

	lastBudgetPeriod = &cli.Command{
		Name:      "last-budget-period",
		Usage:     "Gets the summary for the last active budget period.",
		Flags:     natsFlags,
		ArgsUsage: "<account>",
		Action: func(c *cli.Context) error {
			n := c.NArg()
			if n != 1 {
				return fmt.Errorf("account required")
			}

			account := c.Args().Get(0)

			nc, err := connectNats(c)
			if err != nil {
				return err
			}
			defer nc.Drain() //nolint

			subject := fmt.Sprintf("kmm.services.%s.last-budget-period", account)
			rep, err := nc.Request(subject, []byte{}, defaultRequestTimeout)
			if err != nil {
				return err
			}
			v, err := tr.UnmarshalType(rep.Data, "budget-period")
			if err != nil {
				return err
			}
			s, _ := v.(*kmm.BudgetPeriod)

			if s.PolicyMaxWithdrawAmount.IsZero() {
				fmt.Println("no budget set")
				return nil
			}

			fmt.Printf(`period start: %s
period end: %s
withdrawals: %d
total withdrawn: %s
`, s.PeriodStartTime.Format(time.ANSIC), s.NextPeriodStartTime.Format(time.ANSIC), s.WithdrawalsInPeriod, s.FundsWithdrawnInPeriod)
			return nil
		},
	}
)

func connectNats(c *cli.Context) (*nats.Conn, error) {
	natsUrl := c.String("nats.url")
	natsCreds := c.String("nats.creds")
	natsContext := c.String("nats.context")

	// Setup NATS connection depending on the values available.
	if natsCreds == "" && os.Getenv("NATS_CREDS_B64") != "" {
		// Hack to get the get the creds file content as a Fly.io secret..
		var err error
		natsCreds, err = decodeUserCredsToFile(os.Getenv("NATS_CREDS_B64"))
		if err != nil {
			return nil, err
		}
	}

	var copts []nats.Option
	if natsCreds != "" {
		copts = append(copts, nats.UserCredentials(natsCreds))
	}

	if natsContext != "" {
		return natscontext.Connect(natsContext, copts...)
	}

	return nats.Connect(natsUrl, copts...)
}

func main() {
	if err := app.Run(os.Args); err != nil {
		log.SetFlags(0)
		log.Print(err)
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

func runServer(c *cli.Context) error {
	natsEmbed := c.Bool("nats.embed")
	httpAddr := c.String("http.addr")

	var (
		nc  *nats.Conn
		err error
	)

	if natsEmbed {
		ns := testutil.NewNatsServer(4837)
		defer ns.Shutdown()
		nc, err = nats.Connect(ns.ClientURL())
	} else {
		nc, err = connectNats(c)
	}
	if err != nil {
		return err
	}
	defer nc.Drain() //nolint

	js, err := nc.JetStream()
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
		_ = es.Delete()
	}
	err = es.Create(&nats.StreamConfig{
		Subjects: []string{"kmm.events.>"},
		MaxBytes: 512 * 1000 * 1000, // 512MiB
	})
	if err != nil {
		return err
	}

	handleCommand := func(ctx context.Context, msg *nats.Msg, account, operation string) (any, error) {
		// Unmarshal the command based on the type.
		cmd, err := tr.UnmarshalType(msg.Data, operation)
		if err != nil {
			if err == types.ErrTypeNotRegistered {
				return nil, fmt.Errorf("unknown command: %s", operation)
			}
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

	handleBudgetSummaryQuery := func(ctx context.Context, msg *nats.Msg, account string) (any, error) {
		var s kmm.BudgetPeriod

		subject := fmt.Sprintf("kmm.events.accounts.%s", account)
		_, err := es.Evolve(ctx, subject, &s)
		if err != nil {
			return nil, err
		}

		return &s, nil
	}

	handleLedgerQuery := func(ctx context.Context, msg *nats.Msg, account string) (any, error) {
		var m map[string]string
		_ = json.Unmarshal(msg.Data, &m)
		subject := fmt.Sprintf("kmm.streams.%s", m["id"])

		_, err := js.AddConsumer("kmm", &nats.ConsumerConfig{
			DeliverSubject:    subject,
			DeliverPolicy:     nats.DeliverAllPolicy,
			FilterSubject:     fmt.Sprintf("kmm.events.accounts.%s", account),
			InactiveThreshold: 5 * time.Second,
			AckPolicy:         nats.AckNonePolicy,
		})
		if err != nil {
			return nil, err
		}

		return json.Marshal(map[string]string{
			"subject": subject,
		})
	}

	respondMsg := func(msg *nats.Msg, result any, err error) {
		if err != nil {
			_ = msg.Respond([]byte(err.Error()))
			return
		}

		if result == nil {
			_ = msg.Respond(nil)
			return
		}

		// If bytes, respond directly.
		if b, ok := result.([]byte); ok {
			_ = msg.Respond(b)
			return
		}

		// Otherwise assume its part of the type registry.
		b, err := tr.Marshal(result)
		if err != nil {
			_ = msg.Respond([]byte(err.Error()))
		} else {
			_ = msg.Respond(b)
		}
	}

	// Service to handle services (request/reply).
	sub1, err := nc.QueueSubscribe("kmm.services.*.*", "services", func(msg *nats.Msg) {
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
		// Commands.
		case "deposit-funds", "withdraw-funds", "set-budget", "remove-budget":
			result, err = handleCommand(ctx, msg, account, operation)

		// Queries.
		case "balance":
			result, err = handleCurrentFundsQuery(ctx, msg, account)

		case "last-budget-period":
			result, err = handleBudgetSummaryQuery(ctx, msg, account)

		case "ledger":
			result, err = handleLedgerQuery(ctx, msg, account)

		default:
			err = errors.New("unknown service operation")
		}

		// Respond with result, error, or nil.
		respondMsg(msg, result, err)
	})
	if err != nil {
		return err
	}
	defer sub1.Unsubscribe() //nolint

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		msg := fmt.Sprintf(`Kids Money Manager - hosted on Fly.io, connected with Synadia's NGS
	Connect %s
`, nc.ConnectedUrl())
		w.Write([]byte(msg)) //nolint
	})

	return http.ListenAndServe(httpAddr, nil)
}
