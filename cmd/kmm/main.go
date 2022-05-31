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
	"strings"
	"time"

	"github.com/bruth/kmm"
	"github.com/bruth/rita"
	"github.com/bruth/rita/testutil"
	"github.com/bruth/rita/types"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
	"github.com/urfave/cli/v2"
)

var (
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

			subject := fmt.Sprintf("kmm.commands.%s.deposit-funds", account)
			data, _ := json.Marshal(map[string]string{
				"Amount":      amount,
				"Description": description,
			})

			rep, err := nc.Request(subject, data, time.Second)
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

			subject := fmt.Sprintf("kmm.commands.%s.withdraw-funds", account)
			data, _ := json.Marshal(map[string]string{
				"Amount":      amount,
				"Description": description,
			})

			rep, err := nc.Request(subject, data, time.Second)
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

			subject := fmt.Sprintf("kmm.commands.%s.set-budget", account)
			data, _ := json.Marshal(map[string]string{
				"MaxAmount": amount,
				"Period":    period,
			})

			rep, err := nc.Request(subject, data, time.Second)
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

			subject := fmt.Sprintf("kmm.commands.%s.remove-budget", account)
			rep, err := nc.Request(subject, []byte{}, time.Second)
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

			subject := fmt.Sprintf("kmm.queries.%s.current-funds", account)
			rep, err := nc.Request(subject, []byte{}, time.Second)
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

			rt, _ := rita.New(nc, rita.TypeRegistry(tr))

			subject := fmt.Sprintf("kmm.streams.%s.ledger", account)
			sub, err := nc.SubscribeSync(subject)
			if err != nil {
				return err
			}
			defer sub.Unsubscribe()

			for {
				msg, err := sub.NextMsg(time.Minute)
				switch err {
				case nats.ErrTimeout:
					continue
				case nats.ErrConnectionClosed:
					return nil
				case nats.ErrBadSubscription:
					return nil
				}

				event, err := rt.UnpackEvent(msg)
				if err != nil {
					log.Print(msg.Header)
					log.Print(err)
					continue
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
			}
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

			subject := fmt.Sprintf("kmm.queries.%s.last-budget-period", account)
			rep, err := nc.Request(subject, []byte{}, time.Second)
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
	if natsCreds == "" {
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
	err = es.Create(&rita.EventStoreConfig{Subjects: []string{"kmm.events.>"}})
	if err != nil {
		return err
	}

	// Emulate taking private events and re-publishing them to a public subject.
	// Typically a new type/payload can be used with more enrichment.
	sub, err := js.QueueSubscribe("kmm.events.accounts.*", "live-ledger", func(msg *nats.Msg) {
		idx := strings.LastIndexByte(msg.Subject, '.')
		account := msg.Subject[idx+1:]

		event, err := rt.UnpackEvent(msg)
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
		nmsg := nats.NewMsg(subject)
		nmsg.Header = msg.Header
		nmsg.Data = msg.Data
		err = nc.PublishMsg(nmsg)
		if err != nil {
			log.Print(err)
			return
		}

		err = msg.Ack()
		if err != nil {
			log.Print(err)
		}
	}, nats.BindStream("kmm"), nats.DeliverAll())
	if err != nil {
		return err
	}
	defer sub.Unsubscribe() //nolint

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

	respondMsg := func(msg *nats.Msg, result any, err error) {
		if err != nil {
			_ = msg.Respond([]byte(err.Error()))
			return
		}

		if result != nil {
			b, err := tr.Marshal(result)
			if err != nil {
				_ = msg.Respond([]byte(err.Error()))
			} else {
				_ = msg.Respond(b)
			}
			return
		}

		_ = msg.Respond(nil)
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
	defer sub1.Unsubscribe() //nolint

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

		case "last-budget-period":
			result, err = handleBudgetSummaryQuery(ctx, msg, account)

		default:
			err = errors.New("unknown query")
		}

		// Respond with result, error, or nil.
		respondMsg(msg, result, err)
	})
	if err != nil {
		return err
	}
	defer sub2.Unsubscribe() //nolint

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		msg := fmt.Sprintf(`Kids Money Manager - hosted on Fly.io, connected with Synadia's NGS
	Connect %s
`, nc.ConnectedUrl())
		w.Write([]byte(msg)) //nolint
	})

	return http.ListenAndServe(httpAddr, nil)
}
