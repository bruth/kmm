package kmm

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/bruth/rita"
	"github.com/bruth/rita/testutil"
	"github.com/shopspring/decimal"
)

func prettyPrint(t *testing.T, v any) {
	b, _ := json.MarshalIndent(v, "", " ")
	t.Log(string(b))
}

func TestPeriodWindow(t *testing.T) {
	is := testutil.NewIs(t)

	pt := time.Date(2019, time.May, 3, 12, 20, 30, 0, time.UTC)

	tests := map[Period]struct {
		StartTime     time.Time
		NextStartTime time.Time
	}{
		Minute: {
			time.Date(2019, time.May, 3, 12, 20, 0, 0, time.UTC),
			time.Date(2019, time.May, 3, 12, 21, 0, 0, time.UTC),
		},
		Daily: {
			time.Date(2019, time.May, 3, 0, 0, 0, 0, time.UTC),
			time.Date(2019, time.May, 4, 0, 0, 0, 0, time.UTC),
		},
		Weekly: {
			time.Date(2019, time.April, 29, 0, 0, 0, 0, time.UTC),
			time.Date(2019, time.May, 6, 0, 0, 0, 0, time.UTC),
		},
		Monthly: {
			time.Date(2019, time.May, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2019, time.June, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for key, test := range tests {
		t.Run(string(key), func(t *testing.T) {
			st, nst := periodWindow(pt, key)
			is.Equal(st, test.StartTime)
			is.Equal(nst, test.NextStartTime)
		})
	}
}

func TestAccount(t *testing.T) {
	is := testutil.NewIs(t)

	ten, _ := decimal.NewFromString("10")
	twenty, _ := decimal.NewFromString("20")
	thirty, _ := decimal.NewFromString("30")

	t.Run("deposit-funds", func(t *testing.T) {
		clock := testutil.NewClock(time.Minute)
		a := Account{clock: clock}

		events, err := a.Decide(&rita.Command{
			Data: &DepositFunds{Amount: ten},
		})
		is.NoErr(err)
		is.Equal(len(events), 1)
		e, ok := events[0].Data.(*FundsDeposited)
		is.True(ok)
		is.Equal(*e, FundsDeposited{Amount: ten, Time: e.Time})

		// Evolve account and ensure the current funds are now 10.
		a.Evolve(events[0])
		is.True(a.CurrentFunds.Equal(ten))

		// Deposit more.
		events, _ = a.Decide(&rita.Command{
			Data: &DepositFunds{Amount: twenty},
		})
		a.Evolve(events[0])
		is.True(a.CurrentFunds.Equal(thirty))
	})

	t.Run("withdraw-funds", func(t *testing.T) {
		clock := testutil.NewClock(time.Minute)
		a := Account{clock: clock}

		events, err := a.Decide(&rita.Command{
			Data: &WithdrawFunds{Amount: ten},
		})
		is.Err(err, ErrInsufficientFunds)
		is.Equal(len(events), 0)

		events, _ = a.Decide(&rita.Command{
			Data: &DepositFunds{Amount: ten},
		})
		a.Evolve(events[0])

		events, err = a.Decide(&rita.Command{
			Data: &WithdrawFunds{Amount: ten},
		})
		is.NoErr(err)
		e, _ := events[0].Data.(*FundsWithdrawn)
		is.Equal(*e, FundsWithdrawn{Amount: ten, Time: e.Time})

		a.Evolve(events[0])
		is.True(a.CurrentFunds.Equal(decimal.Zero))
	})

	t.Run("withdraw-policy", func(t *testing.T) {
		clock := testutil.NewClock(time.Minute)
		a := Account{clock: clock}

		prettyPrint(t, a)

		events, _ := a.Decide(&rita.Command{
			Data: &DepositFunds{Amount: thirty},
		})
		a.Evolve(events[0])
		prettyPrint(t, events[0].Data)

		prettyPrint(t, a)

		events, _ = a.Decide(&rita.Command{
			Data: &SetWithdrawPolicy{MaxAmount: ten, Period: Daily},
		})
		e, _ := events[0].Data.(*WithdrawPolicySet)
		is.Equal(*e, WithdrawPolicySet{
			MaxWithdrawAmount:   ten,
			Period:              Daily,
			PolicyStartTime:     e.PolicyStartTime,
			PeriodStartTime:     e.PeriodStartTime,
			NextPeriodStartTime: e.NextPeriodStartTime,
		})
		a.Evolve(events[0])
		prettyPrint(t, events[0].Data)
		prettyPrint(t, a)

		events, err := a.Decide(&rita.Command{
			Data: &WithdrawFunds{Amount: ten},
		})
		is.NoErr(err)

		a.Evolve(events[0])
		prettyPrint(t, events[0].Data)
		prettyPrint(t, a)
		is.True(a.CurrentFunds.Equal(twenty))

		events, err = a.Decide(&rita.Command{
			Data: &WithdrawFunds{Amount: ten},
		})
		is.Err(err, ErrExceedWithinPeriod)

		// Jump to next day..
		clock.Add(24 * time.Hour)

		events, err = a.Decide(&rita.Command{
			Data: &WithdrawFunds{Amount: ten},
		})
		is.NoErr(err)

		a.Evolve(events[0])
		prettyPrint(t, events[0].Data)
		prettyPrint(t, a)
		is.True(a.CurrentFunds.Equal(ten))

		// Hit error again
		events, err = a.Decide(&rita.Command{
			Data: &WithdrawFunds{Amount: ten},
		})
		is.Err(err, ErrExceedWithinPeriod)

		// Remove policy..
		events, err = a.Decide(&rita.Command{
			Data: &RemoveWithdrawPolicy{},
		})
		is.NoErr(err)
		a.Evolve(events[0])
		prettyPrint(t, events[0].Data)
		prettyPrint(t, a)

		// Now can withdraw..
		events, err = a.Decide(&rita.Command{
			Data: &WithdrawFunds{Amount: ten},
		})
		is.NoErr(err)
	})
}

func TestCurrentFunds(t *testing.T) {
	is := testutil.NewIs(t)

	var f CurrentFunds

	ten, _ := decimal.NewFromString("10")
	twenty, _ := decimal.NewFromString("20")
	thirty, _ := decimal.NewFromString("30")

	f.Evolve(&rita.Event{
		Data: &FundsDeposited{
			Amount: ten,
		},
	})

	f.Evolve(&rita.Event{
		Data: &FundsDeposited{
			Amount: thirty,
		},
	})

	f.Evolve(&rita.Event{
		Data: &FundsWithdrawn{
			Amount: twenty,
		},
	})

	is.Equal(f, CurrentFunds{
		Amount: twenty,
	})
}

func TestPeriodSummary(t *testing.T) {
	is := testutil.NewIs(t)

	var p PeriodSummary

	ten, _ := decimal.NewFromString("10")
	twenty, _ := decimal.NewFromString("20")
	thirty, _ := decimal.NewFromString("30")

	pt := time.Date(2019, time.May, 3, 12, 20, 30, 0, time.UTC)
	st, nst := periodWindow(pt, Minute)

	p.Evolve(&rita.Event{
		Data: &WithdrawPolicySet{
			Period:              Minute,
			MaxWithdrawAmount:   thirty,
			PolicyStartTime:     pt,
			PeriodStartTime:     st,
			NextPeriodStartTime: nst,
		},
	})

	p.Evolve(&rita.Event{
		Data: &FundsWithdrawn{
			Amount:        ten,
			Time:          pt.Add(10 * time.Second),
			PeriodChanged: false,
		},
	})

	is.Equal(p, PeriodSummary{
		PolicyPeriod:            Minute,
		PolicyStartTime:         pt,
		PolicyMaxWithdrawAmount: thirty,
		WithdrawalsInPeriod:     1,
		FundsWithdrawnInPeriod:  ten,
		PeriodStartTime:         st,
		NextPeriodStartTime:     nst,
	})

	p.Evolve(&rita.Event{
		Data: &FundsWithdrawn{
			Amount:        twenty,
			Time:          pt.Add(30 * time.Second),
			PeriodChanged: false,
		},
	})

	is.Equal(p, PeriodSummary{
		PolicyPeriod:            Minute,
		PolicyStartTime:         pt,
		PolicyMaxWithdrawAmount: thirty,
		WithdrawalsInPeriod:     2,
		FundsWithdrawnInPeriod:  thirty,
		PeriodStartTime:         st,
		NextPeriodStartTime:     nst,
	})
}
