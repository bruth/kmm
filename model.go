package kmm

import (
	"errors"
	"time"

	"github.com/bruth/rita"
	"github.com/bruth/rita/clock"
	"github.com/shopspring/decimal"
)

var (
	ErrUnknownCommand     = errors.New("unknown command")
	ErrNonZeroAmount      = errors.New("kmm: amount must be greater than zero")
	ErrInvalidPeriod      = errors.New("kmm: period must be daily, weekly, or monthly")
	ErrInsufficientFunds  = errors.New("kmm: insufficient funds")
	ErrExceedWithinPeriod = errors.New("kmm: withdrawal would exceed max amount allowed in current period")
)

type DeciderEvolver interface {
	rita.Decider
	rita.Evolver
}

var (
	_ DeciderEvolver = &Account{}
	_ rita.Evolver   = &PeriodSummary{}
	_ rita.Evolver   = &CurrentFunds{}
)

type DepositFunds struct {
	Amount      decimal.Decimal
	Description string
}

func (c *DepositFunds) Validate() error {
	if c.Amount.LessThanOrEqual(decimal.Zero) {
		return ErrNonZeroAmount
	}
	return nil
}

type FundsDeposited struct {
	Amount      decimal.Decimal
	Description string
	Time        time.Time
}

type WithdrawFunds struct {
	Amount      decimal.Decimal
	Description string
}

func (c *WithdrawFunds) Validate() error {
	if c.Amount.LessThanOrEqual(decimal.Zero) {
		return ErrNonZeroAmount
	}
	return nil
}

type FundsWithdrawn struct {
	Amount        decimal.Decimal
	Description   string
	Time          time.Time
	PeriodChanged bool
}

type Period string

const (
	Minute  Period = "minute" // For demo...
	Daily   Period = "daily"
	Weekly  Period = "weekly"
	Monthly Period = "monthly"
)

type SetWithdrawPolicy struct {
	MaxAmount decimal.Decimal
	Period    Period
}

func (c *SetWithdrawPolicy) Validate() error {
	if c.MaxAmount.LessThan(decimal.Zero) {
		return ErrNonZeroAmount
	}

	// Validate period.
	switch c.Period {
	case Minute, Daily, Weekly, Monthly:
	default:
		return ErrInvalidPeriod
	}
	return nil
}

type WithdrawPolicySet struct {
	MaxWithdrawAmount   decimal.Decimal
	Period              Period
	PolicyStartTime     time.Time
	PeriodStartTime     time.Time
	NextPeriodStartTime time.Time
}

type RemoveWithdrawPolicy struct{}

type WithdrawPolicyRemoved struct {
	PolicyRemoveTime time.Time
}

// periodWindow takes the time value and determines the current start time
// of the period and start time of the next period.
func periodWindow(t time.Time, p Period) (time.Time, time.Time) {
	switch p {
	// Every minute..
	case Minute:
		st := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
		nst := st.Add(time.Minute)
		return st, nst

	// Day starts at midnight
	case Daily:
		// Truncate time.
		st := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		// Add one day.
		nst := st.AddDate(0, 0, 1)
		return st, nst

	// Week starts on Monday at midnight
	case Weekly:
		sd := t.Day() - int(t.Weekday()-time.Monday)
		st := time.Date(t.Year(), t.Month(), sd, 0, 0, 0, 0, t.Location())
		nst := st.AddDate(0, 0, 7)
		return st, nst

	// Month starts the 1st at midnight
	case Monthly:
		st := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
		nst := st.AddDate(0, 1, 0)
		return st, nst
	}

	return time.Time{}, time.Time{}
}

func NewAccount() *Account {
	return &Account{
		clock: clock.Time,
	}
}

// Account aggregate which primarily decides on whether a withdrawal is
// allowed given the current funds and if a policy is set.
//
// The DepositFunds command doesn't even need an aggregate since there is no
// validation other than command validation that the amount is a positive value
// (which is done prior to the command being received here).
//
// The Set/RemoveWithdrawPolicy are in the same category and does not really need
// any aggregated state for them to be accepted.
type Account struct {
	CurrentFunds decimal.Decimal

	// Policy related.
	MaxWithdrawAmount      decimal.Decimal
	PolicyPeriod           Period
	PeriodStartTime        time.Time
	NextPeriodStartTime    time.Time
	FundsWithdrawnInPeriod decimal.Decimal

	clock clock.Clock
}

func (a *Account) Decide(command *rita.Command) ([]*rita.Event, error) {
	switch c := command.Data.(type) {
	case *DepositFunds:
		// As much money can be deposited as desired, so no
		// decision needs to be made.
		return []*rita.Event{
			{
				Data: &FundsDeposited{
					Amount:      c.Amount,
					Description: c.Description,
					Time:        a.clock.Now(),
				},
			},
		}, nil

	case *WithdrawFunds:
		// Ensure funds do not go below zero.
		if a.CurrentFunds.Sub(c.Amount).LessThan(decimal.Zero) {
			return nil, ErrInsufficientFunds
		}

		now := a.clock.Now()

		var periodChanged bool

		// Check if the withdraw is allowed given the policy.
		if a.PolicyPeriod != "" {
			// Next period start time has not been reached.
			periodChanged = !now.Before(a.NextPeriodStartTime)

			if !periodChanged {
				if a.FundsWithdrawnInPeriod.Add(c.Amount).GreaterThan(a.MaxWithdrawAmount) {
					return nil, ErrExceedWithinPeriod
				}
			}
		}

		// Could emit PeriodChanged event as well, however this can be lazily
		// detected on the evolve side. Alternatively, an indepedent actor could
		// monitor the policy changes and a ticker to emit period change events..
		return []*rita.Event{
			{
				Data: &FundsWithdrawn{
					Amount:        c.Amount,
					Description:   c.Description,
					Time:          now,
					PeriodChanged: periodChanged,
				},
			},
		}, nil

	case *SetWithdrawPolicy:
		now := a.clock.Now()
		st, nst := periodWindow(now, c.Period)

		return []*rita.Event{
			{
				Data: &WithdrawPolicySet{
					MaxWithdrawAmount:   c.MaxAmount,
					Period:              c.Period,
					PolicyStartTime:     now,
					PeriodStartTime:     st,
					NextPeriodStartTime: nst,
				},
			},
		}, nil

	case *RemoveWithdrawPolicy:
		return []*rita.Event{
			{
				Data: &WithdrawPolicyRemoved{
					PolicyRemoveTime: a.clock.Now(),
				},
			},
		}, nil
	}

	return nil, ErrUnknownCommand
}

func (a *Account) Evolve(event *rita.Event) error {
	switch e := event.Data.(type) {
	case *FundsDeposited:
		a.CurrentFunds = a.CurrentFunds.Add(e.Amount)

	case *FundsWithdrawn:
		a.CurrentFunds = a.CurrentFunds.Sub(e.Amount)

		if a.PolicyPeriod != "" {
			if e.PeriodChanged {
				a.FundsWithdrawnInPeriod = e.Amount
				a.PeriodStartTime, a.NextPeriodStartTime = periodWindow(e.Time, a.PolicyPeriod)
			} else {
				a.FundsWithdrawnInPeriod = a.FundsWithdrawnInPeriod.Add(e.Amount)
			}
		}

	case *WithdrawPolicySet:
		a.MaxWithdrawAmount = e.MaxWithdrawAmount
		a.PolicyPeriod = e.Period
		a.PeriodStartTime = e.PeriodStartTime
		a.NextPeriodStartTime = e.NextPeriodStartTime
		a.FundsWithdrawnInPeriod = decimal.Zero

	case *WithdrawPolicyRemoved:
		a.MaxWithdrawAmount = decimal.Zero
		a.PolicyPeriod = ""
		a.PeriodStartTime = time.Time{}
		a.NextPeriodStartTime = time.Time{}
		a.FundsWithdrawnInPeriod = decimal.Zero
	}

	return nil
}

type CurrentFunds struct {
	Amount decimal.Decimal
}

func (c *CurrentFunds) Evolve(event *rita.Event) error {
	switch e := event.Data.(type) {
	case *FundsDeposited:
		c.Amount = c.Amount.Add(e.Amount)
	case *FundsWithdrawn:
		c.Amount = c.Amount.Sub(e.Amount)
	}
	return nil
}

type PeriodSummary struct {
	PolicyPeriod            Period
	PolicyStartTime         time.Time
	PolicyMaxWithdrawAmount decimal.Decimal
	WithdrawalsInPeriod     int
	FundsWithdrawnInPeriod  decimal.Decimal
	PeriodStartTime         time.Time
	NextPeriodStartTime     time.Time
}

func (p *PeriodSummary) Evolve(event *rita.Event) error {
	switch e := event.Data.(type) {
	case *WithdrawPolicySet:
		p.PolicyPeriod = e.Period
		p.PolicyMaxWithdrawAmount = e.MaxWithdrawAmount
		p.PolicyStartTime = e.PolicyStartTime
		p.WithdrawalsInPeriod = 0
		p.FundsWithdrawnInPeriod = decimal.Zero
		p.PeriodStartTime, p.NextPeriodStartTime = periodWindow(e.PolicyStartTime, p.PolicyPeriod)

	case *WithdrawPolicyRemoved:
		p.PolicyPeriod = ""
		p.PolicyMaxWithdrawAmount = decimal.Zero
		p.PolicyStartTime = time.Time{}
		p.PeriodStartTime = time.Time{}
		p.NextPeriodStartTime = time.Time{}

	case *FundsWithdrawn:
		if e.PeriodChanged {
			p.WithdrawalsInPeriod = 0
			p.FundsWithdrawnInPeriod = decimal.Zero
			p.PeriodStartTime, p.NextPeriodStartTime = periodWindow(e.Time, p.PolicyPeriod)
		}

		p.WithdrawalsInPeriod++
		p.FundsWithdrawnInPeriod = p.FundsWithdrawnInPeriod.Add(e.Amount)
	}

	return nil
}
