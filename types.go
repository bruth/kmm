package kmm

import "github.com/bruth/rita/types"

var (
	Types = map[string]*types.Type{
		// Commands and events.
		"deposit-funds":           {Init: func() any { return &DepositFunds{} }},
		"funds-deposited":         {Init: func() any { return &FundsDeposited{} }},
		"withdraw-funds":          {Init: func() any { return &WithdrawFunds{} }},
		"funds-withdrawn":         {Init: func() any { return &FundsWithdrawn{} }},
		"set-withdraw-policy":     {Init: func() any { return &SetWithdrawPolicy{} }},
		"withdraw-policy-set":     {Init: func() any { return &WithdrawPolicySet{} }},
		"remove-withdraw-policy":  {Init: func() any { return &RemoveWithdrawPolicy{} }},
		"withdraw-policy-removed": {Init: func() any { return &WithdrawPolicyRemoved{} }},
		// Aggregate state.
		"account": {Init: func() any { return NewAccount() }},
		// Query/stream results.
		"current-funds":  {Init: func() any { return &CurrentFunds{} }},
		"period-summary": {Init: func() any { return &PeriodSummary{} }},
	}
)
