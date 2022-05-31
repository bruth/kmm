package kmm

import "github.com/bruth/rita/types"

var (
	Types = map[string]*types.Type{
		// Commands and events.
		"deposit-funds":   {Init: func() any { return &DepositFunds{} }},
		"funds-deposited": {Init: func() any { return &FundsDeposited{} }},
		"withdraw-funds":  {Init: func() any { return &WithdrawFunds{} }},
		"funds-withdrawn": {Init: func() any { return &FundsWithdrawn{} }},
		"set-budget":      {Init: func() any { return &SetBudget{} }},
		"budget-set":      {Init: func() any { return &BudgetSet{} }},
		"remove-budget":   {Init: func() any { return &RemoveBudget{} }},
		"budget-removed":  {Init: func() any { return &BudgetRemoved{} }},
		// Aggregate state.
		"account": {Init: func() any { return NewAccount() }},
		// Query results.
		"current-funds": {Init: func() any { return &CurrentFunds{} }},
		"budget-period": {Init: func() any { return &BudgetPeriod{} }},
	}
)
