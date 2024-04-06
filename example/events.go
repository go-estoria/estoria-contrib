package main

import "github.com/go-estoria/estoria"

// UserCreatedEvent is an example event representing a user being added to an account.
type UserCreatedEvent struct {
	Username string
}

func (UserCreatedEvent) EventType() string { return "usercreated" }

func (UserCreatedEvent) New() estoria.EventData {
	return &UserCreatedEvent{}
}

// UserDeletedEvent is an example event representing a user being deleted from an account.
type UserDeletedEvent struct {
	Username string
}

func (UserDeletedEvent) EventType() string { return "userdeleted" }

func (UserDeletedEvent) New() estoria.EventData {
	return &UserDeletedEvent{}
}

// BalanceChangedEvent is an example event representing a change in an account's balance.
type BalanceChangedEvent struct {
	Amount int
}

func (BalanceChangedEvent) EventType() string { return "balancechanged" }

func (BalanceChangedEvent) New() estoria.EventData {
	return &BalanceChangedEvent{}
}
