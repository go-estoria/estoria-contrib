package main

import "github.com/go-estoria/estoria"

// UserCreatedEvent is an example event representing a user being added to an account.
type UserCreatedEvent struct {
	Username string
}

var _ estoria.EntityEvent = (*UserCreatedEvent)(nil)

func (UserCreatedEvent) EventType() string { return "usercreated" }

func (UserCreatedEvent) New() estoria.EntityEvent {
	return &UserCreatedEvent{}
}

// UserDeletedEvent is an example event representing a user being deleted from an account.
type UserDeletedEvent struct {
	Username string
}

var _ estoria.EntityEvent = (*UserDeletedEvent)(nil)

func (UserDeletedEvent) EventType() string { return "userdeleted" }

func (UserDeletedEvent) New() estoria.EntityEvent {
	return &UserDeletedEvent{}
}

// BalanceChangedEvent is an example event representing a change in an account's balance.
type BalanceChangedEvent struct {
	Amount int
}

var _ estoria.EntityEvent = (*BalanceChangedEvent)(nil)

func (BalanceChangedEvent) EventType() string { return "balancechanged" }

func (BalanceChangedEvent) New() estoria.EntityEvent {
	return &BalanceChangedEvent{}
}
