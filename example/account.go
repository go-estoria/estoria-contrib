package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
)

const (
	accountType = "account"
)

// An Account is an example entity that satifies the requirements of the estoria.Entity interface.
type Account struct {
	ID      uuid.UUID
	Users   []string
	Balance int
}

// NewAccount creates a new account.
func NewAccount(id uuid.UUID) *Account {
	slog.Debug("creating new account", "type", accountType, "id", id)
	return &Account{
		ID:      id,
		Users:   make([]string, 0),
		Balance: 0,
	}
}

// EntityID returns the ID of the entity.
func (a *Account) EntityID() typeid.UUID {
	return typeid.FromUUID(accountType, a.ID)
}

// SetEntityID sets the ID of the entity.
func (a *Account) SetEntityID(id typeid.UUID) {
	a.ID = id.UUID()
}

// EventTypes returns the event types that can be applied to the entity.
func (a *Account) EventTypes() []estoria.EntityEvent {
	return []estoria.EntityEvent{
		&BalanceChangedEvent{},
		&UserCreatedEvent{},
		&UserDeletedEvent{},
	}
}

// ApplyEvent applies an event to the entity.
func (a *Account) ApplyEvent(_ context.Context, event estoria.EntityEvent) error {
	switch e := event.(type) {

	case *BalanceChangedEvent:
		// slog.Info("applying balance changed event data", "amount", e.Amount)
		a.Balance += e.Amount
		return nil

	case *UserCreatedEvent:
		// slog.Info("applying user created event data", "username", e.Username)
		a.Users = append(a.Users, e.Username)
		return nil

	case *UserDeletedEvent:
		// slog.Info("applying user deleted event data", "username", e.Username)
		for i, user := range a.Users {
			if user == e.Username {
				a.Users = append(a.Users[:i], a.Users[i+1:]...)
				return nil
			}
		}
		return fmt.Errorf("user %s not found", e.Username)

	default:
		return fmt.Errorf("invalid event type: %T", event)
	}
}

// Diff diffs the entity against another entity and returns a series
// of events that represent the state changes between the two.
func (a *Account) Diff(newer *Account) ([]estoria.EntityEvent, error) {
	slog.Info("diffing account", "account", a, "newer", newer)

	events := make([]estoria.EntityEvent, 0)

	// map of user: newly-added
	userMap := make(map[string]bool)
	for _, user := range a.Users {
		userMap[user] = false
	}

	for _, user := range newer.Users {
		if _, exists := userMap[user]; exists {
			userMap[user] = true
		} else {
			events = append(events, &UserCreatedEvent{
				Username: user,
			})
		}
	}

	for user, existsInNewer := range userMap {
		if !existsInNewer {
			events = append(events, &UserDeletedEvent{
				Username: user,
			})
		}
	}

	// balance difference
	if a.Balance != newer.Balance {
		events = append(events, &BalanceChangedEvent{
			Amount: newer.Balance - a.Balance,
		})
	}

	slog.Info("diffed accounts", "events", len(events))
	return events, nil
}

func (a *Account) String() string {
	return fmt.Sprintf("Account %s {Users: %v} Balance: %d", a.ID, a.Users, a.Balance)
}
