package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/typeid"
)

// An Account is an example entity that satifies the requirements of the estoria.Entity interface.
type Account struct {
	ID      typeid.TypeID
	Users   []string
	Balance int
}

// NewAccount creates a new account.
func NewAccount() *Account {
	return &Account{
		ID:      typeid.Must(typeid.NewUUID("account")),
		Users:   make([]string, 0),
		Balance: 0,
	}
}

// EntityID returns the ID of the entity.
func (a *Account) EntityID() typeid.TypeID {
	return a.ID
}

func (a *Account) EntityType() string {
	return "account"
}

// ApplyEvent applies an event to the entity.
func (a *Account) ApplyEvent(_ context.Context, event estoria.EntityEventData) error {
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
func (a *Account) Diff(newer estoria.Entity) ([]any, error) {
	slog.Info("diffing account", "account", a, "newer", newer)
	newerAccount, ok := newer.(*Account)
	if !ok {
		return nil, fmt.Errorf("invalid entity type")
	}

	events := make([]any, 0)

	// map of user: newly-added
	userMap := make(map[string]bool)
	for _, user := range a.Users {
		userMap[user] = false
	}

	for _, user := range newerAccount.Users {
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
	if a.Balance != newerAccount.Balance {
		events = append(events, &BalanceChangedEvent{
			Amount: newerAccount.Balance - a.Balance,
		})
	}

	slog.Info("diffed accounts", "events", len(events))
	return events, nil
}

func (a *Account) String() string {
	return fmt.Sprintf("Account %s {Users: %v} Balance: %d", a.ID, a.Users, a.Balance)
}
