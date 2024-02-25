package eventstore

import (
	"context"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
)

func NewDefaultEventStoreDBClient(ctx context.Context, uri, username, password string) (*esdb.Client, error) {
	settings, err := esdb.ParseConnectionString(uri)
	if err != nil {
		return nil, fmt.Errorf("parsing connection string: %w", err)
	}

	settings.DisableTLS = true
	settings.Username = username
	settings.Password = password
	settings.SkipCertificateVerification = true

	client, err := esdb.NewClient(settings)
	if err != nil {
		return nil, fmt.Errorf("creating EventStoreDB client: %w", err)
	}

	return client, nil
}
