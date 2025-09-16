package eventstore_test

import (
	"context"
	"fmt"
	"log"
	"net"
	"slices"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func must[T any](val T, err error) T {
	if err != nil {
		panic("unexpected error: " + err.Error())
	}
	return val
}

func reversed[T any](s []T) []T {
	r := make([]T, len(s))
	copy(r, s)
	slices.Reverse(r)
	return r
}

var kurrentSem = make(chan struct{}, 10) // limit concurrent KurrentDB containers

func createKurrentContainer(t *testing.T, ctx context.Context) (*kurrentdb.Client, error) {
	t.Helper()

	t.Log("waiting for available KurrentDB slot...")
	kurrentSem <- struct{}{}
	t.Cleanup(func() { <-kurrentSem })

	// random port to avoid collisions when running tests in parallel
	portNum, err := getFreePort()
	if err != nil {
		return nil, fmt.Errorf("getting free port: %w", err)
	}

	portStr := fmt.Sprint(portNum)
	port := nat.Port(portStr + "/tcp")

	req := testcontainers.ContainerRequest{
		Image:        "docker.kurrent.io/kurrent-latest/kurrentdb:latest",
		ExposedPorts: []string{string(port)},
		Env: map[string]string{
			"KURRENTDB_CLUSTER_SIZE":               "1",
			"KURRENTDB_RUN_PROJECTIONS":            "All",
			"KURRENTDB_START_STANDARD_PROJECTIONS": "true",
			"KURRENTDB_NODE_PORT":                  portStr,
			"KURRENTDB_INSECURE":                   "true", // dev/test only
			"KURRENTDB_ENABLE_ATOM_PUB_OVER_HTTP":  "true", // optional; only needed for the Admin UI/feeds
		},
		// bind host port -> container port so the node's advertised port is reachable
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.PortBindings = nat.PortMap{
				port: []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: portStr}},
			}
		},
		WaitingFor: wait.ForLog("InaugurationManager in state (Leader, Idle)"),
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(c); err != nil {
			t.Fatalf("failed to terminate Kurrent container: %v", err)
		}
	})

	host, err := c.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("get host: %w", err)
	}
	mapped, err := c.MappedPort(ctx, port)
	if err != nil {
		return nil, fmt.Errorf("get mapped port: %w", err)
	}

	dsn := fmt.Sprintf("kurrentdb://%s:%s?tls=false", host, mapped.Port())

	settings, err := kurrentdb.ParseConnectionString(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse DSN: %w", err)
	}
	settings.Username = "admin"
	settings.Password = "changeit"

	client, err := kurrentdb.NewClient(settings)
	if err != nil {
		return nil, fmt.Errorf("new client: %w", err)
	}

	log.Printf("Kurrent is up: %s", dsn)
	return client, nil
}

func getFreePort() (int, error) {
	a, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", a)
	if err != nil {
		return 0, err
	}

	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
