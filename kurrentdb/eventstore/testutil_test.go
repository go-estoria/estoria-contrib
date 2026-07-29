package eventstore_test

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/netip"
	"slices"
	"testing"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
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
	port, err := network.ParsePort(portStr + "/tcp")
	if err != nil {
		return nil, fmt.Errorf("parsing port: %w", err)
	}

	hostIP, err := netip.ParseAddr("0.0.0.0")
	if err != nil {
		return nil, fmt.Errorf("parsing host IP: %w", err)
	}

	req := testcontainers.ContainerRequest{
		// Pinned rather than :latest so a server release cannot change the readiness
		// contract below with no change on our side. 26.1 was byte-identical to the
		// :latest this replaced. Matches how the mongo and postgres suites pin.
		Image:        "docker.kurrent.io/kurrent-latest/kurrentdb:26.1",
		ExposedPorts: []string{port.String()},
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
			hc.PortBindings = network.PortMap{
				port: []network.PortBinding{{HostIP: hostIP, HostPort: portStr}},
			}
		},
		// testcontainers' default startup timeout is 60s, which this exceeded on CI once
		// testcontainers-go moved to 0.43.0: up to 10 single-node clusters (see kurrentSem)
		// elect leaders concurrently on a 2-core runner, and the log line simply arrives
		// late. Failures looked like "matched 0 times, expected 1" with no test assertion
		// involved. Three minutes is well inside the suite's 20m budget.
		WaitingFor: wait.ForLog("InaugurationManager in state (Leader, Idle)").
			WithStartupTimeout(3 * time.Minute),
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
	mapped, err := c.MappedPort(ctx, port.String())
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
