package eventstore_test

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func reversed[T any](s []T) []T {
	r := make([]T, len(s))
	copy(r, s)
	slices.Reverse(r)
	return r
}

// allHead returns the commit position of the last record in the node's $all stream, or
// zero when the log is empty — the same server-wide head a ReadAll captures as its
// frontier.
func allHead(t *testing.T, client *kurrentdb.Client) uint64 {
	t.Helper()

	read, err := client.ReadAll(t.Context(), kurrentdb.ReadAllOptions{
		Direction: kurrentdb.Backwards,
		From:      kurrentdb.End{},
	}, 1)
	if err != nil {
		t.Fatalf("reading the $all head: %v", err)
	}
	defer read.Close()

	resolved, err := read.Recv()
	if errors.Is(err, io.EOF) {
		return 0
	} else if err != nil {
		t.Fatalf("receiving the $all head record: %v", err)
	}

	if resolved.Commit == nil {
		t.Fatal("$all head record has no commit position")
	}

	return *resolved.Commit
}

// Limits how many KurrentDB containers start at once.
//
// This was 10, which oversubscribes a 2-core runner badly: ten single-node clusters
// electing leaders simultaneously each take roughly ten times as long as one alone, and
// runs were exceeding even the three-minute startup timeout below. Lowering it does not
// cost wall-clock time, because the suite is bounded by total container-seconds rather
// than by how many run at once — four waves of four starting quickly beats two waves of
// ten all crawling.
var kurrentSem = make(chan struct{}, 4)

func createKurrentContainer(t *testing.T) (*kurrentdb.Client, error) {
	t.Helper()
	return createKurrentContainerWithEnv(t, nil)
}

// createKurrentContainerWithEnv starts a KurrentDB container with the standard
// configuration plus the given environment overrides, which take precedence.
func createKurrentContainerWithEnv(t *testing.T, envOverrides map[string]string) (*kurrentdb.Client, error) {
	t.Helper()

	ctx := t.Context()

	t.Log("waiting for available KurrentDB slot...")
	kurrentSem <- struct{}{}
	t.Cleanup(func() { <-kurrentSem })

	hostIP, err := netip.ParseAddr("0.0.0.0")
	if err != nil {
		return nil, fmt.Errorf("parsing host IP: %w", err)
	}

	// The host port must equal the container port (KURRENTDB_NODE_PORT is what the node
	// advertises to clients), so it is picked here rather than left to Docker — and the
	// free-port probe cannot hold the port through Docker's bind, so a concurrent bind
	// can win it in between ("Bind for 0.0.0.0:<port> failed: port is already
	// allocated" on CI). Retrying with a fresh port is the only correct response.
	const maxStartAttempts = 3
	var c testcontainers.Container
	var port network.Port
	for attempt := 1; ; attempt++ {
		portNum, err := getFreePort()
		if err != nil {
			return nil, fmt.Errorf("getting free port: %w", err)
		}

		portStr := strconv.Itoa(portNum)
		port, err = network.ParsePort(portStr + "/tcp")
		if err != nil {
			return nil, fmt.Errorf("parsing port: %w", err)
		}

		env := map[string]string{
			"KURRENTDB_CLUSTER_SIZE":               "1",
			"KURRENTDB_RUN_PROJECTIONS":            "All",
			"KURRENTDB_START_STANDARD_PROJECTIONS": "true",
			"KURRENTDB_NODE_PORT":                  portStr,
			"KURRENTDB_INSECURE":                   "true", // dev/test only
			"KURRENTDB_ENABLE_ATOM_PUB_OVER_HTTP":  "true", // optional; only needed for the Admin UI/feeds
		}
		for k, v := range envOverrides {
			env[k] = v
		}

		req := testcontainers.ContainerRequest{
			// Pinned rather than :latest so a server release cannot change the readiness
			// contract below with no change on our side. 26.1 was byte-identical to the
			// :latest this replaced. Matches how the mongo and postgres suites pin.
			Image:        "docker.kurrent.io/kurrent-latest/kurrentdb:26.1",
			ExposedPorts: []string{port.String()},
			Env:          env,
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

		c, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err == nil {
			break
		}

		// A created-but-unstarted container would otherwise leak.
		if c != nil {
			_ = testcontainers.TerminateContainer(c)
		}

		if attempt == maxStartAttempts || !strings.Contains(err.Error(), "port is already allocated") {
			t.Fatalf("failed to start container: %v", err)
		}

		t.Logf("host port %s lost to a concurrent bind; retrying with a fresh port", portStr)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(c); err != nil {
			t.Fatalf("failed to terminate Kurrent container: %v", err)
		}
	})

	mapped, err := c.MappedPort(ctx, port.String())
	if err != nil {
		return nil, fmt.Errorf("get mapped port: %w", err)
	}

	// The container binds the IPv4 wildcard, so dial 127.0.0.1 explicitly: the
	// testcontainers host is "localhost", which resolves to ::1 first on some runners,
	// and CI has seen "connection refused [::1]" from exactly that.
	dsn := "kurrentdb://" + net.JoinHostPort("127.0.0.1", mapped.Port()) + "?tls=false"

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

// getFreePort reserves a host port on the IPv4 wildcard — the address Docker publishes
// on. Probing localhost picked ports that were only known-free on loopback (and on hosts
// where localhost resolves to ::1, only on IPv6), so a port could probe free here yet be
// taken where Docker binds it.
func getFreePort() (int, error) {
	l, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4zero})
	if err != nil {
		return 0, err
	}

	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
