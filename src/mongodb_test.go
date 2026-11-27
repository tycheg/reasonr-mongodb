package mongodb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tycheg/reasonr-engine/port"
	"github.com/tycheg/reasonr-engine/port/porttest"
)

// Tests on a live MongoDB (a replica set): the address is
// REASONR_TEST_MONGODB; without it the tests are skipped. Each check runs
// in its own database.

var dbSeq atomic.Int64

// testStore is a store in a new database.
func testStore(t *testing.T) *Store {
	uri := os.Getenv("REASONR_TEST_MONGODB")
	if uri == "" {
		t.Skip("REASONR_TEST_MONGODB is not set")
	}
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("t_%d_%d", time.Now().UnixNano()%1e9, dbSeq.Add(1))
	s := New(client, name, Options{Own: true})
	t.Cleanup(func() {
		_ = client.Database(name).Drop(context.Background())
		_ = s.Close()
	})
	return s
}

// laid is a store with the layout of the suites (porttest.Layout) for both
// tenants.
func laid(t *testing.T) *Store {
	s := testStore(t)
	if err := s.EnsureSchema(context.Background(), porttest.Layout(), []port.Tenant{porttest.T1, porttest.T2}, keep); err != nil {
		t.Fatal(err)
	}
	return s
}

func keep(_ context.Context, _ port.Tenant, _ *port.Table, r port.Row) (port.Row, bool, error) {
	return r, false, nil
}

// order is a row of porttest.Orders with the id n and the code key code.
func order(n byte, version uint64, code string) port.Row {
	keys := make([]port.Scalar, len(porttest.Orders.Keys))
	keys[6] = []byte(code)
	return port.Row{ID: [16]byte{n}, Version: version, State: []byte(fmt.Sprintf(`{"n":%d,"v":%d}`, n, version)), Keys: keys}
}

// write is a unit of work that writes rows of porttest.Orders.
func write(rows ...port.Row) *port.UnitRecord {
	u := &port.UnitRecord{Outbox: []byte(`{}`), Now: porttest.Start}
	for _, r := range rows {
		u.Writes = append(u.Writes, port.RowWrite{Table: porttest.Orders, Row: r})
	}
	return u
}

// TestPorts: the byte-level conformance suites of the contract
// (port/porttest).
func TestPorts(t *testing.T) {
	porttest.RunStore(t, func(t *testing.T) any { return testStore(t) })
}

// TestConcurrentCommits: units of work on one row from many goroutines —
// a stale version is a conflict, with retries every unit is applied once,
// and the outbox positions grow.
func TestConcurrentCommits(t *testing.T) {
	ctx := context.Background()
	s := laid(t)
	if _, err := s.Commit(ctx, porttest.T1, write(order(1, 0, "a"))); err != nil {
		t.Fatal(err)
	}
	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				r, _, err := s.Load(ctx, porttest.T1, porttest.Orders, [16]byte{1})
				if err == nil {
					_, err = s.Commit(ctx, porttest.T1, write(order(1, r.Version, "a")))
				}
				if !errors.Is(err, port.ErrConflict) {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if r, _, err := s.Load(ctx, porttest.T1, porttest.Orders, [16]byte{1}); err != nil || r.Version != n+1 {
		t.Fatalf("version %d: %v", r.Version, err)
	}
	units, _ := s.Read(ctx, 0, 0)
	if len(units) != n+1 {
		t.Fatalf("outbox has %d units of work, want %d", len(units), n+1)
	}
	for i := 1; i < len(units); i++ {
		if units[i].Position <= units[i-1].Position {
			t.Fatalf("outbox positions do not increase: %d after %d", units[i].Position, units[i-1].Position)
		}
	}
}

// TestUniqueDuplicates: a unique index added over records with repeated
// keys is refused with the repeated values listed.
func TestUniqueDuplicates(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	l := porttest.Layout()
	for i := range l.Tables {
		l.Tables[i].Indexes = nil
	}
	if err := s.EnsureSchema(ctx, l, []port.Tenant{porttest.T1}, keep); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(ctx, porttest.T1, write(order(1, 0, "SAME"), order(2, 0, "SAME"))); err != nil {
		t.Fatal(err)
	}
	l = porttest.Layout()
	l.Schema = []byte(`{"model":"shop","version":2}`)
	err := s.EnsureSchema(ctx, l, []port.Tenant{porttest.T1}, keep)
	var se *SchemaError
	if !errors.As(err, &se) || !strings.Contains(err.Error(), "(2 records)") {
		t.Fatalf("want an error with the repeats: %v", err)
	}
}
