package sqlite_test

import (
	"context"

	"database/sql"

	"errors"
	"fmt"
	"path/filepath"

	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"qattidev/sgsp"
	adapter "qattidev/sgsp-sqlite"
	"qattidev/sgsp/placement"
	"qattidev/sgsp/placement/memory"
)

const integrationMaxOpenConns = 16

type integrationSigner struct{}

func (integrationSigner) Sign(context.Context, sgsp.Admission) (string, error) {
	return "integration-ticket", nil
}

func integrationOwner(id byte) sgsp.Owner {
	return sgsp.Owner{
		ID:          string(rune('a' + id)),
		Incarnation: sgsp.Incarnation{id},
		Endpoint: sgsp.Endpoint{
			Address:    fmt.Sprintf("127.0.0.1:%d", 4100+int(id)),
			ServerName: fmt.Sprintf("owner-%d", id),
		},
	}
}

func integrationBootstrap(t *testing.T, app sgsp.AppIdentity, store placement.AssignmentStore, registry placement.Registry) *placement.Bootstrap {
	t.Helper()
	bootstrap, err := placement.NewBootstrap(placement.BootstrapConfig{
		App:      app,
		Store:    store,
		Registry: registry,
		Signer:   integrationSigner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bootstrap.Close)
	return bootstrap
}

func integrationDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(t.TempDir(), "assignments.db")+"?_busy_timeout=10000&_journal_mode=WAL&_synchronous=FULL")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(integrationMaxOpenConns)
	t.Cleanup(func() { db.Close() })
	for range 2 {
		if err := adapter.ApplyMigrations(context.Background(), db); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func TestConcurrentAssignment(t *testing.T) {
	db := integrationDB(t)
	store, err := adapter.New(db)
	if err != nil {
		t.Fatal(err)
	}
	group := placement.GroupID{App: sgsp.AppIdentity{ID: "integration", Version: "1"}, Key: "race"}
	owners := []sgsp.Owner{
		{ID: "a", Incarnation: sgsp.Incarnation{1}, Endpoint: sgsp.Endpoint{Address: "127.0.0.1:1", ServerName: "a"}},
		{ID: "b", Incarnation: sgsp.Incarnation{2}, Endpoint: sgsp.Endpoint{Address: "127.0.0.1:2", ServerName: "b"}},
		{ID: "c", Incarnation: sgsp.Incarnation{3}, Endpoint: sgsp.Endpoint{Address: "127.0.0.1:3", ServerName: "c"}},
	}
	results := make(chan placement.Assignment, 100)
	errs := make(chan error, 100)
	var groupWait sync.WaitGroup
	for index := 0; index < 100; index++ {
		groupWait.Add(1)
		go func(index int) {
			defer groupWait.Done()
			assignment, err := store.Assign(context.Background(), group, owners[index%len(owners)])
			if err != nil {
				errs <- err
				return
			}
			results <- assignment
		}(index)
	}
	groupWait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var winner sgsp.Owner
	for assignment := range results {
		if winner.ID == "" {
			winner = assignment.Owner
		}
		if assignment.Owner != winner {
			t.Fatalf("winner changed: %#v != %#v", assignment.Owner, winner)
		}
	}
	if winner.ID == "" {
		t.Fatal("no assignment winner")
	}
}

func TestConcurrentBootstrapResolve(t *testing.T) {
	db := integrationDB(t)
	firstStore, err := adapter.New(db)
	if err != nil {
		t.Fatal(err)
	}
	secondStore, err := adapter.New(db)
	if err != nil {
		t.Fatal(err)
	}
	app := sgsp.AppIdentity{ID: "integration", Version: "1"}
	registry := memory.NewRegistry()
	for id := byte(0); id < 3; id++ {
		if err := registry.Register(context.Background(), integrationOwner(id)); err != nil {
			t.Fatal(err)
		}
	}
	first := integrationBootstrap(t, app, firstStore, registry)
	second := integrationBootstrap(t, app, secondStore, registry)
	principal := sgsp.Principal{Issuer: "integration", Subject: "player", ExpiresAt: time.Now().Add(time.Minute)}

	const attempts = 100
	start := make(chan struct{})
	placements := make(chan placement.Placement, attempts)
	errs := make(chan error, attempts)
	var ready, workers sync.WaitGroup
	for index := 0; index < attempts; index++ {
		ready.Add(1)
		workers.Add(1)
		bootstrap := first
		if index%2 == 1 {
			bootstrap = second
		}
		go func(bootstrap *placement.Bootstrap) {
			defer workers.Done()
			ready.Done()
			<-start
			resolved, err := bootstrap.Resolve(context.Background(), principal, "shared-winner")
			if err != nil {
				errs <- err
				return
			}
			placements <- resolved
		}(bootstrap)
	}
	ready.Wait()
	close(start)
	workers.Wait()
	close(placements)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var winner sgsp.Owner
	for resolved := range placements {
		if resolved.AdmissionTicket == "" {
			t.Fatal("bootstrap returned an empty admission ticket")
		}
		if winner.ID == "" {
			winner = resolved.Owner
		}
		if resolved.Owner != winner {
			t.Fatalf("bootstrap winner changed: %#v != %#v", resolved.Owner, winner)
		}
	}
	if winner.ID == "" {
		t.Fatal("no bootstrap assignment winner")
	}
	stored, err := firstStore.Get(context.Background(), placement.GroupID{App: app, Key: "shared-winner"})
	if err != nil {
		t.Fatal(err)
	}
	if stored.Closed || stored.Owner != winner {
		t.Fatalf("stored bootstrap assignment = %#v, want open winner %#v", stored, winner)
	}
}

func TestBootstrapCloseRace(t *testing.T) {
	db := integrationDB(t)
	firstStore, err := adapter.New(db)
	if err != nil {
		t.Fatal(err)
	}
	secondStore, err := adapter.New(db)
	if err != nil {
		t.Fatal(err)
	}
	app := sgsp.AppIdentity{ID: "integration", Version: "1"}
	registry := memory.NewRegistry()
	for id := byte(0); id < 3; id++ {
		if err := registry.Register(context.Background(), integrationOwner(id)); err != nil {
			t.Fatal(err)
		}
	}
	first := integrationBootstrap(t, app, firstStore, registry)
	second := integrationBootstrap(t, app, secondStore, registry)
	principal := sgsp.Principal{Issuer: "integration", Subject: "player", ExpiresAt: time.Now().Add(time.Minute)}
	assigned, err := first.Resolve(context.Background(), principal, "close-race")
	if err != nil {
		t.Fatal(err)
	}

	const attempts = 100
	start := make(chan struct{})
	errs := make(chan error, attempts)
	var ready, workers sync.WaitGroup
	for index := 0; index < attempts; index++ {
		ready.Add(1)
		workers.Add(1)
		bootstrap := first
		if index%2 == 1 {
			bootstrap = second
		}
		go func(bootstrap *placement.Bootstrap) {
			defer workers.Done()
			ready.Done()
			<-start
			errs <- bootstrap.CloseGroup(context.Background(), "close-race", assigned.Owner)
		}(bootstrap)
	}
	ready.Wait()
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := second.Resolve(context.Background(), principal, "close-race"); !errors.Is(err, sgsp.ErrGroupClosed) {
		t.Fatalf("closed group bootstrap resolve = %v, want ErrGroupClosed", err)
	}
	stored, err := firstStore.Get(context.Background(), placement.GroupID{App: app, Key: "close-race"})
	if err != nil {
		t.Fatal(err)
	}
	if !stored.Closed || stored.Owner != assigned.Owner {
		t.Fatalf("stored closed assignment = %#v, want closed owner %#v", stored, assigned.Owner)
	}
}

func TestAssignmentVersionAndClose(t *testing.T) {
	db := integrationDB(t)
	store, err := adapter.New(db)
	if err != nil {
		t.Fatal(err)
	}
	group := placement.GroupID{App: sgsp.AppIdentity{ID: "integration", Version: "1"}, Key: "close"}
	owner := sgsp.Owner{ID: "owner", Incarnation: sgsp.Incarnation{1}, Endpoint: sgsp.Endpoint{Address: "127.0.0.1:1", ServerName: "owner"}}
	if _, err := store.Assign(context.Background(), group, owner); err != nil {
		t.Fatal(err)
	}
	wrongVersion := group
	wrongVersion.App.Version = "2"
	if _, err := store.Assign(context.Background(), wrongVersion, owner); !errors.Is(err, sgsp.ErrUnsupportedVersion) {
		t.Fatalf("version conflict = %v", err)
	}
	wrongOwner := owner
	wrongOwner.Incarnation[0]++
	if err := store.Close(context.Background(), group, wrongOwner); !errors.Is(err, sgsp.ErrForbidden) {
		t.Fatalf("wrong owner close = %v", err)
	}
	if err := store.Close(context.Background(), group, owner); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(context.Background(), group, owner); err != nil {
		t.Fatalf("idempotent close = %v", err)
	}
	if _, err := store.Assign(context.Background(), group, owner); !errors.Is(err, sgsp.ErrGroupClosed) {
		t.Fatalf("closed assignment reuse = %v", err)
	}
}

func peerDB(t *testing.T, db *sql.DB) *sql.DB {
	t.Helper()
	var seq int
	var name, path string
	if err := db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	peer, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=10000&_journal_mode=WAL&_synchronous=FULL")
	if err != nil {
		t.Fatal(err)
	}
	peer.SetMaxOpenConns(integrationMaxOpenConns)
	t.Cleanup(func() { peer.Close() })
	return peer
}
