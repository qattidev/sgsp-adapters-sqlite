package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"qattidev/sgsp"
	adapter "qattidev/sgsp-sqlite"
	"qattidev/sgsp/placement"
	"qattidev/sgsp/placement/memory"
)

func newStore(t *testing.T, db *sql.DB) *adapter.Store {
	t.Helper()
	s, err := adapter.New(db)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestContractIndependentConnections(t *testing.T) {
	db := integrationDB(t)
	peer := peerDB(t, db)
	stores := []*adapter.Store{newStore(t, db), newStore(t, peer)}
	ctx := context.Background()
	group := placement.GroupID{App: sgsp.AppIdentity{ID: "contract", Version: "1"}, Key: "winner"}
	start := make(chan struct{})
	results := make(chan placement.Assignment, 64)
	errs := make(chan error, 64)
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			got, err := stores[i%2].Assign(ctx, group, integrationOwner(byte(i%3)))
			results <- got
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var winner placement.Assignment
	for got := range results {
		if winner.Owner.ID == "" {
			winner = got
		}
		if got != winner {
			t.Fatalf("different winners: %#v / %#v", winner, got)
		}
	}
	for _, store := range stores {
		wrong := group
		wrong.App.Version = "2"
		if _, err := store.Get(ctx, wrong); !errors.Is(err, sgsp.ErrUnsupportedVersion) {
			t.Fatalf("Get version: %v", err)
		}
		if err := store.Close(ctx, wrong, winner.Owner); !errors.Is(err, sgsp.ErrUnsupportedVersion) {
			t.Fatalf("Close version: %v", err)
		}
		wrongOwner := winner.Owner
		wrongOwner.ID += "other"
		if err := store.Close(ctx, group, wrongOwner); !errors.Is(err, sgsp.ErrForbidden) {
			t.Fatalf("Close owner: %v", err)
		}
		missing := group
		missing.Key = "missing"
		if _, err := store.Get(ctx, missing); !errors.Is(err, placement.ErrAssignmentNotFound) {
			t.Fatalf("Get missing: %v", err)
		}
		if err := store.Close(ctx, missing, winner.Owner); !errors.Is(err, placement.ErrAssignmentNotFound) {
			t.Fatalf("Close missing: %v", err)
		}
	}
	// A closure racing with assignment must never change the winner or reopen it.
	start = make(chan struct{})
	errs = make(chan error, 64)
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if i%2 == 0 {
				errs <- stores[i%2].Close(ctx, group, winner.Owner)
				return
			}
			got, err := stores[i%2].Assign(ctx, group, integrationOwner(9))
			if err == nil && got.Owner != winner.Owner {
				err = errors.New("winner changed during close")
			}
			if errors.Is(err, sgsp.ErrGroupClosed) {
				err = nil
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	reopened := peerDB(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := newStore(t, reopened).Get(ctx, group)
	if err != nil || !got.Closed || got.Owner != winner.Owner {
		t.Fatalf("persisted closure: %#v, %v", got, err)
	}
	if _, err := newStore(t, reopened).Assign(ctx, group, winner.Owner); !errors.Is(err, sgsp.ErrGroupClosed) {
		t.Fatalf("reopened assignment: %v", err)
	}
}

func TestValidationCancellationAndOutage(t *testing.T) {
	db := integrationDB(t)
	store := newStore(t, db)
	ctx := context.Background()
	group := placement.GroupID{App: sgsp.AppIdentity{ID: "validation", Version: "1"}, Key: "key"}
	owner := integrationOwner(1)
	if _, err := adapter.New(nil); !errors.Is(err, sgsp.ErrInvalidArgument) {
		t.Fatal(err)
	}
	if err := adapter.ApplyMigrations(ctx, nil); !errors.Is(err, adapter.ErrInvalidDatabase) {
		t.Fatal(err)
	}
	for _, field := range []string{"app", "version", "key"} {
		bad := group
		switch field {
		case "app":
			bad.App.ID = ""
		case "version":
			bad.App.Version = ""
		case "key":
			bad.Key = ""
		}
		if _, err := store.Assign(ctx, bad, owner); !errors.Is(err, sgsp.ErrInvalidArgument) {
			t.Fatalf("Assign %s: %v", field, err)
		}
		if _, err := store.Get(ctx, bad); !errors.Is(err, sgsp.ErrInvalidArgument) {
			t.Fatalf("Get %s: %v", field, err)
		}
		if err := store.Close(ctx, bad, owner); !errors.Is(err, sgsp.ErrInvalidArgument) {
			t.Fatalf("Close %s: %v", field, err)
		}
	}
	for _, field := range []string{"id", "address", "server"} {
		bad := owner
		switch field {
		case "id":
			bad.ID = ""
		case "address":
			bad.Endpoint.Address = ""
		case "server":
			bad.Endpoint.ServerName = ""
		}
		if _, err := store.Assign(ctx, group, bad); !errors.Is(err, sgsp.ErrInvalidArgument) {
			t.Fatalf("Assign owner %s: %v", field, err)
		}
		if err := store.Close(ctx, group, bad); !errors.Is(err, sgsp.ErrInvalidArgument) {
			t.Fatalf("Close owner %s: %v", field, err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Assign(canceled, group, owner); !errors.Is(err, context.Canceled) {
		t.Fatalf("Assign cancellation: %v", err)
	}
	if _, err := store.Get(canceled, group); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get cancellation: %v", err)
	}
	if err := store.Close(canceled, group, owner); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close cancellation: %v", err)
	}
	expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel()
	if _, err := store.Assign(expired, group, owner); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline: %v", err)
	}
	db.Close()
	if _, err := store.Assign(ctx, group, owner); err == nil || errors.Is(err, placement.ErrAssignmentNotFound) {
		t.Fatalf("Assign outage: %v", err)
	}
	if _, err := store.Get(ctx, group); err == nil || errors.Is(err, placement.ErrAssignmentNotFound) {
		t.Fatalf("Get outage: %v", err)
	}
	if err := store.Close(ctx, group, owner); err == nil || errors.Is(err, placement.ErrAssignmentNotFound) {
		t.Fatalf("Close outage: %v", err)
	}
}

func TestBootstrapDurableWinner(t *testing.T) {
	db := integrationDB(t)
	peer := peerDB(t, db)
	firstStore, secondStore := newStore(t, db), newStore(t, peer)
	app := sgsp.AppIdentity{ID: "bootstrap", Version: "1"}
	registry := memory.NewRegistry()
	owner := integrationOwner(1)
	if err := registry.Register(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	first := integrationBootstrap(t, app, firstStore, registry)
	second := integrationBootstrap(t, app, secondStore, registry)
	principal := sgsp.Principal{Issuer: "test", Subject: "player", ExpiresAt: time.Now().Add(time.Minute)}
	got, err := first.Resolve(context.Background(), principal, "durable")
	if err != nil {
		t.Fatal(err)
	}
	if next, err := second.Resolve(context.Background(), principal, "durable"); err != nil || next.Owner != got.Owner {
		t.Fatalf("second bootstrap: %#v %v", next, err)
	}
	if err := registry.SetStatus(context.Background(), owner, false, false, true); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Resolve(context.Background(), principal, "durable"); err == nil {
		t.Fatal("unavailable owner accepted")
	}
	if err := registry.Remove(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	// A restarted owner must not take over the old incarnation's assignment.
	restarted := owner
	restarted.Incarnation[0]++
	if err := registry.Register(context.Background(), restarted); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Resolve(context.Background(), principal, "durable"); err == nil {
		t.Fatal("restarted owner accepted")
	}
	stored, err := secondStore.Get(context.Background(), placement.GroupID{App: app, Key: "durable"})
	if err != nil || stored.Owner != owner {
		t.Fatalf("reassigned on restart: %#v %v", stored, err)
	}
	if err := first.CloseGroup(context.Background(), "durable", owner); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Resolve(context.Background(), principal, "durable"); !errors.Is(err, sgsp.ErrGroupClosed) {
		t.Fatalf("closed bootstrap: %v", err)
	}
}
