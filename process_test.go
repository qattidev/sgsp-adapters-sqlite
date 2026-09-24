package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"qattidev/sgsp"
	"qattidev/sgsp/placement"
)

func TestCrossProcessAssignment(t *testing.T) {
	group := placement.GroupID{App: sgsp.AppIdentity{ID: "process", Version: "1"}, Key: "durable"}
	if dsn := os.Getenv("SGSP_ADAPTER_CHILD_DSN"); dsn != "" {
		db, err := sql.Open("sqlite3", dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		store := newStore(t, db)
		candidate := integrationOwner(os.Getenv("SGSP_ADAPTER_CHILD_OWNER")[0])
		var winner sgsp.Owner
		for range 20 {
			got, err := store.Assign(context.Background(), group, candidate)
			if os.Getenv("SGSP_ADAPTER_CHILD_CLOSED") == "1" {
				if !errors.Is(err, sgsp.ErrGroupClosed) {
					t.Fatalf("closed record lost: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if winner.ID == "" {
				winner = got.Owner
			}
			if got.Owner != winner {
				t.Fatal("winner changed within child")
			}
		}
		return
	}
	db := integrationDB(t)
	dsn := processDSN(t, db)
	run := func(id int, closed bool) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestCrossProcessAssignment$", "-test.timeout=30s")
		cmd.Env = append(os.Environ(), "SGSP_ADAPTER_CHILD_DSN="+dsn, fmt.Sprintf("SGSP_ADAPTER_CHILD_OWNER=%d", id))
		if closed {
			cmd.Env = append(cmd.Env, "SGSP_ADAPTER_CHILD_CLOSED=1")
		}
		return cmd
	}
	results := make(chan error, 2)
	for i := range 2 {
		go func() {
			output, err := run(i, false).CombinedOutput()
			if err != nil {
				err = fmt.Errorf("child failed: %w: %s", err, output)
			}
			results <- err
		}()
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	store := newStore(t, db)
	got, err := store.Get(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}
	if got.Owner != integrationOwner('0') && got.Owner != integrationOwner('1') {
		t.Fatalf("unexpected winner: %#v", got)
	}
	if err := store.Close(context.Background(), group, got.Owner); err != nil {
		t.Fatal(err)
	}
	if output, err := run(2, true).CombinedOutput(); err != nil {
		t.Fatalf("closed child: %v: %s", err, output)
	}
}
