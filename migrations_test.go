package sqlite

import (
	"io/fs"
	"strings"
	"testing"
)

func TestEmbeddedMigrationDefinitions(t *testing.T) {
	names, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil || len(names) == 0 {
		t.Fatalf("migrations: %v %v", names, err)
	}
	for _, name := range names {
		content, err := migrations.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, required := range []string{"-- +goose Up", "-- +goose Down", "sgsp_group_assignments"} {
			if !strings.Contains(string(content), required) {
				t.Fatalf("%s missing %s", name, required)
			}
		}
	}
}
