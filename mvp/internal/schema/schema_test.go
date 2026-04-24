package schema

import (
	"path/filepath"
	"testing"
	"time"
)

func TestOnlineAddDropColumn(t *testing.T) {
	dir := t.TempDir()
	cat, err := Load(filepath.Join(dir, "catalog.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateTable("users", "id", []Column{
		{Name: "id", Type: ColString}, {Name: "name", Type: ColString},
	}); err != nil {
		t.Fatal(err)
	}
	// Simulate a row written at t_v1.
	users, _ := cat.Get("users")
	v1 := users.Current()
	rawRow := map[string]any{"id": "u1", "name": "alice"}

	// An older reader at v1's Since sees only id+name.
	out := users.Materialize(rawRow, v1.Since)
	if _, ok := out["email"]; ok {
		t.Fatalf("v1 read must not expose email column")
	}

	// ADD COLUMN email.
	time.Sleep(2 * time.Millisecond)
	if err := cat.AddColumn("users", Column{Name: "email", Type: ColString, Default: ""}); err != nil {
		t.Fatal(err)
	}
	users, _ = cat.Get("users")
	v2 := users.Current()
	if v2.Since <= v1.Since {
		t.Fatalf("v2.Since %d must be strictly after v1.Since %d", v2.Since, v1.Since)
	}
	// A new read at v2's time sees email = default.
	out2 := users.Materialize(rawRow, v2.Since)
	if out2["email"] != "" {
		t.Fatalf("expected email default to be \"\", got %v", out2["email"])
	}
	// An old-transaction read at v1's time STILL does not see email.
	out3 := users.Materialize(rawRow, v1.Since)
	if _, ok := out3["email"]; ok {
		t.Fatalf("legacy snapshot must not see email column")
	}

	// DROP COLUMN name.
	time.Sleep(2 * time.Millisecond)
	if err := cat.DropColumn("users", "name"); err != nil {
		t.Fatal(err)
	}
	users, _ = cat.Get("users")
	v3 := users.Current()
	out4 := users.Materialize(rawRow, v3.Since)
	if _, ok := out4["name"]; ok {
		t.Fatalf("v3 read must not see dropped column name")
	}
	if out4["email"] != "" {
		t.Fatalf("v3 must still see email (default)")
	}
	// Old snapshot at v1 still sees name.
	out5 := users.Materialize(rawRow, v1.Since)
	if out5["name"] != "alice" {
		t.Fatalf("v1 snapshot must still see name=alice, got %v", out5["name"])
	}

	// Can't drop PK.
	if err := cat.DropColumn("users", "id"); err == nil {
		t.Fatalf("dropping PK should fail")
	}
}

func TestCatalogPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.json")
	cat, _ := Load(path)
	if err := cat.CreateTable("t", "k", []Column{{Name: "k", Type: ColString}}); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddColumn("t", Column{Name: "v", Type: ColInt, Default: int64(0)}); err != nil {
		t.Fatal(err)
	}
	// Reload from disk.
	cat2, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	tbl, ok := cat2.Get("t")
	if !ok {
		t.Fatalf("table lost across reload")
	}
	if len(tbl.Versions) != 2 {
		t.Fatalf("want 2 versions after reload, got %d", len(tbl.Versions))
	}
	if len(tbl.Current().Columns) != 2 {
		t.Fatalf("want 2 current columns, got %d", len(tbl.Current().Columns))
	}
}
