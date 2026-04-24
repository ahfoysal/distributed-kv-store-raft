package sql

import (
	"path/filepath"
	"testing"

	"github.com/foysal/distkv/internal/shard"
)

func newTestEngine(t *testing.T) (*Engine, *shard.Router) {
	t.Helper()
	dir := t.TempDir()
	r, err := shard.NewRouter(shard.Config{
		DataRoot:       dir,
		ManifestPath:   filepath.Join(dir, "manifest.json"),
		SplitThreshold: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	return NewEngine(r), r
}

func TestCreateInsertSelect(t *testing.T) {
	e, r := newTestEngine(t)
	defer r.Close()

	if _, err := e.Exec("CREATE TABLE users (id INT, name STRING, PRIMARY KEY (id))"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Exec("INSERT INTO users (id, name) VALUES (1, 'alice')"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Exec("INSERT INTO users (id, name) VALUES (2, 'bob')"); err != nil {
		t.Fatal(err)
	}
	res, err := e.Exec("SELECT name FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if res.Rows[0][0] != "alice" {
		t.Fatalf("expected alice, got %v", res.Rows[0][0])
	}
	// SELECT * returns everything
	res, err = e.Exec("SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}
}

func TestUpdateDelete(t *testing.T) {
	e, r := newTestEngine(t)
	defer r.Close()

	mustExec(t, e, "CREATE TABLE t (k STRING, v STRING, PRIMARY KEY (k))")
	mustExec(t, e, "INSERT INTO t (k, v) VALUES ('a', '1')")
	mustExec(t, e, "INSERT INTO t (k, v) VALUES ('b', '2')")

	res := mustExec(t, e, "UPDATE t SET v = '99' WHERE k = 'a'")
	if res.RowsAffected != 1 {
		t.Fatalf("expected 1 row updated, got %d", res.RowsAffected)
	}
	res = mustExec(t, e, "SELECT v FROM t WHERE k = 'a'")
	if res.Rows[0][0] != "99" {
		t.Fatalf("after UPDATE, expected 99, got %v", res.Rows[0][0])
	}
	res = mustExec(t, e, "DELETE FROM t WHERE k = 'b'")
	if res.RowsAffected != 1 {
		t.Fatalf("expected 1 row deleted")
	}
	res = mustExec(t, e, "SELECT k FROM t")
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row remaining")
	}
}

func TestBetween(t *testing.T) {
	e, r := newTestEngine(t)
	defer r.Close()

	mustExec(t, e, "CREATE TABLE m (k STRING, v STRING, PRIMARY KEY (k))")
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustExec(t, e, "INSERT INTO m (k, v) VALUES ('"+k+"', 'x')")
	}
	res := mustExec(t, e, "SELECT k FROM m WHERE k BETWEEN 'b' AND 'd'")
	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 rows (b,c,d), got %d", len(res.Rows))
	}
}

func TestRoutesAcrossShardsAfterSplit(t *testing.T) {
	e, r := newTestEngine(t)
	defer r.Close()

	mustExec(t, e, "CREATE TABLE t (k STRING, v STRING, PRIMARY KEY (k))")
	for i := 0; i < 50; i++ {
		mustExec(t, e, "INSERT INTO t (k, v) VALUES ('"+formatZeroPad(i)+"', 'x')")
	}
	// Split right through the middle of the single shard.
	if _, _, err := r.Split("s0", "t/t/25"); err != nil {
		t.Fatal(err)
	}
	// A full-table SELECT must still return all 50 rows.
	res := mustExec(t, e, "SELECT k FROM t")
	if len(res.Rows) != 50 {
		t.Fatalf("expected 50 rows after split, got %d", len(res.Rows))
	}
	// A point SELECT on each side of the split must still find its row.
	res = mustExec(t, e, "SELECT v FROM t WHERE k = '05'")
	if len(res.Rows) != 1 {
		t.Fatalf("expected to find '05' after split")
	}
	res = mustExec(t, e, "SELECT v FROM t WHERE k = '40'")
	if len(res.Rows) != 1 {
		t.Fatalf("expected to find '40' after split")
	}
}

func mustExec(t *testing.T, e *Engine, q string) *Result {
	t.Helper()
	res, err := e.Exec(q)
	if err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
	return res
}

func formatZeroPad(i int) string {
	if i < 10 {
		return "0" + itoa(i)
	}
	return itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	n := len(buf)
	for i > 0 {
		n--
		buf[n] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[n:])
}
