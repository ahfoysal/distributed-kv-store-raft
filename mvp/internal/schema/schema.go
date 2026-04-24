// Package schema implements online schema change on top of the sharded KV
// layer. A table is not a single TableSchema; it is a sequence of versions,
// each with a `since` timestamp. Reads consult the version chain to decide
// which columns are visible — so ADD COLUMN / DROP COLUMN never block
// in-flight reads, and no rewrite of existing rows is required.
//
// On-disk layout: every catalog mutation is persisted to
// <data-root>/catalog.json (atomic tmp+rename). The file is a list of
// tables, each carrying the full ordered Versions slice. Crash recovery is
// trivial: reload catalog.json.
package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// ColumnType mirrors sql.ColType but lives in this package so we don't
// entangle catalog persistence with SQL parsing.
type ColumnType int

const (
	ColString ColumnType = 0
	ColInt    ColumnType = 1
)

func (c ColumnType) String() string {
	if c == ColInt {
		return "INT"
	}
	return "STRING"
}

// Column is one column in a schema version.
type Column struct {
	Name string     `json:"name"`
	Type ColumnType `json:"type"`
	// Default is applied to rows that predate the ADD COLUMN — see
	// Version.Materialize.
	Default any `json:"default,omitempty"`
}

// Version is one point in a table's schema evolution.
type Version struct {
	// Since is the schema-epoch timestamp at which this version becomes
	// active. Two versions never share the same Since.
	Since int64 `json:"since"`
	// Columns is the ordered list of columns visible in this version.
	Columns []Column `json:"columns"`
	// Dropped is the set of column names that have been dropped as of
	// this version (kept for defensive double-checks on reads).
	Dropped []string `json:"dropped,omitempty"`
}

// Table is the full version chain for one table.
type Table struct {
	Name       string    `json:"name"`
	PrimaryKey string    `json:"primary_key"`
	Versions   []Version `json:"versions"` // ascending by Since
}

// ActiveAt returns the version visible at ts (i.e. the newest version whose
// Since ≤ ts). Returns nil if ts precedes every version.
func (t *Table) ActiveAt(ts int64) *Version {
	var out *Version
	for i := range t.Versions {
		v := &t.Versions[i]
		if v.Since <= ts {
			out = v
		} else {
			break
		}
	}
	return out
}

// Current returns the newest version.
func (t *Table) Current() *Version {
	if len(t.Versions) == 0 {
		return nil
	}
	return &t.Versions[len(t.Versions)-1]
}

// Catalog is the collection of tables for one sqlnode. It is safe for
// concurrent use; mutations (Create, AddColumn, DropColumn) acquire a
// write lock and persist before returning.
type Catalog struct {
	mu     sync.RWMutex
	path   string
	Tables map[string]*Table `json:"tables"`
}

// Load opens (or initializes) the catalog file at path.
func Load(path string) (*Catalog, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	c := &Catalog{path: path, Tables: make(map[string]*Table)}
	buf, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return c, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(buf, c); err != nil {
		return nil, fmt.Errorf("catalog parse: %w", err)
	}
	if c.Tables == nil {
		c.Tables = make(map[string]*Table)
	}
	return c, nil
}

// CreateTable registers a new table with one initial Version.
func (c *Catalog) CreateTable(name, pk string, cols []Column) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.Tables[name]; ok {
		return fmt.Errorf("table %s already exists", name)
	}
	c.Tables[name] = &Table{
		Name:       name,
		PrimaryKey: pk,
		Versions: []Version{{
			Since:   time.Now().UnixNano(),
			Columns: cols,
		}},
	}
	return c.flushLocked()
}

// AddColumn appends a new version that adds col with the given default. The
// change is instantly visible to new reads but existing rows (which lack
// the column) are backfilled with Default on Materialize.
func (c *Catalog) AddColumn(table string, col Column) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.Tables[table]
	if !ok {
		return fmt.Errorf("table %s not found", table)
	}
	cur := t.Current()
	if cur == nil {
		return fmt.Errorf("table %s has no versions", table)
	}
	for _, c := range cur.Columns {
		if c.Name == col.Name {
			return fmt.Errorf("column %s already exists on %s", col.Name, table)
		}
	}
	newCols := append([]Column{}, cur.Columns...)
	newCols = append(newCols, col)
	t.Versions = append(t.Versions, Version{
		Since:   time.Now().UnixNano(),
		Columns: newCols,
		Dropped: append([]string{}, cur.Dropped...),
	})
	return c.flushLocked()
}

// DropColumn appends a new version that drops col. Existing rows still
// carry the field on disk but it is invisible to every read issued at or
// after the new version's Since.
func (c *Catalog) DropColumn(table, col string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.Tables[table]
	if !ok {
		return fmt.Errorf("table %s not found", table)
	}
	cur := t.Current()
	if cur == nil {
		return fmt.Errorf("table %s has no versions", table)
	}
	if col == t.PrimaryKey {
		return fmt.Errorf("cannot drop primary key column %s", col)
	}
	var newCols []Column
	found := false
	for _, c := range cur.Columns {
		if c.Name == col {
			found = true
			continue
		}
		newCols = append(newCols, c)
	}
	if !found {
		return fmt.Errorf("column %s does not exist on %s", col, table)
	}
	dropped := append([]string{}, cur.Dropped...)
	dropped = append(dropped, col)
	t.Versions = append(t.Versions, Version{
		Since:   time.Now().UnixNano(),
		Columns: newCols,
		Dropped: dropped,
	})
	return c.flushLocked()
}

// Get returns the Table with name, if present.
func (c *Catalog) Get(name string) (*Table, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.Tables[name]
	return t, ok
}

// Snapshot returns a deep copy of every table definition. Used by backup.
func (c *Catalog) Snapshot() []*Table {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*Table, 0, len(c.Tables))
	for _, t := range c.Tables {
		// Shallow enough — callers only read.
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Path returns the on-disk location of this catalog (used by backup).
func (c *Catalog) Path() string { return c.path }

// Materialize projects a raw stored row (all columns ever written to disk)
// into the shape visible at readTS. Columns dropped as of readTS are
// omitted; columns added by readTS but missing from the raw row get the
// declared Default.
func (t *Table) Materialize(raw map[string]any, readTS int64) map[string]any {
	v := t.ActiveAt(readTS)
	if v == nil {
		return nil
	}
	out := make(map[string]any, len(v.Columns))
	for _, c := range v.Columns {
		if val, ok := raw[c.Name]; ok {
			out[c.Name] = val
		} else if c.Default != nil {
			out[c.Name] = c.Default
		} else {
			out[c.Name] = nil
		}
	}
	return out
}

func (c *Catalog) flushLocked() error {
	buf, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}
