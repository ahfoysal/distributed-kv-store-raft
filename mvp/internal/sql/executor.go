package sql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/foysal/distkv/internal/schema"
	"github.com/foysal/distkv/internal/shard"
)

// KV is the narrow view of the sharded store the executor needs. It matches
// the methods shard.Router already exposes, but keeping it as an interface
// lets tests substitute a mock.
type KV interface {
	Put(key, value string) error
	Get(key string) (string, bool)
	Delete(key string) error
	Scan(start, end string) []shard.KV
}

// Engine is the SQL layer wrapped around a KV store. It owns the catalog
// (schemas of declared tables) and exposes Exec to run a single statement.
type Engine struct {
	kv KV

	mu     sync.RWMutex
	tables map[string]*TableSchema

	// Optional: if non-nil, CREATE / ALTER / SELECT also go through the
	// persistent schema catalog so schema changes survive restarts and are
	// versioned (M6 online schema change).
	cat *schema.Catalog
}

// AttachCatalog wires a persistent schema catalog into the engine. When
// set, CREATE TABLE also registers an initial Version in the catalog,
// ALTER TABLE appends a new Version, and SELECT/INSERT/UPDATE use the
// catalog as the source of truth. Safe to call once at startup, before
// the engine starts serving requests.
func (e *Engine) AttachCatalog(cat *schema.Catalog) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cat = cat
	// Hydrate the in-memory TableSchema map from the persisted catalog so
	// existing tables survive restart.
	for _, t := range cat.Snapshot() {
		v := t.Current()
		if v == nil {
			continue
		}
		cols := make([]ColumnDef, 0, len(v.Columns))
		for _, c := range v.Columns {
			ty := TypeString
			if c.Type == schema.ColInt {
				ty = TypeInt
			}
			cols = append(cols, ColumnDef{Name: c.Name, Type: ty})
		}
		e.tables[t.Name] = &TableSchema{Name: t.Name, Columns: cols, PrimaryKey: t.PrimaryKey}
	}
}

// TableSchema is the in-memory catalog entry for one table.
type TableSchema struct {
	Name       string
	Columns    []ColumnDef
	PrimaryKey string
}

// NewEngine constructs an empty-catalog engine over kv.
func NewEngine(kv KV) *Engine {
	return &Engine{kv: kv, tables: make(map[string]*TableSchema)}
}

// Result is the output of Exec. Shape depends on the statement kind.
type Result struct {
	// Columns is set for SELECT.
	Columns []string `json:"columns,omitempty"`
	// Rows is set for SELECT. Each row is aligned with Columns.
	Rows [][]any `json:"rows,omitempty"`
	// RowsAffected is set for INSERT/UPDATE/DELETE.
	RowsAffected int `json:"rows_affected,omitempty"`
	// Message is a human-readable status (e.g. "table created").
	Message string `json:"message,omitempty"`
}

// Exec parses and runs one SQL statement.
func (e *Engine) Exec(src string) (*Result, error) {
	stmt, err := Parse(src)
	if err != nil {
		return nil, err
	}
	switch s := stmt.(type) {
	case *CreateTable:
		return e.execCreate(s)
	case *Insert:
		return e.execInsert(s)
	case *Select:
		return e.execSelect(s)
	case *Update:
		return e.execUpdate(s)
	case *Delete:
		return e.execDelete(s)
	case *AlterTable:
		return e.execAlter(s)
	default:
		return nil, fmt.Errorf("unsupported stmt")
	}
}

func (e *Engine) execCreate(s *CreateTable) (*Result, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.tables[s.Name]; ok {
		return nil, fmt.Errorf("table %s already exists", s.Name)
	}
	if s.PrimaryKey == "" {
		return nil, fmt.Errorf("table %s: no primary key", s.Name)
	}
	// validate PK column exists
	found := false
	for _, c := range s.Columns {
		if c.Name == s.PrimaryKey {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("primary key column %q not declared", s.PrimaryKey)
	}
	e.tables[s.Name] = &TableSchema{Name: s.Name, Columns: s.Columns, PrimaryKey: s.PrimaryKey}
	if e.cat != nil {
		cols := make([]schema.Column, 0, len(s.Columns))
		for _, c := range s.Columns {
			ty := schema.ColString
			if c.Type == TypeInt {
				ty = schema.ColInt
			}
			cols = append(cols, schema.Column{Name: c.Name, Type: ty})
		}
		if err := e.cat.CreateTable(s.Name, s.PrimaryKey, cols); err != nil {
			return nil, err
		}
	}
	return &Result{Message: "table " + s.Name + " created"}, nil
}

func (e *Engine) execAlter(s *AlterTable) (*Result, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ts, ok := e.tables[s.Table]
	if !ok {
		return nil, fmt.Errorf("unknown table %s", s.Table)
	}
	switch s.Op {
	case "add":
		for _, c := range ts.Columns {
			if c.Name == s.Col.Name {
				return nil, fmt.Errorf("column %s already exists on %s", s.Col.Name, s.Table)
			}
		}
		ts.Columns = append(ts.Columns, s.Col)
		if e.cat != nil {
			ty := schema.ColString
			if s.Col.Type == TypeInt {
				ty = schema.ColInt
			}
			var def any
			if s.HasDef {
				def = valueGo(s.Default)
			}
			if err := e.cat.AddColumn(s.Table, schema.Column{Name: s.Col.Name, Type: ty, Default: def}); err != nil {
				return nil, err
			}
		}
		return &Result{Message: fmt.Sprintf("table %s: added column %s", s.Table, s.Col.Name)}, nil
	case "drop":
		if s.DropName == ts.PrimaryKey {
			return nil, fmt.Errorf("cannot drop primary key column %s", s.DropName)
		}
		out := ts.Columns[:0]
		found := false
		for _, c := range ts.Columns {
			if c.Name == s.DropName {
				found = true
				continue
			}
			out = append(out, c)
		}
		if !found {
			return nil, fmt.Errorf("column %s does not exist on %s", s.DropName, s.Table)
		}
		ts.Columns = out
		if e.cat != nil {
			if err := e.cat.DropColumn(s.Table, s.DropName); err != nil {
				return nil, err
			}
		}
		return &Result{Message: fmt.Sprintf("table %s: dropped column %s", s.Table, s.DropName)}, nil
	}
	return nil, fmt.Errorf("unknown ALTER op %q", s.Op)
}

// rowKey encodes (table, pk) → "t/<table>/<pk>".
func rowKey(table, pk string) string { return "t/" + table + "/" + pk }

// rowPrefix is the prefix scan range for a table: ["t/<table>/", "t/<table>0").
// '0' is the byte immediately after '/' in ASCII, so it's the smallest key
// that does NOT belong to this table.
func rowPrefix(table string) (string, string) {
	return "t/" + table + "/", "t/" + table + "0"
}

func (e *Engine) execInsert(s *Insert) (*Result, error) {
	e.mu.RLock()
	ts, ok := e.tables[s.Table]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown table %s", s.Table)
	}
	cols := s.Columns
	if cols == nil {
		cols = make([]string, len(ts.Columns))
		for i, c := range ts.Columns {
			cols[i] = c.Name
		}
	}
	if len(cols) != len(s.Values) {
		return nil, fmt.Errorf("column/value count mismatch")
	}
	row := map[string]any{}
	for i, name := range cols {
		if err := typecheck(ts, name, s.Values[i]); err != nil {
			return nil, err
		}
		row[name] = valueGo(s.Values[i])
	}
	pkVal, ok := row[ts.PrimaryKey]
	if !ok {
		return nil, fmt.Errorf("primary key %q not supplied", ts.PrimaryKey)
	}
	pk := anyToString(pkVal)
	buf, _ := json.Marshal(row)
	if err := e.kv.Put(rowKey(s.Table, pk), string(buf)); err != nil {
		return nil, err
	}
	return &Result{RowsAffected: 1}, nil
}

func (e *Engine) execSelect(s *Select) (*Result, error) {
	e.mu.RLock()
	ts, ok := e.tables[s.Table]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown table %s", s.Table)
	}
	cols := s.Cols
	if cols == nil {
		cols = make([]string, len(ts.Columns))
		for i, c := range ts.Columns {
			cols[i] = c.Name
		}
	}
	rows, err := e.collect(ts, s.Where)
	if err != nil {
		return nil, err
	}
	out := make([][]any, 0, len(rows))
	for _, r := range rows {
		rec := make([]any, len(cols))
		for i, c := range cols {
			rec[i] = r[c]
		}
		out = append(out, rec)
	}
	return &Result{Columns: cols, Rows: out}, nil
}

func (e *Engine) execUpdate(s *Update) (*Result, error) {
	e.mu.RLock()
	ts, ok := e.tables[s.Table]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown table %s", s.Table)
	}
	rows, err := e.collect(ts, s.Where)
	if err != nil {
		return nil, err
	}
	n := 0
	for _, r := range rows {
		for _, a := range s.Assign {
			if a.Col == ts.PrimaryKey {
				return nil, fmt.Errorf("cannot UPDATE primary key")
			}
			if err := typecheck(ts, a.Col, a.Val); err != nil {
				return nil, err
			}
			r[a.Col] = valueGo(a.Val)
		}
		pk := anyToString(r[ts.PrimaryKey])
		buf, _ := json.Marshal(r)
		if err := e.kv.Put(rowKey(s.Table, pk), string(buf)); err != nil {
			return nil, err
		}
		n++
	}
	return &Result{RowsAffected: n}, nil
}

func (e *Engine) execDelete(s *Delete) (*Result, error) {
	e.mu.RLock()
	ts, ok := e.tables[s.Table]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown table %s", s.Table)
	}
	rows, err := e.collect(ts, s.Where)
	if err != nil {
		return nil, err
	}
	n := 0
	for _, r := range rows {
		pk := anyToString(r[ts.PrimaryKey])
		if err := e.kv.Delete(rowKey(s.Table, pk)); err != nil {
			return nil, err
		}
		n++
	}
	return &Result{RowsAffected: n}, nil
}

// collect resolves a WHERE clause into the list of matching rows, using the
// sharded KV's fastest available access path.
//
//	WHERE pk = v              → point Get
//	WHERE pk BETWEEN lo AND hi → ranged Scan
//	WHERE nonpk = v            → full-table scan + filter
//	no WHERE                   → full-table scan
func (e *Engine) collect(ts *TableSchema, pred *Predicate) ([]map[string]any, error) {
	if pred == nil {
		return e.fullScan(ts, nil)
	}
	switch pred.Kind {
	case PredPointEq:
		if pred.Col == ts.PrimaryKey {
			pk := anyToString(valueGo(pred.Val))
			raw, ok := e.kv.Get(rowKey(ts.Name, pk))
			if !ok {
				return nil, nil
			}
			var row map[string]any
			if err := json.Unmarshal([]byte(raw), &row); err != nil {
				return nil, err
			}
			return []map[string]any{row}, nil
		}
		return e.fullScan(ts, func(r map[string]any) bool {
			return equalAny(r[pred.Col], valueGo(pred.Val))
		})
	case PredBetween:
		if pred.Col != ts.PrimaryKey {
			return e.fullScan(ts, func(r map[string]any) bool {
				return betweenAny(r[pred.Col], valueGo(pred.Lo), valueGo(pred.Hi))
			})
		}
		lo := rowKey(ts.Name, anyToString(valueGo(pred.Lo)))
		hi := rowKey(ts.Name, anyToString(valueGo(pred.Hi))+"\x00")
		kvs := e.kv.Scan(lo, hi)
		var rows []map[string]any
		for _, kv := range kvs {
			var r map[string]any
			if err := json.Unmarshal([]byte(kv.Value), &r); err != nil {
				return nil, err
			}
			rows = append(rows, r)
		}
		return rows, nil
	}
	return nil, fmt.Errorf("unsupported predicate")
}

func (e *Engine) fullScan(ts *TableSchema, filter func(map[string]any) bool) ([]map[string]any, error) {
	start, end := rowPrefix(ts.Name)
	kvs := e.kv.Scan(start, end)
	var rows []map[string]any
	for _, kv := range kvs {
		var r map[string]any
		if err := json.Unmarshal([]byte(kv.Value), &r); err != nil {
			return nil, err
		}
		if filter == nil || filter(r) {
			rows = append(rows, r)
		}
	}
	return rows, nil
}

// typecheck verifies v matches the declared type of col in ts.
func typecheck(ts *TableSchema, col string, v Value) error {
	for _, c := range ts.Columns {
		if c.Name != col {
			continue
		}
		if c.Type == TypeInt && !v.IsInt {
			return fmt.Errorf("column %s is INT, got string literal", col)
		}
		if c.Type == TypeString && v.IsInt {
			return fmt.Errorf("column %s is STRING, got int literal", col)
		}
		return nil
	}
	return fmt.Errorf("unknown column %s", col)
}

func valueGo(v Value) any {
	if v.IsInt {
		return v.Int
	}
	return v.Str
}

func anyToString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		// JSON decoding turns numbers into float64; format as integer when exact.
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	}
	return fmt.Sprintf("%v", v)
}

func equalAny(a, b any) bool {
	return anyToString(a) == anyToString(b)
}

func betweenAny(v, lo, hi any) bool {
	// For INT values compare numerically; else lexically via string form.
	if vi, ok := toInt(v); ok {
		li, lok := toInt(lo)
		hi2, hok := toInt(hi)
		if lok && hok {
			return vi >= li && vi <= hi2
		}
	}
	s := anyToString(v)
	return s >= anyToString(lo) && s <= anyToString(hi)
}

func toInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case float64:
		if x == float64(int64(x)) {
			return int64(x), true
		}
	}
	return 0, false
}
