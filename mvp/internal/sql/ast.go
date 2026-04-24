package sql

// Stmt is the sum type returned by Parse.
type Stmt interface{ stmtNode() }

// Column type tags.
type ColType int

const (
	TypeString ColType = iota
	TypeInt
)

func (c ColType) String() string {
	if c == TypeInt {
		return "INT"
	}
	return "STRING"
}

// ColumnDef describes one column of a CREATE TABLE statement.
type ColumnDef struct {
	Name string
	Type ColType
}

// CreateTable: CREATE TABLE t (k STRING, v STRING [, ...] [PRIMARY KEY (k)])
type CreateTable struct {
	Name       string
	Columns    []ColumnDef
	PrimaryKey string // name of the PK column; defaults to the first column
}

func (*CreateTable) stmtNode() {}

// Insert: INSERT INTO t (c1, c2) VALUES (v1, v2)
type Insert struct {
	Table   string
	Columns []string // may be nil → implicit "all columns in declaration order"
	Values  []Value
}

func (*Insert) stmtNode() {}

// Select: SELECT cols FROM t WHERE ...
type Select struct {
	Table string
	Cols  []string // nil = "*"
	Where *Predicate
}

func (*Select) stmtNode() {}

// Update: UPDATE t SET c=v, ... WHERE ...
type Update struct {
	Table  string
	Assign []Assignment
	Where  *Predicate
}

func (*Update) stmtNode() {}

// Delete: DELETE FROM t WHERE ...
type Delete struct {
	Table string
	Where *Predicate
}

func (*Delete) stmtNode() {}

// Assignment is a single col=val in UPDATE ... SET.
type Assignment struct {
	Col string
	Val Value
}

// Value is a literal: either an INT or a STRING.
type Value struct {
	IsInt bool
	Int   int64
	Str   string
}

// String returns the SQL-level string representation.
func (v Value) String() string {
	if v.IsInt {
		return fmtInt(v.Int)
	}
	return "'" + v.Str + "'"
}

func fmtInt(n int64) string {
	// avoid fmt dependency noise; use the stdlib strconv via a local helper
	return formatInt(n)
}

// Predicate is a WHERE clause of the forms we support:
//
//	col = value                  → PointEq
//	col BETWEEN lo AND hi        → Between
type Predicate struct {
	Kind PredKind
	Col  string
	Val  Value // for PointEq
	Lo   Value // for Between (inclusive)
	Hi   Value // for Between (inclusive)
}

type PredKind int

const (
	PredPointEq PredKind = iota
	PredBetween
)
