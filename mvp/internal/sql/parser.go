package sql

import (
	"fmt"
	"strconv"
	"strings"
)

// Parse consumes one SQL statement and returns its AST.
func Parse(src string) (Stmt, error) {
	src = strings.TrimSpace(src)
	src = strings.TrimSuffix(src, ";")
	p := &parser{lex: newLexer(src)}
	if err := p.bump(); err != nil {
		return nil, err
	}
	return p.parseStmt()
}

type parser struct {
	lex *lexer
	cur token
}

func (p *parser) bump() error {
	t, err := p.lex.next()
	if err != nil {
		return err
	}
	p.cur = t
	return nil
}

func (p *parser) expectKeyword(kw string) error {
	if p.cur.kind != tkKeyword || p.cur.val != kw {
		return fmt.Errorf("expected %s, got %q", kw, p.cur.val)
	}
	return p.bump()
}

func (p *parser) expectPunct(punct string) error {
	if p.cur.kind != tkPunct || p.cur.val != punct {
		return fmt.Errorf("expected %q, got %q", punct, p.cur.val)
	}
	return p.bump()
}

func (p *parser) expectIdent() (string, error) {
	if p.cur.kind != tkIdent {
		return "", fmt.Errorf("expected identifier, got %q", p.cur.val)
	}
	name := p.cur.val
	return name, p.bump()
}

func (p *parser) parseStmt() (Stmt, error) {
	if p.cur.kind != tkKeyword {
		return nil, fmt.Errorf("expected SQL keyword, got %q", p.cur.val)
	}
	switch p.cur.val {
	case "CREATE":
		return p.parseCreate()
	case "INSERT":
		return p.parseInsert()
	case "SELECT":
		return p.parseSelect()
	case "UPDATE":
		return p.parseUpdate()
	case "DELETE":
		return p.parseDelete()
	default:
		return nil, fmt.Errorf("unsupported statement: %s", p.cur.val)
	}
}

func (p *parser) parseCreate() (Stmt, error) {
	if err := p.expectKeyword("CREATE"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}

	ct := &CreateTable{Name: name}
	for {
		// PRIMARY KEY (col)?
		if p.cur.kind == tkKeyword && p.cur.val == "PRIMARY" {
			if err := p.bump(); err != nil {
				return nil, err
			}
			if err := p.expectKeyword("KEY"); err != nil {
				return nil, err
			}
			if err := p.expectPunct("("); err != nil {
				return nil, err
			}
			pk, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			if err := p.expectPunct(")"); err != nil {
				return nil, err
			}
			ct.PrimaryKey = pk
		} else {
			col, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			if p.cur.kind != tkKeyword || (p.cur.val != "STRING" && p.cur.val != "INT") {
				return nil, fmt.Errorf("expected STRING or INT after %q, got %q", col, p.cur.val)
			}
			ty := TypeString
			if p.cur.val == "INT" {
				ty = TypeInt
			}
			if err := p.bump(); err != nil {
				return nil, err
			}
			ct.Columns = append(ct.Columns, ColumnDef{Name: col, Type: ty})
		}
		if p.cur.kind == tkPunct && p.cur.val == "," {
			if err := p.bump(); err != nil {
				return nil, err
			}
			continue
		}
		break
	}
	if err := p.expectPunct(")"); err != nil {
		return nil, err
	}
	if ct.PrimaryKey == "" && len(ct.Columns) > 0 {
		ct.PrimaryKey = ct.Columns[0].Name
	}
	return ct, nil
}

func (p *parser) parseInsert() (Stmt, error) {
	if err := p.expectKeyword("INSERT"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("INTO"); err != nil {
		return nil, err
	}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	ins := &Insert{Table: name}
	if p.cur.kind == tkPunct && p.cur.val == "(" {
		if err := p.bump(); err != nil {
			return nil, err
		}
		for {
			c, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			ins.Columns = append(ins.Columns, c)
			if p.cur.kind == tkPunct && p.cur.val == "," {
				if err := p.bump(); err != nil {
					return nil, err
				}
				continue
			}
			break
		}
		if err := p.expectPunct(")"); err != nil {
			return nil, err
		}
	}
	if err := p.expectKeyword("VALUES"); err != nil {
		return nil, err
	}
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}
	for {
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		ins.Values = append(ins.Values, v)
		if p.cur.kind == tkPunct && p.cur.val == "," {
			if err := p.bump(); err != nil {
				return nil, err
			}
			continue
		}
		break
	}
	if err := p.expectPunct(")"); err != nil {
		return nil, err
	}
	return ins, nil
}

func (p *parser) parseSelect() (Stmt, error) {
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}
	sel := &Select{}
	// cols
	if p.cur.kind == tkPunct && p.cur.val == "*" {
		if err := p.bump(); err != nil {
			return nil, err
		}
	} else {
		for {
			c, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			sel.Cols = append(sel.Cols, c)
			if p.cur.kind == tkPunct && p.cur.val == "," {
				if err := p.bump(); err != nil {
					return nil, err
				}
				continue
			}
			break
		}
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	sel.Table = name
	if p.cur.kind == tkKeyword && p.cur.val == "WHERE" {
		pred, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		sel.Where = pred
	}
	return sel, nil
}

func (p *parser) parseUpdate() (Stmt, error) {
	if err := p.expectKeyword("UPDATE"); err != nil {
		return nil, err
	}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	up := &Update{Table: name}
	if err := p.expectKeyword("SET"); err != nil {
		return nil, err
	}
	for {
		col, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		if err := p.expectPunct("="); err != nil {
			return nil, err
		}
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		up.Assign = append(up.Assign, Assignment{Col: col, Val: v})
		if p.cur.kind == tkPunct && p.cur.val == "," {
			if err := p.bump(); err != nil {
				return nil, err
			}
			continue
		}
		break
	}
	if p.cur.kind == tkKeyword && p.cur.val == "WHERE" {
		pred, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		up.Where = pred
	}
	return up, nil
}

func (p *parser) parseDelete() (Stmt, error) {
	if err := p.expectKeyword("DELETE"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	d := &Delete{Table: name}
	if p.cur.kind == tkKeyword && p.cur.val == "WHERE" {
		pred, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		d.Where = pred
	}
	return d, nil
}

func (p *parser) parseWhere() (*Predicate, error) {
	if err := p.expectKeyword("WHERE"); err != nil {
		return nil, err
	}
	col, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	// BETWEEN lo AND hi
	if p.cur.kind == tkKeyword && p.cur.val == "BETWEEN" {
		if err := p.bump(); err != nil {
			return nil, err
		}
		lo, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("AND"); err != nil {
			return nil, err
		}
		hi, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		return &Predicate{Kind: PredBetween, Col: col, Lo: lo, Hi: hi}, nil
	}
	// col = val
	if err := p.expectPunct("="); err != nil {
		return nil, err
	}
	v, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	return &Predicate{Kind: PredPointEq, Col: col, Val: v}, nil
}

func (p *parser) parseValue() (Value, error) {
	switch p.cur.kind {
	case tkString:
		v := Value{Str: p.cur.val}
		return v, p.bump()
	case tkNumber:
		n, err := strconv.ParseInt(p.cur.val, 10, 64)
		if err != nil {
			return Value{}, err
		}
		v := Value{IsInt: true, Int: n}
		return v, p.bump()
	}
	return Value{}, fmt.Errorf("expected literal, got %q", p.cur.val)
}

// formatInt is a small stdlib-free int formatter (keeps ast.go lean).
func formatInt(n int64) string { return strconv.FormatInt(n, 10) }
