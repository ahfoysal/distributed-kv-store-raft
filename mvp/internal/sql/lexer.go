// Package sql is a tiny SQL frontend that compiles a narrow subset of SQL
// down to KV operations on the sharded store.
//
// Grammar (case-insensitive keywords, single-quoted string literals):
//
//	CREATE TABLE <name> ( <col> <type>, ... [PRIMARY KEY (<col>)] )
//	INSERT INTO <name> (<cols>) VALUES (<vals>)
//	SELECT <cols>|* FROM <name> [WHERE <col> = <val> | <col> BETWEEN <v> AND <v>]
//	UPDATE <name> SET <col> = <val>, ... WHERE <col> = <val>
//	DELETE FROM <name> WHERE <col> = <val>
//
// Supported column types are STRING and INT. Every row is encoded as one KV
// entry: key = "t/<table>/<pk>", value = JSON object of the row. PK-equality
// WHERE becomes a point Get; WHERE on PK BETWEEN becomes a Scan; anything
// else falls back to a full-table scan with a filter in the executor.
package sql

import (
	"fmt"
	"strings"
	"unicode"
)

type tokenKind int

const (
	tkEOF tokenKind = iota
	tkIdent
	tkNumber
	tkString
	tkPunct
	tkKeyword
)

type token struct {
	kind tokenKind
	val  string
}

var keywords = map[string]bool{
	"CREATE": true, "TABLE": true, "INSERT": true, "INTO": true,
	"VALUES": true, "SELECT": true, "FROM": true, "WHERE": true,
	"UPDATE": true, "SET": true, "DELETE": true, "AND": true,
	"PRIMARY": true, "KEY": true, "STRING": true, "INT": true,
	"BETWEEN": true,
	"ALTER": true, "ADD": true, "DROP": true, "COLUMN": true, "DEFAULT": true,
}

type lexer struct {
	src string
	pos int
}

func newLexer(src string) *lexer { return &lexer{src: src} }

func (l *lexer) peek() (token, error) {
	save := l.pos
	tok, err := l.next()
	l.pos = save
	return tok, err
}

func (l *lexer) next() (token, error) {
	// skip whitespace
	for l.pos < len(l.src) && unicode.IsSpace(rune(l.src[l.pos])) {
		l.pos++
	}
	if l.pos >= len(l.src) {
		return token{kind: tkEOF}, nil
	}
	ch := l.src[l.pos]
	// punctuation
	if strings.ContainsRune("(),*=;", rune(ch)) {
		l.pos++
		return token{kind: tkPunct, val: string(ch)}, nil
	}
	// string literal
	if ch == '\'' {
		l.pos++
		start := l.pos
		for l.pos < len(l.src) && l.src[l.pos] != '\'' {
			l.pos++
		}
		if l.pos >= len(l.src) {
			return token{}, fmt.Errorf("unterminated string literal")
		}
		v := l.src[start:l.pos]
		l.pos++ // consume closing quote
		return token{kind: tkString, val: v}, nil
	}
	// number
	if unicode.IsDigit(rune(ch)) || (ch == '-' && l.pos+1 < len(l.src) && unicode.IsDigit(rune(l.src[l.pos+1]))) {
		start := l.pos
		if ch == '-' {
			l.pos++
		}
		for l.pos < len(l.src) && unicode.IsDigit(rune(l.src[l.pos])) {
			l.pos++
		}
		return token{kind: tkNumber, val: l.src[start:l.pos]}, nil
	}
	// identifier / keyword
	if unicode.IsLetter(rune(ch)) || ch == '_' {
		start := l.pos
		for l.pos < len(l.src) && (unicode.IsLetter(rune(l.src[l.pos])) || unicode.IsDigit(rune(l.src[l.pos])) || l.src[l.pos] == '_') {
			l.pos++
		}
		word := l.src[start:l.pos]
		up := strings.ToUpper(word)
		if keywords[up] {
			return token{kind: tkKeyword, val: up}, nil
		}
		return token{kind: tkIdent, val: word}, nil
	}
	return token{}, fmt.Errorf("unexpected character %q at position %d", ch, l.pos)
}
