package utils

import (
	"fmt"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

type Expr struct {
	Delete bool   `parser:"@-"`
	Key    string `parser:"@Ident"`
	Value  string `parser:"( '=' @Rest )?"`
}

var lexerRules = lexer.MustSimple([]lexer.SimpleRule{
	{Name: "Ident", Pattern: `[A-Za-z_][A-Za-z0-9_]*`},
	{Name: "Punct", Pattern: `[-=]`},
	{Name: "Whitespace", Pattern: `\s+`},
})

var ManipulatorParser = participle.MustBuild[Expr](
	participle.Lexer(lexerRules),
	participle.Elide("Whitespace"),
)

// ParseManipulator parses a single override expression string.
func ParseManipulator(raw string) (*Expr, error) {
	token := strings.TrimSpace(raw)
	if token == "" {
		return nil, fmt.Errorf("empty override expression")
	}
	expr, err := ManipulatorParser.ParseString("", token)
	if err != nil {
		return nil, fmt.Errorf("parse override %q: %w", raw, err)
	}
	if expr.Key == "" {
		return nil, fmt.Errorf("override %q missing key", raw)
	}
	if expr.Delete && expr.Value != "" {
		return nil, fmt.Errorf("override %q deletes and assigns simultaneously", raw)
	}
	return expr, nil
}

// ParseManipulators parses multiple expressions in order.
func ParseManipulators(entries []string) ([]*Expr, error) {
	result := make([]*Expr, 0, len(entries))
	for _, entry := range entries {
		expr, err := ParseManipulator(entry)
		if err != nil {
			return nil, err
		}
		result = append(result, expr)
	}
	return result, nil
}
