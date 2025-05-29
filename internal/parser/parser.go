package parser

import (
	"fmt"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

var (
	cmdLexer = lexer.MustSimple([]lexer.SimpleRule{
		{Name: "Reserved", Pattern: `\b(where|set)\b`},
		{Name: "Ident", Pattern: `[a-zA-Z_][a-zA-Z0-9_]*`},
		{Name: "String", Pattern: `"(?:\\.|[^"])*"`},
		{Name: "Number", Pattern: `-?\d+(?:\.\d+)?`},
		{Name: "Operator", Pattern: `[+-]?=`},
		{Name: "Comma", Pattern: `\s*,\s*`},
		{Name: "Space", Pattern: `\s+`},
	})
)

type Value interface{ v() string }

type StringVal struct {
	Value string `parser:"@String"`
}
type NumberVal struct {
	Value float64 `parser:"@Number"`
}
type IdentVal struct {
	Value string `parser:"@Ident"`
} // If no quotes, it is an IdentVal

func (val StringVal) v() string {
	return val.Value
}
func (val NumberVal) v() string {
	return fmt.Sprintf("%f", val.Value)
}
func (val IdentVal) v() string {
	return val.Value
}

func GetParser[T any]() *participle.Parser[T] {
	parser := participle.MustBuild[T](
		participle.Lexer(cmdLexer),
		participle.Unquote("String"),
		participle.Union[Value](StringVal{}, NumberVal{}, IdentVal{}),
	)

	return parser
}

func Parse[T any](s string) (*T, error) {
	parser := GetParser[T]()
	expr, err := parser.ParseString("", s)
	if err != nil {
		return expr, fmt.Errorf("failed to parse input: %w", err)
	}
	return expr, nil
}
