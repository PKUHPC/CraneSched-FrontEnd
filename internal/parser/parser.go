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
		{Name: "Whitespace", Pattern: `\s+`},
	})
)

type Command struct {
	Where []*Filter    `parser:"'where'? @@+"` // Implicit WHERE
	Set   []*Operation `parser:"'set'? @@*"`   // Explicit SET
}

type Filter struct {
	Key   string `parser:"@Ident '='? "`
	Value Value  `parser:"@@?"`
}

type Operation struct {
	Key      string `parser:"@Ident"`
	Operator string `parser:"@Operator"`
	Value    Value  `parser:"@@"`
}

type Value interface{ v() string }

type StringVal struct {
	Value string `parser:"@String"`
}

func (val StringVal) v() string {
	return val.Value
}

type NumberVal struct {
	Value float64 `parser:"@Number"`
}

func (val NumberVal) v() string {
	return fmt.Sprintf("%f", val.Value)
}

type IdentVal struct {
	Value string `parser:"@Ident"`
} // If no quotes, it is an IdentVal

func (val IdentVal) v() string {
	return val.Value
}

func GetParser() *participle.Parser[Command] {
	parser := participle.MustBuild[Command](
		participle.Lexer(cmdLexer),
		participle.Unquote("String"),
		participle.Union[Value](StringVal{}, NumberVal{}, IdentVal{}),
		participle.Elide("Whitespace"),
	)

	return parser
}

func Parse(s string) (*Command, error) {
	parser := GetParser()
	cmd, err := parser.ParseString("", s)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}
