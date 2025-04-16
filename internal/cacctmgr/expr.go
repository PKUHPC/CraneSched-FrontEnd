package cacctmgr

import (
	"CraneFrontEnd/internal/parser"
)

type Expr struct {
	Where []*Filter    `parser:"'where'? Space? @@+ Space?"` // Implicit WHERE
	Set   []*Operation `parser:"'set'? Space? @@* Space?"`   // Explicit SET
}

type Filter struct {
	Key   string         `parser:"@Ident Space? '='? Space?"`
	Value []parser.Value `parser:"@@ ( ( Comma | Space (?! Ident Space? '=')) @@ )* Space?"`
}

type Operation struct {
	Key      string       `parser:"@Ident Space?"`
	Operator string       `parser:"@Operator Space?"`
	Value    parser.Value `parser:"@@ Space?"`
}
