package ccontrol

import (
	"CraneFrontEnd/internal/parser"
)

type Expr struct {
	Where *Filter      `parser:"@@ Space?"`  // in ccontrol, only 1 entity allowed
	Set   []*Operation `parser:"@@* Space?"` // implicit SET
}

type Filter struct {
	Key   string         `parser:"@Ident ( '=' | Space )"`
	Value []parser.Value `parser:"@@ ( Comma @@ )* Space?"` // comma separated
}

type Operation struct {
	Key   string       `parser:"@Ident ( '=' | Space )"`
	Value parser.Value `parser:"@@ Space?"`
}
