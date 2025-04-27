package ccontrol

import (
	"fmt"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

// CControlCommand
type CControlCommand struct {
	Action   *ActionType   `parser:"@@"`
	Resource *ResourceType `parser:"@@?"`
	Flags    []*Flag       `parser:"@@*"`
	Args     []*Argument   `parser:"@@*"`
	Flags2   []*Flag       `parser:"@@*"`
}

// ActionType
type ActionType struct {
	Show    bool `parser:"@'show'"`
	Update  bool `parser:"| @'update'"`
	Hold    bool `parser:"| @'hold'"`
	Release bool `parser:"| @'release'"`
}

// ResourceType
type ResourceType struct {
	Node      bool `parser:"@'node'"`
	Partition bool `parser:"| @'partition'"`
	Job       bool `parser:"| @'job'"`
	Config    bool `parser:"| @'config'"`
}

// Flag
type Flag struct {
	Name  string `parser:"'-' '-'? @Ident"`
	Value string `parser:"( '=' @String | '=' @Ident | '=' @TimeFormat | ' '* @String | ' '* @Ident | ' '* @TimeFormat | ' '* @Number )?"`
}

// Argument
type Argument struct {
	Value string `parser:"@String | @Ident | @TimeFormat | @Number"`
}

var CControlLexer = lexer.MustSimple([]lexer.SimpleRule{
	{"whitespace", `\s+`},
	{"String", `"[^"]*"|'[^']*'`},
	{"TimeFormat", `\d+:\d+:\d+|\d+-\d+:\d+:\d+`},
	{"Number", `[-+]?\d+(\.\d+)?`},
	{"Ident", `[a-zA-Z][a-zA-Z0-9_\-\.]*`},
	{"Punct", `[-=,:]`},
})

var CControlParser = participle.MustBuild[CControlCommand](
	participle.Lexer(CControlLexer),
	participle.Elide("whitespace"),
)

func ParseCControlCommand(input string) (*CControlCommand, error) {
	return CControlParser.ParseString("", input)
}

func (a ActionType) String() string {
	switch {
	case a.Show:
		return "show"
	case a.Update:
		return "update"
	case a.Hold:
		return "hold"
	case a.Release:
		return "release"
	default:
		return ""
	}
}

func (r ResourceType) String() string {
	switch {
	case r.Node:
		return "node"
	case r.Partition:
		return "partition"
	case r.Job:
		return "job"
	case r.Config:
		return "config"
	default:
		return ""
	}
}

// GetAction
func (c *CControlCommand) GetAction() string {
	if c.Action == nil {
		return ""
	}
	return c.Action.String()
}

// GetResource
func (c *CControlCommand) GetResource() string {
	if c.Resource == nil {
		return ""
	}
	return c.Resource.String()
}

// GetFlag
func (c *CControlCommand) GetFlag(name string) (string, bool) {
	for _, flag := range c.Flags {
		if flag.Name == name {
			return flag.Value, true
		}
	}
	return "", false
}

// GetFlag2
func (c *CControlCommand) GetFlag2(name string) (string, bool) {
	for _, flag := range c.Flags2 {
		if flag.Name == name {
			return flag.Value, true
		}
	}
	return "", false
}

// GetArgs
func (c *CControlCommand) GetArgs() []string {
	var args []string
	for _, arg := range c.Args {
		args = append(args, arg.Value)
	}
	return args
}

// GetFirstArg
func (c *CControlCommand) GetFirstArg() (string, bool) {
	if len(c.Args) > 0 {
		return c.Args[0].Value, true
	}
	return "", false
}

// IsHoldOrReleaseOperation
func (c *CControlCommand) IsHoldOrReleaseOperation() bool {
	if c.Action == nil {
		return false
	}
	return c.Action.Hold || c.Action.Release
}

// GetHoldOrReleaseID 获取作业ID（从第一个参数获取）
func (c *CControlCommand) GetHoldOrReleaseID() string {
	if len(c.Args) > 0 {
		return c.Args[0].Value
	}
	return ""
}

// IsValid
func (c *CControlCommand) IsValid() bool {
	if c.Action == nil {
		return false
	}

	if c.IsHoldOrReleaseOperation() {
		return c.GetHoldOrReleaseID() != ""
	}

	return c.Resource != nil
}

func (c *CControlCommand) String() string {
	var parts []string

	if c.Action != nil {
		parts = append(parts, c.Action.String())
	}

	if c.Resource != nil {
		parts = append(parts, c.Resource.String())
	}

	for _, flag := range c.Flags {
		if flag.Value != "" {
			parts = append(parts, fmt.Sprintf("--%s=%s", flag.Name, flag.Value))
		} else {
			parts = append(parts, fmt.Sprintf("--%s", flag.Name))
		}
	}

	for _, arg := range c.Args {
		parts = append(parts, arg.Value)
	}

	return strings.Join(parts, " ")
}
