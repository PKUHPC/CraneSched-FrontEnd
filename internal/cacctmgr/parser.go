package cacctmgr

import (
	"fmt"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

// CAcctMgrCommand
type CAcctMgrCommand struct {
	Action         *ActionType   `parser:"@@"`
	Resource       *ResourceType `parser:"@@?"`
	PrimaryFlags   []*Flag       `parser:"@@*"`
	Args           []*Argument   `parser:"@@*"`
	SecondaryFlags []*Flag       `parser:"@@*"`
}

// ActionType
type ActionType struct {
	Add     bool `parser:"@'add'"`
	Delete  bool `parser:"| @'delete'"`
	Remove  bool `parser:"| @'remove'"`
	Modify  bool `parser:"| @'modify'"`
	Show    bool `parser:"| @'show'"`
	Search  bool `parser:"| @'search'"`
	Query   bool `parser:"| @'query'"`
	Find    bool `parser:"| @'find'"`
	Block   bool `parser:"| @'block'"`
	Unblock bool `parser:"| @'unblock'"`
}

// ResourceType
type ResourceType struct {
	Account bool `parser:"@'account'"`
	User    bool `parser:"| @'user'"`
	Qos     bool `parser:"| @'qos'"`
	Event   bool `parser:"| @'event'"`
}

// Flag
type Flag struct {
	Name  string `parser:"'-' '-'? @Ident"`
	Value string `parser:"( '=' (@String | @Ident | @Number) | (@String | @Ident | @Number) )?"`
}

// Argument
type Argument struct {
	Value string `parser:"@String | @Ident | @Number"`
}

var CAcctMgrLexer = lexer.MustSimple([]lexer.SimpleRule{
	{"whitespace", `\s+`},
	{"String", `"[^"]*"|'[^']*'`},
	{"Number", `[-+]?\d+(\.\d+)?`},
	{"Ident", `[a-zA-Z][a-zA-Z0-9_\-\.]*`},
	{"Punct", `[-=,:]`},
})

var CAcctMgrParser = participle.MustBuild[CAcctMgrCommand](
	participle.Lexer(CAcctMgrLexer),
	participle.Elide("whitespace"),
)

func ParseCAcctMgrCommand(input string) (*CAcctMgrCommand, error) {
	return CAcctMgrParser.ParseString("", input)
}

func (a ActionType) String() string {
	switch {
	case a.Add:
		return "add"
	case a.Delete:
		return "delete"
	case a.Remove:
		return "remove"
	case a.Modify:
		return "modify"
	case a.Show:
		return "show"
	case a.Search:
		return "search"
	case a.Query:
		return "query"
	case a.Find:
		return "find"
	case a.Block:
		return "block"
	case a.Unblock:
		return "unblock"
	default:
		return ""
	}
}

func (r ResourceType) String() string {
	switch {
	case r.Account:
		return "account"
	case r.User:
		return "user"
	case r.Qos:
		return "qos"
	case r.Event:
		return "event"
	default:
		return ""
	}
}

// GetAction
func (c *CAcctMgrCommand) GetAction() string {
	if c.Action == nil {
		return ""
	}
	return c.Action.String()
}

// GetResource
func (c *CAcctMgrCommand) GetResource() string {
	if c.Resource == nil {
		return ""
	}
	return c.Resource.String()
}

// GetPrimaryFlag
func (c *CAcctMgrCommand) GetPrimaryFlag(name string) (string, bool) {
	for _, flag := range c.PrimaryFlags {
		if flag.Name == name {
			return flag.Value, true
		}
	}
	return "", false
}

// GetSecondaryFlag
func (c *CAcctMgrCommand) GetSecondaryFlag(name string) (string, bool) {
	for _, flag := range c.SecondaryFlags {
		if flag.Name == name {
			return flag.Value, true
		}
	}
	return "", false
}

// GetFlag (save for backward compatibility)
func (c *CAcctMgrCommand) GetFlag(name string) (string, bool) {
	return c.GetPrimaryFlag(name)
}

// GetArgs
func (c *CAcctMgrCommand) GetArgs() []string {
	var args []string
	for _, arg := range c.Args {
		args = append(args, arg.Value)
	}
	return args
}

// GetFirstArg
func (c *CAcctMgrCommand) GetFirstArg() (string, bool) {
	if len(c.Args) > 0 {
		return c.Args[0].Value, true
	}
	return "", false
}

// IsAccountOperation
func (c *CAcctMgrCommand) IsAccountOperation() bool {
	return c.Resource != nil && c.Resource.Account
}

// IsUserOperation
func (c *CAcctMgrCommand) IsUserOperation() bool {
	return c.Resource != nil && c.Resource.User
}

// IsQosOperation
func (c *CAcctMgrCommand) IsQosOperation() bool {
	return c.Resource != nil && c.Resource.Qos
}

// IsValid
func (c *CAcctMgrCommand) IsValid() bool {
	if c.Action == nil {
		return false
	}
	return c.Resource != nil
}

func (c *CAcctMgrCommand) String() string {
	var parts []string

	if c.Action != nil {
		parts = append(parts, c.Action.String())
	}

	if c.Resource != nil {
		parts = append(parts, c.Resource.String())
	}

	for _, flag := range c.PrimaryFlags {
		if flag.Value != "" {
			parts = append(parts, fmt.Sprintf("--%s=%s", flag.Name, flag.Value))
		} else {
			parts = append(parts, fmt.Sprintf("--%s", flag.Name))
		}
	}

	for _, arg := range c.Args {
		parts = append(parts, arg.Value)
	}

	for _, flag2 := range c.SecondaryFlags {
		if flag2.Value != "" {
			parts = append(parts, fmt.Sprintf("--%s=%s", flag2.Name, flag2.Value))
		} else {
			parts = append(parts, fmt.Sprintf("--%s", flag2.Name))
		}
	}

	return strings.Join(parts, " ")
}
